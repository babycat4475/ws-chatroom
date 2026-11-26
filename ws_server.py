#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WebSocket 实时聊天服务器（纯标准库，手写协议）
------------------------------------------------
考点：
  1. HTTP 升级握手（Sec-WebSocket-Key -> Sec-WebSocket-Accept）
  2. WebSocket 数据帧的解析（FIN / opcode / Mask / 长度）
  3. 多线程 + 广播

启动： python3 ws_server.py
然后浏览器打开： http://localhost:8765
"""
from contextlib import closing
from pathlib import Path
import secrets
import sqlite3
import webbrowser
import socket, threading, base64, hashlib, struct, json, time, os, sys

# ---- 跨平台：强制 UTF-8 输出（Windows 控制台默认 GBK 会报错/乱码）----
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

HOST        = "0.0.0.0"
# 单端口：网页和 WebSocket 共用同一个端口。
# 这样只要把这一条端口暴露到公网（隧道/服务器），外面的浏览器
# 就能同时拿到网页和长连接，不用再开两个端口。
PORT        = int(os.environ.get("PORT", "8765"))
GUID        = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"   # RFC 6455 规定
MAX_PAYLOAD = 1 << 20        # 单帧上限 1MB：防止客户端声明超大帧把服务端内存拖垮
SEND_TIMEOUT = 5             # 单个客户端发送超时（秒）：防止慢客户端阻塞整个广播

HISTORY_LIMIT = 50          # 进入房间时推送的最近消息数
HISTORY_MAX = 2000          # 数据库最多保留的消息数
DB_PATH = Path(__file__).with_name("chat.sqlite3")
clients = {}
# 加入时的历史快照和实时广播按同一顺序发送，避免重复或漏掉交界处的消息。
room_lock = threading.RLock()


def init_history():
    with closing(sqlite3.connect(DB_PATH)) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY, name TEXT NOT NULL
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id TEXT NOT NULL, name TEXT NOT NULL,
            text TEXT NOT NULL, ts TEXT NOT NULL
        )""")


def history_messages(limit=HISTORY_LIMIT, before_id=None, query=None):
    """按 id 倒序取一页，再反转成从旧到新返回，方便前端直接追加。"""
    try:
        limit = max(1, min(int(limit), 100))
    except (TypeError, ValueError):
        limit = HISTORY_LIMIT
    try:
        before_id = int(before_id) if before_id is not None else 10 ** 18
    except (TypeError, ValueError):
        before_id = 10 ** 18

    where = ["id < ?"]
    params = [before_id]
    if query:
        q = str(query)[:100]
        like = "%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        where.append("(name LIKE ? ESCAPE '\\' OR text LIKE ? ESCAPE '\\')")
        params.extend([like, like])

    sql = ("SELECT id, sender_id, name, text, ts FROM messages WHERE "
           + " AND ".join(where) + " ORDER BY id DESC LIMIT ?")
    params.append(limit + 1)
    with closing(sqlite3.connect(DB_PATH)) as db:
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute(sql, params).fetchall()]

    has_more = len(rows) > limit
    rows = rows[:limit]
    rows.reverse()
    return rows, has_more


def recent_messages():
    rows, _ = history_messages(HISTORY_LIMIT)
    return rows


def save_message(sender_id, name, text):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with closing(sqlite3.connect(DB_PATH)) as db, db:
        row = db.execute("INSERT INTO messages(sender_id, name, text, ts) VALUES (?, ?, ?, ?)",
                         (sender_id, name, text, ts))
        message_id = row.lastrowid
        db.execute("DELETE FROM messages WHERE id <= ?", (message_id - HISTORY_MAX,))
    return {"type": "chat", "id": message_id, "sender_id": sender_id,
            "name": name, "text": text, "ts": ts}


def join_room(conn, name, token):
    if not isinstance(token, str) or len(token) != 64 or any(c not in "0123456789abcdef" for c in token):
        token = secrets.token_hex(32)
    sender_id = hashlib.sha256(token.encode("ascii")).hexdigest()
    with closing(sqlite3.connect(DB_PATH)) as db, db:
        account = db.execute("SELECT name FROM accounts WHERE id = ?", (sender_id,)).fetchone()
        if account:
            name = account[0]
        else:
            # 旧版凭据产生相同的发送者 ID，可沿用旧记录里的昵称。
            previous = db.execute("SELECT name FROM messages WHERE sender_id = ? ORDER BY id DESC LIMIT 1",
                                  (sender_id,)).fetchone()
            if previous:
                name = previous[0]
            db.execute("INSERT INTO accounts(id, name) VALUES (?, ?)", (sender_id, name))
    already_online = any(user["id"] == sender_id for user in clients.values())
    clients[conn] = {"id": sender_id, "name": name}
    send_to(conn, {"type": "welcome", "name": name, "sender_id": sender_id,
                   "resume_token": token, "online": online_list(), "history": recent_messages()})
    if not already_online:
        notice("%s 加入了聊天室" % name, "join")
    broadcast({"type": "online", "online": online_list()})


# ---------- 1. 握手 ----------
def do_handshake(conn, raw=None):
    """读取 HTTP 请求头，回 101 Switching Protocols
    raw: 允许把已经读到的请求头直接传进来（单端口分发时已经读过一遍）"""
    if raw is None:
        raw = b""
        while b"\r\n\r\n" not in raw:
            chunk = conn.recv(1024)
            if not chunk:
                return None
            raw += chunk
            if len(raw) > 8192:
                return None
    text = raw.decode("utf-8", "ignore")
    headers = {}
    for line in text.split("\r\n")[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()

    key = headers.get("sec-websocket-key")
    if not key:
        return None

    # 核心公式：accept = base64( sha1( key + GUID ) )
    accept = base64.b64encode(
        hashlib.sha1((key + GUID).encode()).digest()
    ).decode()

    resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " + accept + "\r\n"
        "\r\n"
    )
    conn.sendall(resp.encode())

    # 修复②：设置发送超时（只影响 send，不影响 recv）
    try:
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO,
                        SEND_TIMEOUT * 1000 if os.name == "nt" else struct.pack("@ll", SEND_TIMEOUT, 0))
    except Exception:
        pass
    return headers

# ---------- 2. 帧解析 ----------
def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

def read_frame(conn):
    """返回 (opcode, payload_bytes)，连接断开返回 None"""
    hdr = recv_exact(conn, 2)
    if not hdr:
        return None
    b1, b2 = hdr[0], hdr[1]
    opcode = b1 & 0x0F
    masked = (b2 & 0x80) != 0
    length = b2 & 0x7F

    if length == 126:
        ext = recv_exact(conn, 2)
        if not ext: return None
        length = struct.unpack(">H", ext)[0]
    elif length == 127:
        ext = recv_exact(conn, 8)
        if not ext: return None
        length = struct.unpack(">Q", ext)[0]

    # 修复①：帧长上限。原来解出长度后直接 recv_exact(length)，
    # 客户端只要声明 2^60 就会让服务端一直申请内存直到 OOM。
    if length > MAX_PAYLOAD:
        print("[警告] 声明帧长 %d 超过上限 %d，断开该连接" % (length, MAX_PAYLOAD), flush=True)
        return None

    mask = recv_exact(conn, 4) if masked else None
    payload = recv_exact(conn, length) if length else b""
    if payload is None:
        return None

    if masked and mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload

def build_frame(data, opcode=0x1):
    """服务端 -> 客户端的帧（不掩码）"""
    if isinstance(data, str):
        data = data.encode("utf-8")
    fin_op = 0x80 | opcode
    n = len(data)
    if n < 126:
        head = bytes([fin_op, n])
    elif n < 65536:
        head = bytes([fin_op, 126]) + struct.pack(">H", n)
    else:
        head = bytes([fin_op, 127]) + struct.pack(">Q", n)
    return head + data

# ---------- 3. 广播 ----------
def send_to(conn, obj):
    try:
        conn.sendall(build_frame(json.dumps(obj, ensure_ascii=False)))
    except OSError:
        # 唤醒接收线程，由该连接的 finally 统一处理离开事件。
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

def broadcast(obj):
    for conn in clients:
        send_to(conn, obj)

def online_list():
    return list({user["id"]: user["name"] for user in clients.values()}.values())

def notice(text, kind="sys"):
    broadcast({"type": "notice", "kind": kind, "text": text, "ts": time.strftime("%H:%M:%S")})

# ---------- 4. 每个连接一个线程 ----------
def handle_client(conn, addr, raw=None):
    try:
        headers = do_handshake(conn, raw)
        if headers is None:
            conn.close(); return
        print("[握手成功] %s:%s  %s" % (addr[0], addr[1], headers.get("origin", "")))

        while True:
            frame = read_frame(conn)
            if frame is None:
                break
            opcode, payload = frame

            if opcode == 0x8:      # CLOSE
                break
            if opcode == 0x9:      # PING -> PONG
                with room_lock:
                    conn.sendall(build_frame(payload, opcode=0xA))
                continue
            if opcode != 0x1:      # 只处理文本帧
                continue

            try:
                msg = json.loads(payload.decode("utf-8"))
            except Exception:
                continue

            if not isinstance(msg, dict):
                continue
            with room_lock:
                if msg.get("type") == "join" and conn not in clients:
                    raw_name = msg.get("name")
                    name = raw_name.strip()[:16] if isinstance(raw_name, str) else ""
                    join_room(conn, name or "匿名", msg.get("resume_token"))
                elif msg.get("type") == "chat" and conn in clients:
                    text = msg.get("text")
                    if isinstance(text, str) and text.strip():
                        user = clients[conn]
                        broadcast(save_message(user["id"], user["name"], text.strip()[:500]))
                elif msg.get("type") == "history" and conn in clients:
                    query = msg.get("query")
                    if isinstance(query, str):
                        query = query.strip()[:100]
                    else:
                        query = None
                    rows, has_more = history_messages(
                        limit=msg.get("limit", HISTORY_LIMIT),
                        before_id=msg.get("before_id"),
                        query=query,
                    )
                    send_to(conn, {
                        "type": "history",
                        "messages": rows,
                        "has_more": has_more,
                        "next_before": rows[0]["id"] if rows else None,
                        "query": query or "",
                    })
    except Exception as e:
        print("[异常]", addr, e)
    finally:
        with room_lock:
            info = clients.pop(conn, None)
            if info and all(user["id"] != info["id"] for user in clients.values()):
                notice("%s 离开了聊天室" % info["name"], "leave")
                broadcast({"type": "online", "online": online_list()})
        try: conn.close()
        except Exception: pass

# ---------- 5. 单端口：同一个端口既发网页也接 WebSocket ----------
def serve_http(conn, raw):
    """普通 HTTP 请求：只返回 index.html（白名单，其它一律 404）"""
    here = os.path.dirname(os.path.abspath(__file__))
    line = raw.decode("utf-8", "ignore").split("\r\n", 1)[0]
    parts = line.split(" ")
    path = parts[1].split("?")[0].strip("/") if len(parts) > 1 else ""
    fname = path or "index.html"

    if fname != "index.html":
        body = b"404 Not Found"
        head = ("HTTP/1.1 404 Not Found\r\n"
                "Content-Type: text/plain; charset=utf-8\r\n"
                "Content-Length: %d\r\nConnection: close\r\n\r\n" % len(body))
        conn.sendall(head.encode() + body)
        return

    with open(os.path.join(here, "index.html"), "rb") as f:
        body = f.read()
    head = ("HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "Content-Length: %d\r\nConnection: close\r\n\r\n" % len(body))
    conn.sendall(head.encode() + body)

def handle_conn(conn, addr):
    """统一入口：先读一段请求头，再决定走 WebSocket 还是普通网页"""
    try:
        raw = b""
        conn.settimeout(10)
        while b"\r\n\r\n" not in raw:
            chunk = conn.recv(1024)
            if not chunk:
                conn.close(); return
            raw += chunk
            if len(raw) > 8192:
                conn.close(); return
        conn.settimeout(None)

        if b"upgrade: websocket" in raw.lower():
            handle_client(conn, addr, raw)
        else:
            serve_http(conn, raw)
            print("[网页] %s:%s 打开页面" % (addr[0], addr[1]))
            conn.close()
    except Exception as e:
        print("[连接异常]", addr, e)
        try: conn.close()
        except Exception: pass

def main():
    init_history()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(50)
    print("=" * 56)
    print("  WebSocket 实时聊天服务器已启动（单端口模式）")
    print("  本机打开   : http://localhost:%d" % PORT)
    print("  局域网打开 : http://<本机IP>:%d" % PORT)
    print("  公网访问   : cloudflared tunnel --url http://localhost:%d" % PORT)
    print("  （Ctrl+C 停止）")
    print("=" * 56)
    if "--open-browser" in sys.argv:
        webbrowser.open("http://localhost:%d" % PORT)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
