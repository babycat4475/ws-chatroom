import socket, base64, os, time, struct, json, hashlib, threading
KEY = base64.b64encode(os.urandom(16)).decode()
def connect():
    s = socket.create_connection(('127.0.0.1', 8765), timeout=5)
    s.sendall(('GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n' % KEY).encode())
    r = b''
    while b'\r\n\r\n' not in r: r += s.recv(1024)
    return s
def send(s, obj):
    d = json.dumps(obj).encode(); n = len(d); m = bytes([1,2,3,4])
    h = bytes([0x81]); h += bytes([0x80|n]) if n < 126 else bytes([0x80|126]) + struct.pack('>H', n)
    s.sendall(h + m + bytes(b ^ m[i%4] for i,b in enumerate(d)))
def drain(s, t=0.6):
    s.settimeout(t); out = []
    try:
        while True:
            h = s.recv(2)
            if len(h) < 2: break
            ln = h[1] & 0x7F
            if ln == 126: ln = struct.unpack('>H', s.recv(2))[0]
            elif ln == 127: ln = struct.unpack('>Q', s.recv(8))[0]
            b = b''
            while len(b) < ln: b += s.recv(ln - len(b))
            out.append(json.loads(b.decode()))
    except Exception: pass
    return out
print('=== 严格验证广播 ===')
a = connect(); send(a, {'type':'join','name':'小明'}); print('A 加入:', drain(a))
b = connect(); send(b, {'type':'join','name':'小红'}); print('B 加入:', drain(b))
print('A 收到 B 加入的通知:', drain(a))
print()
print('--- A 发一条消息 ---')
send(a, {'type':'chat','text':'你好，WebSocket 实时消息'})
ma = drain(a); mb = drain(b)
print('A 收到:', ma)
print('B 收到:', mb)
chat_a = [m for m in ma if m.get('type')=='chat']
chat_b = [m for m in mb if m.get('type')=='chat']
print()
print('✅ 广播给发送者:', 'PASS' if chat_a and chat_a[0]['text']=='你好，WebSocket 实时消息' else 'FAIL')
print('✅ 广播给其他客户端:', 'PASS' if chat_b and chat_b[0]['text']=='你好，WebSocket 实时消息' else 'FAIL')
print('✅ 昵称正确:', 'PASS' if chat_b and chat_b[0]['name']=='小明' else 'FAIL')
print('✅ 带时间戳:', 'PASS' if chat_b and chat_b[0].get('ts') else 'FAIL')
print()
print('--- 断开 B，A 应收到离开通知 ---')
b.close()
print('A 收到:', [m for m in drain(a) if m.get('type')=='notice'])
a.close()