# 测试客户端：验证握手 + 帧收发
import socket, base64, os, threading, time, struct, json
KEY = base64.b64encode(os.urandom(16)).decode()
def connect(name):
    s = socket.create_connection(('127.0.0.1', 8765), timeout=5)
    req = ('GET /chat HTTP/1.1\r\nHost: localhost:8765\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n' % KEY)
    s.sendall(req.encode())
    resp = b''
    while b'\r\n\r\n' not in resp: resp += s.recv(1024)
    head = resp.decode('utf-8','ignore')
    assert '101' in head.split('\r\n')[0], head
    acc = [l for l in head.split('\r\n') if l.lower().startswith('sec-websocket-accept')][0].split(':',1)[1].strip()
    import hashlib
    want = base64.b64encode(hashlib.sha1((KEY + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
    print('  握手 101 OK | Accept 校验:', 'PASS' if acc == want else 'FAIL')
    return s
def send(s, obj, mask=b'\x01\x02\x03\x04'):
    data = json.dumps(obj).encode()
    n = len(data); head = bytes([0x81])
    if n < 126: head += bytes([0x80 | n])
    else: head += bytes([0x80 | 126]) + struct.pack('>H', n)
    masked = bytes(b ^ mask[i%4] for i,b in enumerate(data))
    s.sendall(head + mask + masked)
def recv(s):
    h = s.recv(2)
    if len(h) < 2: return None
    ln = h[1] & 0x7F
    if ln == 126: ln = struct.unpack('>H', s.recv(2))[0]
    elif ln == 127: ln = struct.unpack('>Q', s.recv(8))[0]
    buf = b''
    while len(buf) < ln: buf += s.recv(ln - len(buf))
    return json.loads(buf.decode('utf-8'))
print('=== 测试 1：客户端 A 连接 ===')
a = connect('A')
send(a, {'type':'join','name':'小明'})
print('  收到:', recv(a))
print('=== 测试 2：客户端 B 连接并加入 ===')
b = connect('B')
send(b, {'type':'join','name':'小红'})
print('  B 收到:', recv(b))
print('  A 收到通知:', recv(a))
print('=== 测试 3：A 发消息，B 应收到 ===')
send(a, {'type':'chat','text':'大家好，这是 WebSocket 实时消息'})
print('  A 回显:', recv(a))
print('  B 收到:', recv(b))
print('=== 测试 4：B 断开，A 应收到离开通知 ===')
b.close(); time.sleep(0.5)
print('  A 收到:', recv(a))
a.close()
print()
print('全部测试完成')