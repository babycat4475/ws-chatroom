# Mia 客户端：进来说几句话（用于演示截图）
# 使用 RFC 6455 文档里的示例 Sec-WebSocket-Key
import socket, os, time, json, struct
KEY = 'dGhlIHNhbXBsZSBub25jZQ=='
def connect():
    s = socket.create_connection(('127.0.0.1', 8765), timeout=5)
    req = ('GET / HTTP/1.1' + chr(13) + chr(10) +
           'Host: localhost:8765' + chr(13) + chr(10) +
           'Upgrade: websocket' + chr(13) + chr(10) +
           'Connection: Upgrade' + chr(13) + chr(10) +
           'Sec-WebSocket-Key: ' + KEY + chr(13) + chr(10) +
           'Sec-WebSocket-Version: 13' + chr(13) + chr(10) + chr(13) + chr(10))
    s.sendall(req.encode())
    r = b''
    while b'\r\n\r\n' not in r:
        r += s.recv(1024)
    return s
def send(s, obj):
    d = json.dumps(obj, ensure_ascii=False).encode('utf-8')
    n = len(d); m = bytes([7, 7, 7, 7])
    h = bytes([0x81])
    h += bytes([0x80 | n]) if n < 126 else bytes([0x80 | 126]) + struct.pack('>H', n)
    s.sendall(h + m + bytes(b ^ m[i % 4] for i, b in enumerate(d)))
s = connect()
send(s, {'type':'join','name':'Mia'})
time.sleep(1.5)
send(s, {'type':'chat','text':'Hey Alex! Can you see this message?'})
time.sleep(2.5)
send(s, {'type':'chat','text':'It arrived instantly - no refresh at all'})
time.sleep(2.5)
send(s, {'type':'chat','text':'That is the WebSocket handshake at work'})
time.sleep(10)
s.close()