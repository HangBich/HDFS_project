"""Các hàm dùng chung cho mini-HDFS: RPC, đóng gói gói tin, giới hạn băng thông."""
import json
import socket
import struct
import threading
import time
import zlib
from multiprocessing.connection import Client as _MPClient

AUTHKEY = b"minihdfs"
PACKET_SIZE = 64 * 1024          # kích thước gói tin trong pipeline (giống HDFS: 64 KB)
CELL_SIZE = 1024 * 1024          # cell 1 MB cho erasure coding XOR-2-1
DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024


# --------------------------------------------------------------------------
# RPC tới NameNode (multiprocessing.connection, pickle)
# --------------------------------------------------------------------------
class RpcClient:
    def __init__(self, addr):
        self.addr = tuple(addr)
        self.lock = threading.Lock()
        self.conn = _MPClient(self.addr, authkey=AUTHKEY)
        self.calls = 0

    def call(self, method, *args, **kw):
        with self.lock:
            self.calls += 1
            self.conn.send((method, args, kw))
            ok, res = self.conn.recv()
        if not ok:
            raise RuntimeError(f"RPC {method} lỗi: {res}")
        return res

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Giao thức luồng dữ liệu (client <-> DataNode, DataNode <-> DataNode)
# --------------------------------------------------------------------------
def send_header(sock, obj):
    b = json.dumps(obj).encode()
    sock.sendall(struct.pack(">I", len(b)) + b)


def recv_exact(sock, n):
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        r = sock.recv_into(view[got:], n - got)
        if r == 0:
            raise ConnectionError("kết nối bị đóng")
        got += r
    return buf


def recv_header(sock):
    (n,) = struct.unpack(">I", recv_exact(sock, 4))
    return json.loads(bytes(recv_exact(sock, n)))


PKT_HDR = struct.Struct(">qiI")  # seq, len, crc32


def send_packet(sock, seq, data):
    hdr = PKT_HDR.pack(seq, len(data), zlib.crc32(data))
    total = len(hdr) + len(data)
    sent = sock.sendmsg([hdr, data])
    if sent < total:
        rest = (hdr + bytes(data))[sent:]
        sock.sendall(rest)


def recv_packet(sock, verify=True):
    seq, ln, crc = PKT_HDR.unpack(recv_exact(sock, PKT_HDR.size))
    if ln == 0:
        return seq, None
    data = recv_exact(sock, ln)
    if verify and zlib.crc32(data) != crc:
        raise IOError(f"sai checksum ở gói {seq}")
    return seq, data


ACK = struct.Struct(">qb")


def connect(host, port, timeout=30):
    s = socket.create_connection((host, port), timeout=timeout)
    s.settimeout(None)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    return s


# --------------------------------------------------------------------------
# Giới hạn băng thông (token bucket) - mô phỏng mỗi worker có đĩa riêng
# --------------------------------------------------------------------------
class Throttle:
    def __init__(self, bytes_per_sec):
        self.rate = bytes_per_sec
        self.lock = threading.Lock()
        self.next_free = time.monotonic()

    def consume(self, n):
        if not self.rate:
            return
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_free)
            self.next_free = start + n / self.rate
            wait = self.next_free - now
        if wait > 0:
            time.sleep(wait)
