"""DataNode của mini-HDFS: lưu block trên đĩa cục bộ, pipeline ghi, đọc trực tiếp,
heartbeat, thực hiện lệnh TRANSFER / DELETE / RECONSTRUCT từ NameNode."""
import argparse
import os
import socketserver
import threading
import time
import traceback

import numpy as np

from common import (ACK, PACKET_SIZE, PKT_HDR, RpcClient, Throttle, connect,
                    recv_exact, recv_header, send_header, send_packet, recv_packet)
import zlib


class DataNode:
    def __init__(self, a):
        self.id, self.port, self.rack = a.id, a.port, a.rack
        self.dir = os.path.join(a.dir, "current")
        os.makedirs(self.dir, exist_ok=True)
        self.capacity = a.capacity
        self.throttle = Throttle(a.mbps * 1024 * 1024 if a.mbps else 0)
        self.hb = a.hb
        self.lock = threading.Lock()
        self.sizes = {}
        for fn in os.listdir(self.dir):
            if fn.startswith("blk_") and not fn.endswith(".tmp"):
                self.sizes[int(fn[4:])] = os.path.getsize(os.path.join(self.dir, fn))
            elif fn.endswith(".tmp"):
                os.remove(os.path.join(self.dir, fn))
        self.c = {"disk_w": 0, "disk_r": 0, "net_in": 0, "net_out": 0}
        self.recon_lock = threading.Lock()     # khôi phục EC lần lượt từng block để giới hạn RAM
        self.nn = RpcClient(("127.0.0.1", a.nn))
        self.nn.call("register", self.id, "127.0.0.1", self.port, self.rack, self.capacity)
        self.nn.call("block_report", self.id, list(self.sizes.items()))
        threading.Thread(target=self._heartbeat, daemon=True).start()

    def bpath(self, bid):
        return os.path.join(self.dir, f"blk_{bid}")

    def used(self):
        with self.lock:
            return sum(self.sizes.values())

    # ------------------------------------------------------------------ ghi block (pipeline)
    def write_block(self, sock, hdr):
        bid, targets = hdr["bid"], hdr["targets"]
        tmp = self.bpath(bid) + ".tmp"
        down = None
        if targets:
            _, h, p = targets[0]
            down = connect(h, p)
            send_header(down, {"op": "WRITE", "bid": bid, "targets": targets[1:]})
        n = 0
        size = 0
        with open(tmp, "wb", buffering=0) as f:
            while True:
                raw = recv_exact(sock, PKT_HDR.size)
                seq, ln, crc = PKT_HDR.unpack(raw)
                data = recv_exact(sock, ln) if ln else b""
                self.c["net_in"] += ln
                if down:                         # chuyển tiếp xuống DataNode tiếp theo trước
                    down.sendall(raw)
                    if ln:
                        down.sendall(data)
                    self.c["net_out"] += ln
                if ln == 0:
                    break
                if zlib.crc32(data) != crc:
                    raise IOError("sai checksum khi nhận")
                self.throttle.consume(ln)        # rồi mới ghi xuống đĩa cục bộ
                f.write(data)
                self.c["disk_w"] += ln
                size += ln
                n += 1
        os.replace(tmp, self.bpath(bid))
        with self.lock:
            self.sizes[bid] = size
        # gom ACK từ phía sau pipeline, kiểm tra đủ rồi mới gửi ACK ngược lên
        if down:
            acks = recv_exact(down, ACK.size * (n + 1))
            down.close()
            for i in range(n + 1):
                _, st = ACK.unpack_from(acks, i * ACK.size)
                if st != 0:
                    raise IOError("DataNode phía sau báo lỗi")
        self.nn.call("block_received", self.id, bid, size)
        sock.sendall(b"".join(ACK.pack(i, 0) for i in range(n)) + ACK.pack(-1, 0))

    # ------------------------------------------------------------------ đọc block
    def read_block(self, sock, hdr):
        bid, off, length = hdr["bid"], hdr.get("off", 0), hdr.get("len", -1)
        path = self.bpath(bid)
        if not os.path.exists(path):
            send_header(sock, {"ok": False})
            return
        send_header(sock, {"ok": True})
        seq = 0
        with open(path, "rb", buffering=0) as f:
            pos = off
            end = os.path.getsize(path) if length < 0 else min(off + length, os.path.getsize(path))
            fd = f.fileno()
            while pos < end:
                n = min(PACKET_SIZE, end - pos)
                chunk = os.pread(fd, n, pos)
                self.throttle.consume(n)
                send_packet(sock, seq, chunk)
                self.c["disk_r"] += n
                self.c["net_out"] += n
                pos += n
                seq += 1
        send_packet(sock, seq, b"")

    # ------------------------------------------------------------------ lệnh từ NameNode
    def _heartbeat(self):
        while True:
            try:
                cmds = self.nn.call("heartbeat", self.id, self.used(), self.capacity)
                for c in cmds:
                    threading.Thread(target=self._run_cmd, args=(c,), daemon=True).start()
            except Exception:
                traceback.print_exc()
            time.sleep(self.hb)

    def _run_cmd(self, c):
        try:
            if c[0] == "DELETE":
                bid = c[1]
                with self.lock:
                    self.sizes.pop(bid, None)
                try:
                    os.remove(self.bpath(bid))
                except FileNotFoundError:
                    pass
                self.nn.call("block_deleted", self.id, bid)
            elif c[0] == "BLOCKREPORT":
                with self.lock:
                    items = list(self.sizes.items())
                self.nn.call("block_report", self.id, items)
            elif c[0] == "TRANSFER":
                self._transfer(c[1], c[2])
            elif c[0] == "RECONSTRUCT":
                self._reconstruct(*c[1:])
        except Exception:
            traceback.print_exc()

    def _transfer(self, bid, targets):
        """Nhân bản lại / di chuyển block: đọc block cục bộ, đẩy qua pipeline tới các node đích."""
        _, h, p = targets[0]
        s = connect(h, p)
        send_header(s, {"op": "WRITE", "bid": bid, "targets": targets[1:]})
        n = 0
        with open(self.bpath(bid), "rb", buffering=0) as f:
            while True:
                chunk = f.read(PACKET_SIZE)
                if not chunk:
                    break
                self.throttle.consume(len(chunk))
                self.c["disk_r"] += len(chunk)
                self.c["net_out"] += len(chunk)
                send_packet(s, n, chunk)
                n += 1
        send_packet(s, n, b"")
        recv_exact(s, ACK.size * (n + 1))
        s.close()

    def _fetch(self, addr, bid):
        _, h, p = addr
        s = connect(h, p)
        send_header(s, {"op": "READ", "bid": bid})
        if not recv_header(s)["ok"]:
            raise IOError("không đọc được block")
        parts = []
        while True:
            _, d = recv_packet(s)
            if d is None:
                break
            parts.append(d)
            self.c["net_in"] += len(d)
        s.close()
        return b"".join(parts)

    def _reconstruct(self, bid, others, size):
        with self.recon_lock:
            self._reconstruct1(bid, others, size)

    def _reconstruct1(self, bid, others, size):
        """Khôi phục block EC bị mất: đọc 2 block còn lại rồi XOR."""
        bufs = [np.frombuffer(self._fetch(addr, ib), dtype=np.uint8) for ib, addr in others]
        L = max(size, *(len(b) for b in bufs))
        out = np.zeros(L, dtype=np.uint8)
        for b in bufs:
            out[:len(b)] ^= b
        data = out[:size].tobytes()
        self.throttle.consume(len(data))
        with open(self.bpath(bid), "wb") as f:
            f.write(data)
        self.c["disk_w"] += len(data)
        with self.lock:
            self.sizes[bid] = len(data)
        self.nn.call("block_received", self.id, bid, len(data))


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        dn = self.server.dn
        sock = self.request
        try:
            hdr = recv_header(sock)
            op = hdr["op"]
            if op == "WRITE":
                dn.write_block(sock, hdr)
            elif op == "READ":
                dn.read_block(sock, hdr)
            elif op == "STATS":
                send_header(sock, {**dn.c, "used": dn.used(), "blocks": len(dn.sizes)})
        except (ConnectionError, BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            traceback.print_exc()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 256


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--rack", default="/default-rack")
    ap.add_argument("--dir", required=True)
    ap.add_argument("--nn", type=int, required=True)
    ap.add_argument("--capacity", type=int, default=50 * 2**30)
    ap.add_argument("--mbps", type=float, default=0)
    ap.add_argument("--hb", type=float, default=1.0)
    a = ap.parse_args()
    srv = Server(("127.0.0.1", a.port), Handler)
    srv.dn = DataNode(a)
    print(f"DataNode {a.id} ({a.rack}) chạy ở cổng {a.port}", flush=True)
    srv.serve_forever()
