"""Client của mini-HDFS (tương đương DFSClient / lệnh hdfs dfs)."""
import threading

import numpy as np

import zlib
from common import (ACK, CELL_SIZE, DEFAULT_BLOCK_SIZE, PACKET_SIZE, PKT_HDR, RpcClient, connect,
                    recv_exact, recv_header, recv_packet, send_header, send_packet)


class HdfsClient:
    def __init__(self, nn_port, node=None):
        self.nn = RpcClient(("127.0.0.1", nn_port))
        self.node = node              # client đang chạy trên node nào (None = ngoài cụm)
        self.stats = {"failover": 0, "degraded": 0}

    # --------------------------------------------------------------- thao tác metadata
    def mkdirs(self, p):
        return self.nn.call("mkdirs", p)

    def rename(self, s, d):
        return self.nn.call("rename", s, d)

    def delete(self, p, recursive=True):
        return self.nn.call("delete", p, recursive)

    def info(self, p):
        return self.nn.call("get_file_info", p)

    def exists(self, p):
        return self.info(p) is not None

    def count(self, p):
        return self.nn.call("content_summary", p)

    def setrep(self, p, r):
        return self.nn.call("set_replication", p, r)

    def ls(self, p):
        """Liệt kê theo lô 1000 mục / RPC."""
        out, after = [], None
        while True:
            r = self.nn.call("list_status", p, after, 1000)
            out.extend(r["entries"])
            if not r["more"]:
                return out
            after = r["entries"][-1]["name"]

    def ls_r(self, p):
        res = []
        for e in self.ls(p):
            full = p.rstrip("/") + "/" + e["name"]
            res.append((full, e))
            if e["type"] == "d":
                res.extend(self.ls_r(full))
        return res

    # --------------------------------------------------------------- ghi
    def write(self, path, data, repl=3, block_size=DEFAULT_BLOCK_SIZE, ec=None):
        mv = memoryview(data)
        self.nn.call("create", path, repl, block_size, ec)
        if ec:
            groups = self._write_ec(path, mv, block_size)
            self.nn.call("complete", path, len(mv), groups)
            return
        for off in range(0, len(mv), block_size):
            blk = self.nn.call("add_block", path, self.node)
            self._write_block(blk["bid"], blk["targets"], mv[off:off + block_size])
        self.nn.call("complete", path, len(mv))

    @staticmethod
    def _write_block(bid, targets, chunk):
        _, h, p = targets[0]
        s = connect(h, p)
        send_header(s, {"op": "WRITE", "bid": bid, "targets": targets[1:]})
        n = 0
        for off in range(0, len(chunk), PACKET_SIZE):
            send_packet(s, n, chunk[off:off + PACKET_SIZE])
            n += 1
        send_packet(s, n, b"")
        acks = recv_exact(s, ACK.size * (n + 1))     # chỉ coi là xong khi đủ ACK
        s.close()
        for i in range(n + 1):
            _, st = ACK.unpack_from(acks, i * ACK.size)
            if st != 0:
                raise IOError("pipeline báo lỗi")

    def _write_ec(self, path, mv, block_size):
        """XOR-2-1: chia dữ liệu thành cell 1 MB, xen kẽ vào 2 block dữ liệu, block thứ 3 là parity."""
        groups = []
        G = 2 * block_size
        for goff in range(0, len(mv), G):
            g = np.frombuffer(mv[goff:goff + G], dtype=np.uint8)
            info = self.nn.call("add_block", path, self.node)
            d = [bytearray(), bytearray()]
            par = bytearray()
            ncell = (len(g) + CELL_SIZE - 1) // CELL_SIZE
            for j in range(0, ncell, 2):
                c0 = g[j * CELL_SIZE:(j + 1) * CELL_SIZE]
                c1 = g[(j + 1) * CELL_SIZE:(j + 2) * CELL_SIZE]
                d[0] += c0.tobytes()
                d[1] += c1.tobytes()
                p = c0.copy()
                p[:len(c1)] ^= c1
                par += p.tobytes()
            bufs = [d[0], d[1], par]
            ths = []
            for (bid, addr), buf in zip(info["internal"], bufs):
                if len(buf):
                    t = threading.Thread(target=self._write_block, args=(bid, [addr], memoryview(buf)))
                    t.start()
                    ths.append(t)
            for t in ths:
                t.join()
            groups.append((len(g), [len(b) for b in bufs]))
        return groups

    # --------------------------------------------------------------- đọc
    @staticmethod
    def _read_block(addr, bid, off=0, length=-1, size_hint=None):
        _, h, p = addr
        s = connect(h, p, timeout=10)
        try:
            send_header(s, {"op": "READ", "bid": bid, "off": off, "len": length})
            if not recv_header(s)["ok"]:
                raise IOError("DataNode không có block")
            cap = length if length >= 0 else (size_hint - off if size_hint else 0)
            buf = bytearray(cap) if cap else bytearray()
            view = memoryview(buf)
            pos = 0
            while True:
                seq, ln, crc = PKT_HDR.unpack(recv_exact(s, PKT_HDR.size))
                if ln == 0:
                    break
                if pos + ln > len(buf):
                    buf.extend(bytes(pos + ln - len(buf)))
                    view = memoryview(buf)
                got = 0
                while got < ln:
                    r = s.recv_into(view[pos + got:pos + ln], ln - got)
                    if r == 0:
                        raise ConnectionError("đóng kết nối")
                    got += r
                if zlib.crc32(view[pos:pos + ln]) != crc:
                    raise IOError("sai checksum khi đọc")
                pos += ln
            del view
            if pos != len(buf):
                del buf[pos:]
            return buf
        finally:
            s.close()

    def _read_replicated(self, b, off=0, length=-1):
        last = None
        for addr in b["locs"]:          # danh sách đã sắp theo khoảng cách
            try:
                return self._read_block(addr, b["bid"], off, length, b["size"])
            except (OSError, ConnectionError) as e:
                self.stats["failover"] += 1
                last = e
        raise IOError(f"không đọc được block {b['bid']}: {last}")

    def read(self, path):
        loc = self.nn.call("get_block_locations", path, self.node)
        if loc["ec"]:
            return b"".join(self._read_group(g) for g in loc["blocks"])
        return b"".join(self._read_replicated(b) for b in loc["blocks"])

    def read_range(self, path, off, length):
        loc = self.nn.call("get_block_locations", path, self.node)
        out, pos = [], 0
        for b in loc["blocks"]:
            bs, be = pos, pos + b["size"]
            if be > off and bs < off + length:
                s = max(off, bs) - bs
                e = min(off + length, be) - bs
                out.append(self._read_replicated(b, s, e - s))
            pos = be
        return b"".join(out)

    def _read_group(self, g):
        res = [None, None, None]
        err = [None, None, None]

        def fetch(i):
            ib, size, locs = g["internal"][i]
            if size == 0:
                res[i] = b""
                return
            for addr in locs:
                try:
                    res[i] = self._read_block(addr, ib, 0, -1, size)
                    return
                except (OSError, ConnectionError) as e:
                    err[i] = e
            err[i] = err[i] or IOError("không có vị trí")

        ths = [threading.Thread(target=fetch, args=(i,)) for i in (0, 1)]
        [t.start() for t in ths]
        [t.join() for t in ths]
        if res[0] is None or res[1] is None:     # đọc khi thiếu block: giải mã từ parity
            self.stats["degraded"] += 1
            fetch(2)
            miss = 0 if res[0] is None else 1
            other = res[1 - miss]
            p = np.frombuffer(res[2], dtype=np.uint8).copy()
            o = np.frombuffer(other, dtype=np.uint8)
            p[:len(o)] ^= o
            size = g["internal"][miss][1]
            res[miss] = p[:size].tobytes()
        d0, d1 = res[0], res[1]
        out = bytearray()
        total = g["size"]
        ncell = (total + CELL_SIZE - 1) // CELL_SIZE
        for j in range(ncell):
            src = d0 if j % 2 == 0 else d1
            o = (j // 2) * CELL_SIZE
            out += src[o:o + min(CELL_SIZE, total - j * CELL_SIZE)]
        return bytes(out)

    def fsck(self, path):
        return self.nn.call("get_block_locations", path, None)


def dn_stats(port):
    s = connect("127.0.0.1", port)
    send_header(s, {"op": "STATS"})
    r = recv_header(s)
    s.close()
    return r
