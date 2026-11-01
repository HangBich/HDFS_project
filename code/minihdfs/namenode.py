"""NameNode của mini-HDFS.

Quản lý namespace (cây thư mục) trong RAM, ánh xạ tệp -> block -> DataNode,
chính sách đặt bản sao Rack Awareness, theo dõi heartbeat, nhân bản lại khi
DataNode chết, xóa bản sao thừa, khôi phục block EC XOR-2-1.
"""
import itertools
import random
import sys
import threading
import time
import traceback
from multiprocessing.connection import Listener

from common import AUTHKEY, DEFAULT_BLOCK_SIZE, CELL_SIZE


def split_path(p):
    return [x for x in p.split("/") if x]


def deep_size(obj, seen=None):
    """Ước lượng dung lượng RAM (byte) của một cấu trúc Python."""
    if seen is None:
        seen = set()
    oid = id(obj)
    if oid in seen:
        return 0
    seen.add(oid)
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        for k, v in obj.items():
            size += deep_size(k, seen) + deep_size(v, seen)
    elif isinstance(obj, (list, tuple, set, frozenset)):
        for x in obj:
            size += deep_size(x, seen)
    return size


class NameNode:
    def __init__(self, port, dead_timeout=6.0, seed=1):
        self.port = port
        self.dead_timeout = dead_timeout
        self.lock = threading.RLock()
        self.rand = random.Random(seed)
        self.root = {"t": "d", "c": {}, "mtime": time.time()}
        self.blocks = {}      # bid -> {size, repl, locs:set, file, ec: None|(gid, idx)}
        self.groups = {}      # gid -> {size, internal:[bid0,bid1,bidp], file}
        self.dns = {}         # dnid -> info
        self.ids = itertools.count(1_073_741_825)   # id block bắt đầu giống HDFS
        self.pending = {}     # bid -> thời điểm lên lịch nhân bản
        self.delete_hint = {}  # bid -> dn nên xóa sau khi di chuyển (balancer)
        self.stats = {"rpc": 0, "replications": 0, "reconstructions": 0, "deletes": 0}
        threading.Thread(target=self._monitor, daemon=True).start()

    # ------------------------------------------------------------------ namespace
    def _lookup(self, path, create_dirs=False):
        node = self.root
        for name in split_path(path):
            if node["t"] != "d":
                raise FileNotFoundError(path)
            nxt = node["c"].get(name)
            if nxt is None:
                if not create_dirs:
                    return None
                nxt = {"t": "d", "c": {}, "mtime": time.time()}
                node["c"][name] = nxt
            node = nxt
        return node

    def _parent(self, path, create=True):
        parts = split_path(path)
        parent = self._lookup("/" + "/".join(parts[:-1]), create_dirs=create)
        return parent, parts[-1]

    def mkdirs(self, path):
        with self.lock:
            self._lookup(path, create_dirs=True)
            return True

    def create(self, path, repl=3, block_size=DEFAULT_BLOCK_SIZE, ec=None, overwrite=True):
        with self.lock:
            parent, name = self._parent(path)
            if name in parent["c"]:
                if not overwrite:
                    raise FileExistsError(path)
                self._delete_node(parent["c"][name])
            parent["c"][name] = {"t": "f", "b": [], "repl": repl, "bs": block_size,
                                 "ec": ec, "size": 0, "done": False, "mtime": time.time()}
            return True

    def add_block(self, path, client_node=None, excluded=()):
        with self.lock:
            f = self._lookup(path)
            if f["ec"]:
                gid = next(self.ids)
                targets = self._choose_ec_targets(3, excluded)
                internal = []
                for i, dn in enumerate(targets):
                    bid = next(self.ids)
                    self.blocks[bid] = {"size": 0, "repl": 1, "locs": set(), "file": path, "ec": (gid, i)}
                    internal.append((bid, self._addr(dn)))
                self.groups[gid] = {"size": 0, "internal": [b for b, _ in internal], "file": path}
                f["b"].append(gid)
                return {"gid": gid, "internal": internal}
            bid = next(self.ids)
            targets = self._choose_targets(f["repl"], client_node, set(excluded))
            self.blocks[bid] = {"size": 0, "repl": f["repl"], "locs": set(), "file": path, "ec": None}
            f["b"].append(bid)
            return {"bid": bid, "targets": [self._addr(d) for d in targets]}

    def complete(self, path, size, groups=None):
        """Hoàn tất tệp: chỉ thành công khi mọi block đã có ít nhất 1 bản sao được báo cáo.
        groups (chỉ với EC): danh sách (kích thước dữ liệu, [kích thước 3 block nội bộ])."""
        deadline = time.time() + 120
        while True:
            with self.lock:
                f = self._lookup(path)
                ok = True
                for i, b in enumerate(f["b"]):
                    if f["ec"]:
                        for ib, exp in zip(self.groups[b]["internal"], groups[i][1]):
                            if exp > 0 and not self.blocks[ib]["locs"]:
                                ok = False
                    elif not self.blocks[b]["locs"]:
                        ok = False
                if ok:
                    f["size"] = size
                    f["done"] = True
                    if groups:
                        for gid, (gs, _) in zip(f["b"], groups):
                            self.groups[gid]["size"] = gs
                    return True
            if time.time() > deadline:
                raise TimeoutError(f"complete {path}")
            time.sleep(0.003)

    def get_block_locations(self, path, client_node=None):
        with self.lock:
            f = self._lookup(path)
            if f is None or f["t"] != "f":
                raise FileNotFoundError(path)
            out = []
            for b in f["b"]:
                if f["ec"]:
                    g = self.groups[b]
                    out.append({"gid": b, "size": g["size"],
                                "internal": [(ib, self.blocks[ib]["size"],
                                              [self._addr(d) for d in self.blocks[ib]["locs"]])
                                             for ib in g["internal"]]})
                else:
                    blk = self.blocks[b]
                    locs = list(blk["locs"])
                    self.rand.shuffle(locs)
                    locs.sort(key=lambda d: self._distance(client_node, d))
                    out.append({"bid": b, "size": blk["size"], "locs": [self._addr(d) for d in locs]})
            return {"size": f["size"], "ec": f["ec"], "blocks": out}

    def get_file_info(self, path):
        with self.lock:
            n = self._lookup(path)
            if n is None:
                return None
            return self._status(path.rstrip("/").split("/")[-1], n)

    def _status(self, name, n):
        if n["t"] == "d":
            return {"name": name, "type": "d", "size": 0, "repl": 0}
        return {"name": name, "type": "f", "size": n["size"], "repl": n["repl"] if not n["ec"] else 0,
                "ec": n["ec"], "blocks": len(n["b"])}

    def list_status(self, path, start_after=None, limit=1000):
        """Liệt kê thư mục theo lô (giống dfs.ls.limit = 1000 của HDFS)."""
        with self.lock:
            d = self._lookup(path)
            if d is None:
                raise FileNotFoundError(path)
            if d["t"] == "f":
                return {"entries": [self._status(split_path(path)[-1], d)], "more": False}
            names = sorted(d["c"])
            if start_after is not None:
                import bisect
                names = names[bisect.bisect_right(names, start_after):]
            batch = names[:limit]
            return {"entries": [self._status(n, d["c"][n]) for n in batch], "more": len(names) > limit}

    def content_summary(self, path):
        with self.lock:
            d = self._lookup(path)
            cnt = {"dirs": 0, "files": 0, "bytes": 0}

            def walk(n):
                if n["t"] == "d":
                    cnt["dirs"] += 1
                    for c in n["c"].values():
                        walk(c)
                else:
                    cnt["files"] += 1
                    cnt["bytes"] += n["size"]
            walk(d)
            return cnt

    def rename(self, src, dst):
        """Đổi tên = chỉ sửa cây thư mục trong RAM, không đụng tới dữ liệu (O(1))."""
        with self.lock:
            sp, sn = self._parent(src, create=False)
            node = sp["c"].pop(sn)
            dp = self._lookup(dst)
            if dp is not None and dp["t"] == "d":
                dp["c"][sn] = node
            else:
                dpar, dn = self._parent(dst)
                dpar["c"][dn] = node
            self._rename_blocks(node, dst)
            return True

    def _rename_blocks(self, node, newpath):
        # chỉ cập nhật tham chiếu ngược phục vụ in báo cáo; không ảnh hưởng dữ liệu
        return

    def delete(self, path, recursive=True):
        with self.lock:
            parent, name = self._parent(path, create=False)
            node = parent["c"].pop(name, None)
            if node is None:
                return False
            self._delete_node(node)
            return True

    def _delete_node(self, node):
        if node["t"] == "d":
            for c in list(node["c"].values()):
                self._delete_node(c)
            return
        for b in node["b"]:
            bids = self.groups.pop(b)["internal"] if node["ec"] else [b]
            for bid in bids:
                blk = self.blocks.pop(bid, None)
                if blk:
                    for dn in blk["locs"]:
                        self.dns[dn]["cmds"].append(("DELETE", bid))
                        self.stats["deletes"] += 1

    def set_replication(self, path, repl):
        with self.lock:
            f = self._lookup(path)
            f["repl"] = repl
            for b in f["b"]:
                self.blocks[b]["repl"] = repl
            return True

    # ------------------------------------------------------------------ DataNode
    def register(self, dnid, host, port, rack, capacity):
        with self.lock:
            self.dns[dnid] = {"host": host, "port": port, "rack": rack, "capacity": capacity,
                              "used": 0, "last": time.time(), "alive": True, "cmds": [],
                              "blocks": set()}
            return True

    def heartbeat(self, dnid, used, capacity):
        with self.lock:
            d = self.dns[dnid]
            d["used"], d["capacity"], d["last"] = used, capacity, time.time()
            cmds, d["cmds"] = d["cmds"], []
            if not d["alive"]:
                d["alive"] = True
                cmds = [("BLOCKREPORT",)] + cmds
            return cmds

    def block_report(self, dnid, items):
        with self.lock:
            for bid, size in items:
                blk = self.blocks.get(bid)
                if blk is None:
                    self.dns[dnid]["cmds"].append(("DELETE", bid))
                    continue
                blk["locs"].add(dnid)
                blk["size"] = max(blk["size"], size)
                self.dns[dnid]["blocks"].add(bid)
            for bid in [b for b, _ in items]:
                blk = self.blocks.get(bid)
                if blk and blk["ec"] is None and len(self._live(blk["locs"])) > blk["repl"]:
                    self._remove_excess(bid)
            return True

    def block_received(self, dnid, bid, size):
        with self.lock:
            blk = self.blocks.get(bid)
            if blk is None:  # block đã bị xóa trong lúc ghi
                self.dns[dnid]["cmds"].append(("DELETE", bid))
                return False
            blk["locs"].add(dnid)
            blk["size"] = max(blk["size"], size)
            self.dns[dnid]["blocks"].add(bid)
            self.pending.pop(bid, None)
            if blk["ec"] is None and len(self._live(blk["locs"])) > blk["repl"]:
                self._remove_excess(bid)
            return True

    def block_deleted(self, dnid, bid):
        with self.lock:
            self.dns[dnid]["blocks"].discard(bid)
            return True

    # ------------------------------------------------------------------ chính sách đặt bản sao
    def _addr(self, dn):
        d = self.dns[dn]
        return (dn, d["host"], d["port"])

    def _rack(self, dn):
        return self.dns[dn]["rack"] if dn in self.dns else None

    def _distance(self, client, dn):
        if client == dn:
            return 0
        if client in self.dns and self._rack(client) == self._rack(dn):
            return 2
        return 4

    def _live(self, dns):
        return [d for d in dns if self.dns[d]["alive"]]

    def _good(self, excluded, need=0):
        return [d for d, i in self.dns.items()
                if i["alive"] and d not in excluded and i["capacity"] - i["used"] > need]

    def _choose_targets(self, r, writer, excluded, existing=()):
        """BlockPlacementPolicyDefault: bản 1 tại node của client (nếu client là DataNode),
        bản 2 ở rack khác, bản 3 cùng rack với bản 2 nhưng khác node."""
        chosen = list(existing)
        res = []
        excl = set(excluded) | set(existing)

        def pick(cands):
            cands = [c for c in cands if c not in excl]
            if not cands:
                return None
            c = self.rand.choice(cands)
            excl.add(c)
            res.append(c)
            chosen.append(c)
            return c

        good = self._good(set())
        while len(res) < r:
            n = len(chosen)
            if n == 0:
                if writer in good and writer not in excl:
                    excl.add(writer); res.append(writer); chosen.append(writer)
                elif pick(good) is None:
                    break
            elif n == 1:
                r0 = self._rack(chosen[0])
                if pick([d for d in good if self._rack(d) != r0]) is None and pick(good) is None:
                    break
            elif n == 2:
                r1 = self._rack(chosen[1])
                if self._rack(chosen[0]) == r1:
                    c = pick([d for d in good if self._rack(d) != r1])
                else:
                    c = pick([d for d in good if self._rack(d) == r1])
                if c is None and pick(good) is None:
                    break
            else:
                if pick(good) is None:
                    break
        return res

    def _choose_ec_targets(self, n, excluded):
        """Rải các block nội bộ của một nhóm EC lên càng nhiều rack càng tốt."""
        good = [d for d in self._good(set(excluded))]
        self.rand.shuffle(good)
        racks = {}
        for d in good:
            racks.setdefault(self._rack(d), []).append(d)
        order = []
        while len(order) < n and any(racks.values()):
            for rk in list(racks):
                if racks[rk]:
                    order.append(racks[rk].pop())
                    if len(order) == n:
                        break
        if len(order) < n:
            raise RuntimeError("không đủ DataNode cho EC")
        return order

    def _remove_excess(self, bid):
        blk = self.blocks[bid]
        live = self._live(blk["locs"])
        while len(live) > blk["repl"]:
            hint = self.delete_hint.pop(bid, None)
            if hint in live:
                victim = hint
            else:
                # ưu tiên xóa ở rack có nhiều bản sao nhất, node dùng nhiều nhất
                def key(d):
                    same = sum(1 for x in live if self._rack(x) == self._rack(d))
                    return (same, self.dns[d]["used"] / max(1, self.dns[d]["capacity"]))
                victim = max(live, key=key)
            live.remove(victim)
            blk["locs"].discard(victim)
            self.dns[victim]["blocks"].discard(bid)
            self.dns[victim]["cmds"].append(("DELETE", bid))
            self.stats["deletes"] += 1

    # ------------------------------------------------------------------ giám sát
    def _monitor(self):
        while True:
            time.sleep(0.5)
            try:
                with self.lock:
                    now = time.time()
                    for dn, d in self.dns.items():
                        if d["alive"] and now - d["last"] > self.dead_timeout:
                            d["alive"] = False
                            for bid in list(d["blocks"]):
                                if bid in self.blocks:
                                    self.blocks[bid]["locs"].discard(dn)
                            d["blocks"] = set()
                    self._schedule_replication(now)
            except Exception:
                traceback.print_exc()

    def _schedule_replication(self, now):
        for bid, blk in list(self.blocks.items()):
            if bid in self.pending and now - self.pending[bid] < 30:
                continue
            live = self._live(blk["locs"])
            if blk["ec"] is not None:
                if live or blk["size"] == 0:
                    continue
                gid, idx = blk["ec"]
                g = self.groups.get(gid)
                if g is None:
                    continue
                others = []
                for ib in g["internal"]:
                    if ib == bid:
                        continue
                    l2 = self._live(self.blocks[ib]["locs"])
                    if not l2:
                        break
                    others.append((ib, self._addr(l2[0])))
                else:
                    used_dns = set()
                    for ib in g["internal"]:
                        used_dns |= self.blocks[ib]["locs"]
                    cands = self._good(used_dns)
                    if not cands:
                        continue
                    target = self.rand.choice(cands)
                    self.dns[target]["cmds"].append(("RECONSTRUCT", bid, others, blk["size"]))
                    self.pending[bid] = now
                    self.stats["reconstructions"] += 1
                continue
            if 0 < len(live) < blk["repl"]:
                targets = self._choose_targets(blk["repl"] - len(live), None, set(), existing=live)
                if not targets:
                    continue
                src = self.rand.choice(live)
                self.dns[src]["cmds"].append(("TRANSFER", bid, [self._addr(t) for t in targets]))
                self.pending[bid] = now
                self.stats["replications"] += 1

    # ------------------------------------------------------------------ công cụ quản trị
    def dn_report(self):
        with self.lock:
            return {dn: {"rack": d["rack"], "used": d["used"], "capacity": d["capacity"],
                         "alive": d["alive"], "blocks": len(d["blocks"])}
                    for dn, d in sorted(self.dns.items())}

    def block_map(self):
        with self.lock:
            return {bid: (b["size"], sorted(b["locs"]), b["repl"]) for bid, b in self.blocks.items()
                    if b["ec"] is None}

    def move_block(self, bid, src, dst):
        """Dùng cho Balancer: chép bản sao từ src sang dst rồi xóa ở src."""
        with self.lock:
            blk = self.blocks.get(bid)
            if blk is None or src not in blk["locs"] or dst in blk["locs"]:
                return False
            self.delete_hint[bid] = src
            self.dns[src]["cmds"].append(("TRANSFER", bid, [self._addr(dst)]))
            self.pending[bid] = time.time()
            return True

    def metrics(self):
        with self.lock:
            files = dirs = 0
            stack = [self.root]
            while stack:
                n = stack.pop()
                if n["t"] == "d":
                    dirs += 1
                    stack.extend(n["c"].values())
                else:
                    files += 1
            under = sum(1 for b in self.blocks.values()
                        if b["ec"] is None and len(self._live(b["locs"])) < b["repl"])
            missing = sum(1 for b in self.blocks.values() if b["size"] > 0 and not self._live(b["locs"]))
            return {"files": files, "dirs": dirs, "blocks": len(self.blocks), "groups": len(self.groups),
                    "objects": files + dirs + len(self.blocks), "under_replicated": under,
                    "missing": missing, **self.stats}

    def namespace_memory(self):
        with self.lock:
            return {"namespace": deep_size(self.root), "blockmap": deep_size(self.blocks) + deep_size(self.groups)}

    def set_dead_timeout(self, t):
        self.dead_timeout = t
        return True

    def ping(self):
        return True


def serve(port, dead_timeout):
    nn = NameNode(port, dead_timeout)
    listener = Listener(("127.0.0.1", port), authkey=AUTHKEY, backlog=256)

    def handle(conn):
        try:
            while True:
                try:
                    method, args, kw = conn.recv()
                except (EOFError, OSError):
                    return
                nn.stats["rpc"] += 1
                try:
                    conn.send((True, getattr(nn, method)(*args, **kw)))
                except Exception as e:
                    conn.send((False, f"{type(e).__name__}: {e}"))
        finally:
            conn.close()

    print(f"NameNode chạy ở cổng {port}", flush=True)
    while True:
        c = listener.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()


if __name__ == "__main__":
    serve(int(sys.argv[1]), float(sys.argv[2]) if len(sys.argv) > 2 else 6.0)
