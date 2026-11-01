"""Giao diện hệ thống tệp chung cho hdfs:// (mini-HDFS) và gs:// (MinIO + độ trễ mô phỏng)."""
from client import HdfsClient
from objstore import ObjStore

_cache = {}


class HdfsFS:
    scheme = "hdfs"

    def __init__(self, port, node=None, repl=3):
        self.c = HdfsClient(port, node)
        self.repl = repl

    def requests(self):
        return {"RPC": self.c.nn.calls}

    def write(self, path, data, repl=None):
        self.c.write(path, data, repl=repl or self.repl)

    def read(self, path):
        return self.c.read(path)

    def read_range(self, path, off, n):
        return self.c.read_range(path, off, n)

    def mkdirs(self, p):
        return self.c.mkdirs(p)

    def rename(self, s, d):
        return self.c.rename(s, d)

    def delete(self, p):
        return self.c.delete(p)

    def exists(self, p):
        return self.c.exists(p)

    def info(self, p):
        return self.c.info(p)

    def files(self, d):
        """Danh sách tệp (đường dẫn, kích thước) trực tiếp trong thư mục d."""
        return [(d.rstrip("/") + "/" + e["name"], e["size"]) for e in self.c.ls(d) if e["type"] == "f"]

    def ls_r(self, d):
        return [(p, e["size"]) for p, e in self.c.ls_r(d) if e["type"] == "f"]

    def count(self, d):
        return self.c.count(d)


class ObjFS:
    scheme = "gs"

    def __init__(self, bucket, L0=0.0, d=0.0, threads=16):
        self.s = ObjStore(bucket, L0=L0, d=d, threads=threads)

    def requests(self):
        return dict(self.s.req)

    def write(self, path, data, repl=None):
        self.s.write(path, data)

    def read(self, path):
        return self.s.read(path)

    def read_range(self, path, off, n):
        return self.s.read_range(path, off, n)

    def mkdirs(self, p):
        return self.s.mkdirs(p)

    def rename(self, s, d):
        return self.s.rename(s, d)

    def delete(self, p):
        return self.s.delete(p)

    def exists(self, p):
        return self.s.exists(p)

    def files(self, d):
        pre = d.strip("/") + "/"
        out = []
        for k, sz in self.s.list_keys(d):
            rest = k[len(pre):]
            if rest and "/" not in rest:
                out.append(("/" + k, sz))
        return out

    def ls_r(self, d):
        return [("/" + k, s) for k, s in self.s.ls_r(d)]

    def count(self, d):
        return self.s.count(d)


def make_fs(spec, node=None):
    key = (tuple(sorted(spec.items())), node)
    fs = _cache.get(key)
    if fs is None:
        if spec["type"] == "hdfs":
            fs = HdfsFS(spec["port"], node, spec.get("repl", 3))
        else:
            fs = ObjFS(spec["bucket"], spec.get("L0", 0.0), spec.get("d", 0.0), spec.get("threads", 16))
        _cache[key] = fs
    return fs
