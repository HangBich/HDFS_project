import os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
"""Khởi động / dừng cụm mini-HDFS (1 NameNode + nhiều DataNode, mỗi node một tiến trình)."""
import os
import shutil
import signal
import subprocess
import sys
import time

from common import RpcClient

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.join(ROOT, "data/hdfs")
ENV = dict(os.environ, LD_PRELOAD="/usr/lib/x86_64-linux-gnu/libjemalloc.so.2",
           MALLOC_CONF="dirty_decay_ms:5000,muzzy_decay_ms:5000")   # node chạy lâu: trả lại RAM sau 5 s


class Cluster:
    def __init__(self, name, nn_port=8020, dead_timeout=6.0, fresh=True):
        self.name = name
        self.root = os.path.join(BASE, name)
        if fresh and os.path.exists(self.root):
            shutil.rmtree(self.root)
        os.makedirs(self.root, exist_ok=True)
        self.nn_port = nn_port
        self.procs = {}
        self.dn_ports = {}
        log = open(os.path.join(self.root, "namenode.log"), "a")
        self.procs["nn"] = subprocess.Popen([sys.executable, os.path.join(HERE, "namenode.py"),
                                             str(nn_port), str(dead_timeout)],
                                            cwd=HERE, stdout=log, stderr=subprocess.STDOUT, env=ENV)
        for _ in range(100):
            try:
                RpcClient(("127.0.0.1", nn_port)).call("ping")
                break
            except Exception:
                time.sleep(0.1)
        self.nn = RpcClient(("127.0.0.1", nn_port))

    def add_dn(self, dnid, rack="/default-rack", capacity=50 * 2**30, mbps=0, hb=1.0):
        port = self.nn_port + 100 + len(self.dn_ports) + 1
        d = os.path.join(self.root, dnid)
        log = open(os.path.join(self.root, f"{dnid}.log"), "a")
        self.procs[dnid] = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "datanode.py"), "--id", dnid, "--port", str(port),
             "--rack", rack, "--dir", d, "--nn", str(self.nn_port), "--capacity", str(capacity),
             "--mbps", str(mbps), "--hb", str(hb)], cwd=HERE, stdout=log, stderr=subprocess.STDOUT, env=ENV)
        self.dn_ports[dnid] = port
        for _ in range(100):
            if dnid in self.nn.call("dn_report"):
                return port
            time.sleep(0.1)
        raise RuntimeError(f"{dnid} không khởi động được")

    def kill_dn(self, dnid):
        p = self.procs.pop(dnid)
        p.send_signal(signal.SIGKILL)
        p.wait()

    def dn_dir_bytes(self, dnid):
        d = os.path.join(self.root, dnid, "current")
        return sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if not f.endswith(".tmp"))

    def stop(self, remove=True):
        for p in self.procs.values():
            p.send_signal(signal.SIGKILL)
            p.wait()
        self.procs.clear()
        self.nn.close()
        if remove:
            shutil.rmtree(self.root, ignore_errors=True)
