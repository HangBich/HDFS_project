"""Thí nghiệm 4: nhân bản 2, nhân bản 3, erasure coding XOR-2-1 trên 3 DataNode.

Đo: thời gian ghi, dung lượng thật trên đĩa các DataNode, thời gian đọc bình thường,
thời gian đọc khi tắt 1 DataNode (trước khi NameNode kịp coi node đó là chết),
và (bổ sung) thời gian + lưu lượng đọc để phục hồi đủ dư thừa sang một node mới.
"""
import csv
import hashlib
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, "/home/claude/sim/minihdfs")
from cluster import Cluster            # noqa: E402
from client import HdfsClient, dn_stats  # noqa: E402

RUNS = int(os.environ.get("E4_RUNS", 3))
MBPS = float(os.environ.get("E4_MBPS", 100))
N_FILES, FSIZE = 4, int(os.environ.get("E4_FSIZE", 256 * 2**20))
OUT = "/home/claude/sim/results"
CONFIGS = [("repl2", 2, None), ("repl3", 3, None), ("ec_xor21", 1, "XOR-2-1")]


def disk_read_total(c, dns):
    return sum(dn_stats(c.dn_ports[d])["disk_r"] for d in dns)


def one(cfg, run, payload, digests):
    name, repl, ec = cfg
    c = Cluster(f"e4_{name}", nn_port=8022, dead_timeout=600)
    for w, rack in [("w1", "/rack1"), ("w2", "/rack1"), ("w3", "/rack2")]:
        c.add_dn(w, rack, mbps=MBPS)
    cl = HdfsClient(8022, node=None)          # client đặt trên master (ngoài các DataNode)
    t = time.time()
    for i in range(N_FILES):
        cl.write(f"/e4/f{i}", payload[i], repl=repl, ec=ec)
    t_write = time.time() - t
    time.sleep(1.5)
    raw = {w: c.dn_dir_bytes(w) for w in ("w1", "w2", "w3")}
    t = time.time()
    ok = all(hashlib.md5(cl.read(f"/e4/f{i}")).hexdigest() == digests[i] for i in range(N_FILES))
    t_read = time.time() - t
    # tắt 1 DataNode
    c.kill_dn("w3")
    cl2 = HdfsClient(8022, node=None)
    t = time.time()
    ok2 = all(hashlib.md5(cl2.read(f"/e4/f{i}")).hexdigest() == digests[i] for i in range(N_FILES))
    t_degraded = time.time() - t
    # phục hồi sang node mới w4
    c.add_dn("w4", "/rack2", mbps=MBPS)
    r0 = disk_read_total(c, ["w1", "w2"])
    c.nn.call("set_dead_timeout", 2.0)
    while c.nn.call("dn_report")["w3"]["alive"]:      # chờ NameNode phát hiện w3 đã chết
        time.sleep(0.05)
    t = time.time()
    while True:
        m = c.nn.call("metrics")
        if m["under_replicated"] == 0 and m["missing"] == 0:
            break
        time.sleep(0.1)
    t_recover = time.time() - t
    recover_read = disk_read_total(c, ["w1", "w2"]) - r0
    lost = raw["w3"]
    row = {"config": name, "run": run, "write_s": t_write, "raw_bytes": sum(raw.values()),
           "overhead": sum(raw.values()) / (N_FILES * FSIZE), "read_s": t_read, "degraded_s": t_degraded,
           "failover": cl2.stats["failover"], "degraded_groups": cl2.stats["degraded"],
           "recover_s": t_recover, "lost_bytes": lost, "recover_read_bytes": recover_read,
           "ok": ok and ok2, "raw_w1": raw["w1"], "raw_w2": raw["w2"], "raw_w3": raw["w3"]}
    c.stop()
    return row


def make_payload():
    rng = np.random.default_rng(7)
    payload = [rng.integers(0, 256, FSIZE, dtype=np.uint8).tobytes() for _ in range(N_FILES)]
    return payload, [hashlib.md5(p).hexdigest() for p in payload]


def main():
    """Mỗi (cấu hình, lần đo) chạy trong một tiến trình con riêng để RAM được giải phóng hoàn toàn."""
    import subprocess
    os.makedirs(OUT, exist_ok=True)
    rows = []
    for run in range(1, RUNS + 1):          # mỗi lần đo đã là một cụm và tiến trình mới hoàn toàn
        for cfg in CONFIGS:
            out = subprocess.run([sys.executable, __file__, "--one", cfg[0], str(run)],
                                 capture_output=True, text=True)
            os.system("/home/claude/sim/killcluster.sh")
            line = [l for l in out.stdout.splitlines() if l.startswith("{")]
            if out.returncode != 0 or not line:
                raise RuntimeError(f"lỗi ở {cfg[0]} lần {run}: {out.stderr[-2000:]}")
            row = json.loads(line[-1])
            print(json.dumps(row), flush=True)
            if run > 0:
                rows.append(row)
    with open(f"{OUT}/e4_ec.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main_one(name, run):
    cfg = [c for c in CONFIGS if c[0] == name][0]
    payload, digests = make_payload()
    print(json.dumps(one(cfg, run, payload, digests)), flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--one":
        main_one(sys.argv[2], int(sys.argv[3]))
    else:
        try:
            main()
        finally:
            os.system("/home/claude/sim/killcluster.sh")
