import os
ROOT = os.path.dirname(os.path.abspath(__file__))
"""Thí nghiệm 5: mở rộng cụm 2 -> 3 worker, đo độ lệch dữ liệu, chạy Balancer, đo lại.

Mỗi DataNode được giới hạn băng thông đĩa (mô phỏng mỗi worker có đĩa riêng), nên thời gian
job quét dữ liệu phụ thuộc vào cách block phân bố giữa các node.
"""
import csv
import json
import os
import statistics
import sys
import threading
import time

import numpy as np

sys.path.insert(0, os.path.join(ROOT, "minihdfs"))
from balancer import balance, report   # noqa: E402
from cluster import Cluster            # noqa: E402
from client import HdfsClient          # noqa: E402

RUNS = int(os.environ.get("E5_RUNS", 3))
MBPS = float(os.environ.get("E5_MBPS", 60))
CAP = int(os.environ.get("E5_CAP", 4 * 2**30))
N_FILES, FSIZE = 16, int(os.environ.get("E5_FSIZE", 128 * 2**20))   # 2 GiB dữ liệu, nhân bản 2
OUT = os.path.join(ROOT, "results")


def scan_job(port, workers, files):
    """Job quét toàn bộ dữ liệu: mỗi worker một task chạy song song, ưu tiên block có bản sao tại chỗ."""
    clients = {w: HdfsClient(port, node=w) for w in workers}
    blocks = []
    for f in files:
        locs = {w: clients[w].nn.call("get_block_locations", f, w)["blocks"] for w in workers}
        for k in range(len(locs[workers[0]])):
            blocks.append({w: locs[w][k] for w in workers})
    lock = threading.Lock()
    remaining = list(range(len(blocks)))
    stat = {"local": 0, "remote": 0}

    def worker(w):
        while True:
            with lock:
                if not remaining:
                    return
                pick = next((i for i in remaining if blocks[i][w]["locs"][0][0] == w), None)
                local = pick is not None
                if pick is None:
                    pick = remaining[0]
                remaining.remove(pick)
                stat["local" if local else "remote"] += 1
            clients[w]._read_replicated(blocks[pick][w])
    t = time.time()
    ths = [threading.Thread(target=worker, args=(w,)) for w in workers]
    [x.start() for x in ths]
    [x.join() for x in ths]
    return time.time() - t, stat


def snapshot(nn, label):
    rep, util, avg = report(nn)
    vals = list(util.values())
    return {"label": label, "util": {k: round(v, 2) for k, v in util.items()}, "avg": round(avg, 2),
            "stdev": round(statistics.pstdev(vals), 2), "spread": round(max(vals) - min(vals), 2),
            "used": {k: v["used"] for k, v in rep.items()}}


def one(run, payload):
    c = Cluster("e5", nn_port=8023, dead_timeout=600)
    c.add_dn("w1", capacity=CAP, mbps=MBPS)
    c.add_dn("w2", capacity=CAP, mbps=MBPS)
    cl = HdfsClient(8023, node=None)
    files = [f"/e5/f{i:02d}" for i in range(N_FILES)]
    for f in files:
        cl.write(f, payload, repl=2)
    time.sleep(2.5)
    snaps = [snapshot(c.nn, "2 worker")]
    t_before, st_before = scan_job(8023, ["w1", "w2"], files)
    c.add_dn("w3", capacity=CAP, mbps=MBPS)            # thêm worker mới (dataproc update)
    time.sleep(2.5)
    snaps.append(snapshot(c.nn, "thêm w3"))
    t_added, st_added = scan_job(8023, ["w1", "w2", "w3"], files)
    bal = balance(c.nn, threshold=10.0)
    snaps.append(snapshot(c.nn, "sau balancer"))
    t_bal, st_bal = scan_job(8023, ["w1", "w2", "w3"], files)
    # ghi thêm dữ liệu mới từ client chạy trên w1 (bản sao thứ nhất luôn nằm tại w1)
    cl1 = HdfsClient(8023, node="w1")
    for i in range(4):
        cl1.write(f"/e5/new{i}", payload, repl=2)
    time.sleep(2.5)
    snaps.append(snapshot(c.nn, "ghi thêm 512 MiB từ w1"))
    c.stop()
    return {"run": run, "scan_2w_s": t_before, "scan_added_s": t_added, "scan_balanced_s": t_bal,
            "loc_2w": st_before, "loc_added": st_added, "loc_bal": st_bal,
            "bal_iterations": bal["iterations"], "bal_moved_bytes": bal["moved_bytes"],
            "bal_moved_blocks": bal["moved_blocks"], "bal_seconds": bal["seconds"],
            "bal_history": bal["history"], "snapshots": snaps}


def main():
    os.makedirs(OUT, exist_ok=True)
    payload = np.random.default_rng(5).integers(0, 256, FSIZE, dtype=np.uint8).tobytes()
    rows = []
    for run in range(RUNS + 1):
        r = one(run, payload)
        print(json.dumps(r), flush=True)
        if run > 0:
            rows.append(r)
    json.dump(rows, open(f"{OUT}/e5_balancer.json", "w"), indent=1)
    with open(f"{OUT}/e5_balancer.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run", "scan_2w_s", "scan_added_s", "scan_balanced_s", "bal_iterations",
                    "bal_moved_bytes", "bal_seconds"] +
                   [f"{s}_{k}" for s in ("s0", "s1", "s2", "s3") for k in ("stdev", "spread")])
        for r in rows:
            w.writerow([r["run"], r["scan_2w_s"], r["scan_added_s"], r["scan_balanced_s"], r["bal_iterations"],
                        r["bal_moved_bytes"], r["bal_seconds"]] +
                       [r["snapshots"][i][k] for i in range(4) for k in ("stdev", "spread")])


if __name__ == "__main__":
    try:
        main()
    finally:
        os.system(os.path.join(ROOT, "killcluster.sh"))
