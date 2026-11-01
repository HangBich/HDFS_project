"""Thí nghiệm 1: TeraGen -> TeraSort -> TeraValidate trên hdfs:// và gs:// (mô phỏng).

Bộ máy MapReduce thu nhỏ: 3 slot (mỗi slot tương ứng một worker), map đọc từng split,
sắp xếp và phân vùng theo các điểm chia lấy mẫu (TotalOrderPartitioner), reduce gộp
và ghi kết quả. Kết quả được chốt bằng FileOutputCommitter thuật toán v1
(đổi tên khi task commit và khi job commit).
"""
import csv
import json
import os
import shutil
import sys
import time
from multiprocessing import get_context

import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "minihdfs"))
from cluster import Cluster          # noqa: E402
from fsadapter import make_fs        # noqa: E402

LOCAL = os.path.join(BASE, "data", "local")
WORKERS = ["w1", "w2", "w3"]
REC = 100
N_MAPS = 8
REC_PER_MAP = 1_342_177              # 134.217.700 byte, vừa đúng 1 block 128 MiB
N_RED = 3
if os.environ.get("E1_SMALL"):
    N_MAPS, REC_PER_MAP = 3, 200_000


def req_delta(fs, before):
    now = fs.requests()
    return {k: now.get(k, 0) - before.get(k, 0) for k in now}


# ------------------------------------------------------------------ committer v1
def attempt_path(out, tid, name):
    return f"{out}/_temporary/0/_temporary/attempt_{tid}/{name}"


def task_commit(fs, out, tid):
    fs.rename(f"{out}/_temporary/0/_temporary/attempt_{tid}", f"{out}/_temporary/0/task_{tid}")


def job_commit(fs, out, tids):
    for tid in tids:
        for p, _ in fs.files(f"{out}/_temporary/0/task_{tid}"):
            fs.rename(p, f"{out}/{p.split('/')[-1]}")
    fs.delete(f"{out}/_temporary")
    fs.write(f"{out}/_SUCCESS", b"")


# ------------------------------------------------------------------ TeraGen
def teragen_task(args):
    spec, out, tid, node = args
    fs = make_fs(spec, node)
    b = fs.requests()
    t0 = time.time()
    rng = np.random.default_rng(1000 + tid)
    n = REC_PER_MAP
    rec = np.empty((n, REC), dtype=np.uint8)
    rec[:, :10] = rng.integers(0, 256, (n, 10), dtype=np.uint8)          # khóa 10 byte
    rec[:, 10:12] = (0x00, 0x11)
    rows = np.arange(tid * n, (tid + 1) * n, dtype=">u8")
    rec[:, 12:20] = rows.view(np.uint8).reshape(n, 8)                      # số thứ tự bản ghi
    rec[:, 20:98] = (np.arange(78, dtype=np.uint8) % 26 + 65)              # phần đệm
    rec[:, 98:100] = (13, 10)
    data = rec.reshape(-1)
    tg = time.time() - t0
    k8 = rec[:, :8].copy().view(">u8").ravel()
    chk = (int(k8.astype(np.uint64).sum(dtype=np.uint64)), int(rows.astype(np.uint64).sum()), n)
    t1 = time.time()
    fs.write(attempt_path(out, tid, f"part-m-{tid:05d}"), data)
    tw = time.time() - t1
    task_commit(fs, out, tid)
    return {"tid": tid, "gen": tg, "write": tw, "chk": chk, "req": req_delta(fs, b)}


# ------------------------------------------------------------------ TeraSort
def keys_of(rec):
    k8 = rec[:, :8].copy().view(">u8").ravel()
    k2 = rec[:, 8:10].copy().view(">u2").ravel()
    return k8, k2


def map_task(args):
    spec, path, j, node, splits, job = args
    fs = make_fs(spec, node)
    b = fs.requests()
    t0 = time.time()
    data = fs.read(path)
    tr = time.time() - t0
    rec = np.frombuffer(data, dtype=np.uint8).reshape(-1, REC)
    k8, k2 = keys_of(rec)
    order = np.lexsort((k2, k8))
    srt = rec[order]
    part = np.searchsorted(splits, k8[order], side="right")
    bounds = np.searchsorted(part, np.arange(N_RED + 1)) * REC
    d = os.path.join(LOCAL, job)
    os.makedirs(d, exist_ok=True)
    fn = os.path.join(d, f"map_{j}.out")
    with open(fn, "wb") as f:                     # đầu ra map ghi xuống đĩa cục bộ (giống spill)
        f.write(memoryview(srt.reshape(-1)))
    with open(fn + ".index", "w") as f:
        json.dump([int(x) for x in bounds], f)
    return {"j": j, "read": tr, "total": time.time() - t0, "req": req_delta(fs, b)}


def reduce_task(args):
    spec, out, r, node, nmaps, job = args
    fs = make_fs(spec, node)
    b = fs.requests()
    t0 = time.time()
    d = os.path.join(LOCAL, job)
    segs = []
    for j in range(nmaps):                        # shuffle: lấy phân vùng r từ mọi map
        fn = os.path.join(d, f"map_{j}.out")
        bounds = json.load(open(fn + ".index"))
        with open(fn, "rb") as f:
            f.seek(bounds[r])
            segs.append(f.read(bounds[r + 1] - bounds[r]))
    rec = np.frombuffer(b"".join(segs), dtype=np.uint8).reshape(-1, REC)
    k8, k2 = keys_of(rec)
    srt = rec[np.lexsort((k2, k8))]
    t1 = time.time()
    fs.write(attempt_path(out, f"r{r}", f"part-r-{r:05d}"), srt.reshape(-1), repl=1)  # TeraSort: 1 bản
    tw = time.time() - t1
    task_commit(fs, out, f"r{r}")
    return {"r": r, "write": tw, "total": time.time() - t0, "n": len(rec), "req": req_delta(fs, b)}


# ------------------------------------------------------------------ TeraValidate
def validate_task(args):
    spec, path, node = args
    fs = make_fs(spec, node)
    b = fs.requests()
    data = fs.read(path)
    rec = np.frombuffer(data, dtype=np.uint8).reshape(-1, REC)
    k8, k2 = keys_of(rec)
    ok = bool(np.all((k8[1:] > k8[:-1]) | ((k8[1:] == k8[:-1]) & (k2[1:] >= k2[:-1]))))
    rows = rec[:, 12:20].copy().view(">u8").ravel()
    chk = (int(k8.astype(np.uint64).sum(dtype=np.uint64)), int(rows.astype(np.uint64).sum()), len(rec))
    return {"path": path, "ok": ok, "first": (int(k8[0]), int(k2[0])), "last": (int(k8[-1]), int(k2[-1])),
            "chk": chk, "req": req_delta(fs, b)}


def sum_req(results):
    tot = {}
    for r in results:
        for k, v in r["req"].items():
            tot[k] = tot.get(k, 0) + v
    return tot


def preferred_node(spec, path, rr):
    if spec["type"] != "hdfs":
        return WORKERS[rr % 3]
    fs = make_fs(spec, None)
    loc = fs.c.fsck(path)
    return loc["blocks"][0]["locs"][0][0] if loc["blocks"] else WORKERS[rr % 3]


def run_once(pool, spec, base, run_id):
    gen_out, sort_out = f"{base}/tera-in", f"{base}/tera-out"
    job = f"job_{run_id}"
    shutil.rmtree(os.path.join(LOCAL, job), ignore_errors=True)
    fsd = make_fs(spec, None)
    res = {"run": run_id}
    req = {}

    # TeraGen
    t0 = time.time()
    g = pool.map(teragen_task, [(spec, gen_out, i, WORKERS[i % 3]) for i in range(N_MAPS)], chunksize=1)
    res["teragen_tasks"] = time.time() - t0
    b = fsd.requests()
    t1 = time.time()
    job_commit(fsd, gen_out, list(range(N_MAPS)))
    res["teragen_commit"] = time.time() - t1
    res["teragen"] = time.time() - t0
    req["teragen"] = sum_req(g)
    req["teragen_commit"] = req_delta(fsd, b)
    chk_in = tuple(int(x) for x in np.sum([r["chk"] for r in g], axis=0, dtype=object))

    # TeraSort: lấy mẫu
    t0 = time.time()
    inputs = sorted(p for p, _ in fsd.files(gen_out) if "part-" in p)
    samples = []
    for p in inputs[:10]:
        rec = np.frombuffer(fsd.read_range(p, 0, 10_000 * REC), dtype=np.uint8).reshape(-1, REC)
        samples.append(keys_of(rec)[0])
    s = np.sort(np.concatenate(samples))
    splits = s[[len(s) * i // N_RED for i in range(1, N_RED)]]
    res["terasort_sample"] = time.time() - t0
    # map (ưu tiên chạy trên node có dữ liệu)
    t1 = time.time()
    m = pool.map(map_task, [(spec, p, j, preferred_node(spec, p, j), splits, job)
                            for j, p in enumerate(inputs)], chunksize=1)
    res["terasort_map"] = time.time() - t1
    req["terasort_map"] = sum_req(m)
    t2 = time.time()
    r = pool.map(reduce_task, [(spec, sort_out, k, WORKERS[k % 3], len(inputs), job)
                               for k in range(N_RED)], chunksize=1)
    res["terasort_reduce"] = time.time() - t2
    req["terasort_reduce"] = sum_req(r)
    b = fsd.requests()
    t3 = time.time()
    job_commit(fsd, sort_out, [f"r{k}" for k in range(N_RED)])
    res["terasort_commit"] = time.time() - t3
    req["terasort_commit"] = req_delta(fsd, b)
    res["terasort"] = time.time() - t0

    # TeraValidate
    t0 = time.time()
    outs = sorted(p for p, _ in fsd.files(sort_out) if "part-" in p)
    v = pool.map(validate_task, [(spec, p, WORKERS[i % 3]) for i, p in enumerate(outs)], chunksize=1)
    ok = all(x["ok"] for x in v) and all(v[i]["last"] <= v[i + 1]["first"] for i in range(len(v) - 1))
    chk_out = tuple(int(x) for x in np.sum([x["chk"] for x in v], axis=0, dtype=object))
    res["teravalidate"] = time.time() - t0
    req["teravalidate"] = sum_req(v)
    res["valid"] = ok and (chk_in[2] == chk_out[2]) and (chk_in[1] == chk_out[1])
    res["records"] = chk_out[2]
    # dọn dẹp
    fsd.delete(base)
    shutil.rmtree(os.path.join(LOCAL, job), ignore_errors=True)
    from objstore import purge_trash
    purge_trash()
    return res, req


def main():
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    outdir = os.path.join(BASE, "results")
    os.makedirs(outdir, exist_ok=True)
    c = Cluster("e1", nn_port=8020)
    # topology: w1, w2 ở rack1; w3 ở rack2. Đĩa mỗi worker giới hạn 240 MiB/s (pd-standard 2000 GB)
    for w, rack in zip(WORKERS, ["/rack1", "/rack1", "/rack2"]):
        c.add_dn(w, rack, capacity=2000 * 2**30, mbps=240)
    from objstore import ObjStore
    ObjStore("bucket-e1").ensure_bucket()
    backends = {
        "hdfs": {"type": "hdfs", "port": 8020, "repl": 3},
        "gs_local": {"type": "obj", "bucket": "bucket-e1", "L0": 0.0, "d": 0.0},
        "gs_cloud": {"type": "obj", "bucket": "bucket-e1", "L0": 0.030, "d": 0.020},
    }
    ctx = get_context("fork")
    pool = ctx.Pool(3)
    rows, reqs = [], {}
    # chạy làm nóng (không tính)
    for name, spec in backends.items():
        print("warmup", name, flush=True)
        run_once(pool, spec, f"/warm_{name}", f"warm_{name}")
    for i in range(runs):
        for name, spec in backends.items():
            res, req = run_once(pool, spec, f"/run{i}_{name}", f"{name}_{i}")
            res["backend"] = name
            rows.append(res)
            reqs[f"{name}_{i}"] = req
            print(json.dumps(res), flush=True)
            print(json.dumps(req), flush=True)
    pool.close()
    keys = ["backend", "run", "teragen_tasks", "teragen_commit", "teragen", "terasort_sample",
            "terasort_map", "terasort_reduce", "terasort_commit", "terasort", "teravalidate", "valid", "records"]
    with open(f"{outdir}/e1_terasort.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in keys})
    json.dump(reqs, open(f"{outdir}/e1_requests.json", "w"), indent=1)
    c.stop()


if __name__ == "__main__":
    try:
        main()
    finally:
        os.system(f"sh {os.path.join(BASE, 'killcluster.sh')}")
