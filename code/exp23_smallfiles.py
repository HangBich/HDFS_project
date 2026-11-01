import os
ROOT = os.path.dirname(os.path.abspath(__file__))
"""Thí nghiệm 2 (đổi tên, liệt kê thư mục 10.000 tệp) và thí nghiệm 3 (tệp nhỏ).

Dữ liệu: 10.000 tệp văn bản x 100 KB (mô phỏng chứng từ, log), tổng khoảng 1 GB.
TN2: hdfs dfs -count, -ls -R, -mv trên thư mục 10.000 tệp; so với kho đối tượng.
TN3: bố trí tệp rời / HAR / tệp gộp trên HDFS và tệp rời trên kho đối tượng;
     đo số đối tượng NameNode giữ, bộ nhớ metadata, thời gian job WordCount
     (mỗi map task chạy trong một tiến trình mới, giống YARN cấp container).
"""
import csv
import json
import os
import statistics
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import get_context

import numpy as np

sys.path.insert(0, os.path.join(ROOT, "minihdfs"))
import fsadapter                                # noqa: E402
from cluster import Cluster                     # noqa: E402
from fsadapter import HdfsFS, ObjFS, make_fs    # noqa: E402
from objstore import ObjStore, purge_trash      # noqa: E402

N_FILES = int(os.environ.get("E3_N", 10_000))
FILE_SIZE = 100 * 1024
RUNS = int(os.environ.get("E3_RUNS", 3))
OUT = os.path.join(ROOT, "results")
WORKERS = ["w1", "w2", "w3"]
BLOCK = 128 * 1024 * 1024
HDFS = {"type": "hdfs", "port": 8021, "repl": 3}
GS_LOCAL = {"type": "obj", "bucket": "bucket-e23", "L0": 0.0, "d": 0.0}
GS_CLOUD = {"type": "obj", "bucket": "bucket-e23", "L0": 0.030, "d": 0.020}

VOCAB = np.array([f"tu{i:04d}" for i in range(5000)] +
                 ["giao_dich", "tai_khoan", "so_du", "chuyen_khoan", "ma_kh", "ngay", "so_tien"])


def make_file(i):
    rng = np.random.default_rng(i)
    idx = np.minimum(rng.zipf(1.3, 20000) - 1, len(VOCAB) - 1)
    words = VOCAB[idx]
    lines = [" ".join(words[k:k + 12]) for k in range(0, len(words), 12)]
    txt = ("\n".join(lines) + "\n").encode()
    return txt[:FILE_SIZE - 1] + b"\n"


def fname(i):
    return f"f{i:05d}.txt"


# ------------------------------------------------------------------ nạp dữ liệu
def put_chunk(args):
    spec, base, ids, node = args
    fs = make_fs(spec, node)
    for i in ids:
        fs.write(f"{base}/{fname(i)}", make_file(i))
    return len(ids)


def ingest(pool, spec, base, threads=1):
    t0 = time.time()
    if spec["type"] == "hdfs":
        chunks = [(spec, base, list(range(k, N_FILES, 3)), WORKERS[k]) for k in range(3)]
        pool.map(put_chunk, chunks)
    else:
        fs = ObjFS(spec["bucket"], spec["L0"], spec["d"])
        with ThreadPoolExecutor(16) as ex:
            list(ex.map(lambda i: fs.write(f"{base}/{fname(i)}", make_file(i)), range(N_FILES)))
    return time.time() - t0


# ------------------------------------------------------------------ WordCount
def _init_child():
    fsadapter._cache.clear()


def wc_file(args):
    spec, path, node = args
    fs = make_fs(spec, node)
    return Counter(fs.read(path).split())


def wc_har(args):
    spec, har, name, node = args
    fs = make_fs(spec, node)
    idx = {}
    for line in fs.read(f"{har}/_index").decode().splitlines():   # mỗi task phải đọc chỉ mục HAR
        n, part, off, ln = line.split()
        idx[n] = (part, int(off), int(ln))
    part, off, ln = idx[name]
    return Counter(fs.read_range(f"{har}/{part}", off, ln).split())


def wc_split(args):
    spec, path, start, length, total, node = args
    fs = make_fs(spec, node)
    end = min(start + length, total)
    data = fs.read_range(path, start, min(end + 4096, total) - start)
    s = 0 if start == 0 else data.find(b"\n") + 1          # bỏ dòng dở đầu split (giống TextInputFormat)
    e = end - start
    if end < total:                                           # đọc nốt dòng bắt đầu trước hoặc tại điểm cuối split
        e = e + data[e:].find(b"\n") + 1
    return Counter(data[s:e].split())


def run_job(fn, tasks, out_fs, out_path, slots=3):
    """Chạy job: mỗi map task là một tiến trình con mới (fork), tối đa `slots` task cùng lúc,
    giống YARN cấp một container cho mỗi task. Kết quả trả về qua pipe."""
    import pickle
    import selectors
    import traceback
    sel = selectors.DefaultSelector()
    t0 = time.time()
    total = Counter()
    running, idx = 0, 0
    bufs = {}
    while idx < len(tasks) or running:
        while running < slots and idx < len(tasks):
            r, w = os.pipe()
            pid = os.fork()
            if pid == 0:                                   # tiến trình con = một container
                os.close(r)
                code = 0
                try:
                    fsadapter._cache.clear()
                    data = pickle.dumps(fn(tasks[idx]))
                    with os.fdopen(w, "wb") as f:
                        f.write(data)
                except BaseException:
                    traceback.print_exc()
                    code = 1
                os._exit(code)
            os.close(w)
            bufs[r] = (pid, bytearray())
            sel.register(r, selectors.EVENT_READ)
            running += 1
            idx += 1
        for key, _ in sel.select():
            fd = key.fd
            chunk = os.read(fd, 1 << 20)
            if chunk:
                bufs[fd][1].extend(chunk)
                continue
            sel.unregister(fd)
            os.close(fd)
            pid, data = bufs.pop(fd)
            _, status = os.waitpid(pid, 0)
            if status != 0:
                raise RuntimeError("map task lỗi")
            total.update(pickle.loads(bytes(data)))
            running -= 1
    out_fs.write(out_path, "\n".join(f"{w}\t{n}" for w, n in total.most_common()).encode())
    return time.time() - t0, total


def median(xs):
    return statistics.median(xs)


def main():
    os.makedirs(OUT, exist_ok=True)
    c = Cluster("e23", nn_port=8021)
    for w, rack in zip(WORKERS, ["/rack1", "/rack1", "/rack2"]):
        c.add_dn(w, rack, capacity=2000 * 2**30, mbps=240)
    ObjStore("bucket-e23").ensure_bucket()
    hd = HdfsFS(8021, None)
    gl = ObjFS("bucket-e23", 0.0, 0.0)
    gc = ObjFS("bucket-e23", 0.030, 0.020)
    log = {}

    # ---------------- nạp dữ liệu
    m0 = c.nn.call("metrics"); mem0 = c.nn.call("namespace_memory")
    ctx = get_context("fork")
    with ctx.Pool(3, initializer=_init_child) as pool:
        log["ingest_hdfs"] = ingest(pool, HDFS, "/e3/files")
    log["ingest_gs_cloud"] = ingest(None, GS_CLOUD, "/e3/files")
    m1 = c.nn.call("metrics"); mem1 = c.nn.call("namespace_memory")
    print("ingest", log, flush=True)

    # ---------------- TN2: thao tác metadata
    e2 = []
    for run in (range(RUNS + 1) if not os.environ.get("E23_PHASE_A") else []):   # lần 0 là làm nóng
        for name, fs in [("hdfs", hd), ("gs_local", gl), ("gs_cloud", gc)]:
            r0 = dict(fs.requests())
            t = time.time(); cnt = fs.count("/e3/files"); t_count = time.time() - t
            t = time.time(); lst = fs.ls_r("/e3/files"); t_ls = time.time() - t
            t = time.time(); fs.rename("/e3/files", "/e3/files_moved"); t_mv = time.time() - t
            t = time.time(); fs.rename("/e3/files_moved", "/e3/files"); t_mv2 = time.time() - t
            r1 = fs.requests()
            assert len(lst) == N_FILES and cnt["files"] == N_FILES, (name, len(lst), cnt)
            row = {"backend": name, "run": run, "count_s": t_count, "lsR_s": t_ls, "mv_s": t_mv,
                   "mv_back_s": t_mv2, "requests": {k: r1.get(k, 0) - r0.get(k, 0) for k in r1}}
            print(json.dumps(row), flush=True)
            purge_trash()
            if run > 0:
                e2.append(row)
    if os.environ.get("E23_PHASE_A"):
        e2 = None
    if e2 is not None:
      with open(f"{OUT}/e2_metadata.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["backend", "run", "count_s", "lsR_s", "mv_s", "mv_back_s", "requests"])
        for r in e2:
            w.writerow([r["backend"], r["run"], r["count_s"], r["lsR_s"], r["mv_s"], r["mv_back_s"],
                        json.dumps(r["requests"])])

    # ---------------- TN3: dựng HAR và tệp gộp
    names = [fname(i) for i in range(N_FILES)]
    t = time.time()
    parts, idx, off = [], [], 0
    for n in names:
        d = hd.read(f"/e3/files/{n}")
        parts.append(d); idx.append(f"{n} part-0 {off} {len(d)}"); off += len(d)
    hd.write("/e3/archive.har/part-0", b"".join(parts))
    hd.write("/e3/archive.har/_index", "\n".join(idx).encode())
    hd.write("/e3/archive.har/_masterindex", b"3\n0 0 0 " + str(len(idx)).encode() + b"\n")
    log["build_har"] = time.time() - t
    m2 = c.nn.call("metrics"); mem2 = c.nn.call("namespace_memory")
    t = time.time()
    hd.write("/e3/merged/all.txt", b"".join(parts))          # tương đương getmerge rồi put lại
    log["build_merged"] = time.time() - t
    m3 = c.nn.call("metrics"); mem3 = c.nn.call("namespace_memory")
    del parts

    def delta(a, b):
        return {k: b[k] - a[k] for k in ("files", "dirs", "blocks", "objects")}
    objects = {"hdfs_loose": delta(m0, m1), "hdfs_har": delta(m1, m2), "hdfs_merged": delta(m2, m3)}
    memory = {"hdfs_loose": {k: mem1[k] - mem0[k] for k in mem0},
              "hdfs_har": {k: mem2[k] - mem1[k] for k in mem0},
              "hdfs_merged": {k: mem3[k] - mem2[k] for k in mem0}}
    log["objects"], log["memory"] = objects, memory
    print("objects", objects, "memory", memory, flush=True)
    print("build", {"build_har": log["build_har"], "build_merged": log["build_merged"]}, flush=True)
    if os.environ.get("E23_PHASE_A"):     # giữ cụm chạy để exp3_jobs.py dùng tiếp
        return "keep"

    # ---------------- TN3: WordCount
    total_merged = hd.info("/e3/merged/all.txt")["size"]
    layouts = {
        "hdfs_loose": (wc_file, [(HDFS, f"/e3/files/{n}", WORKERS[i % 3]) for i, n in enumerate(names)], hd),
        "hdfs_har": (wc_har, [(HDFS, "/e3/archive.har", n, WORKERS[i % 3]) for i, n in enumerate(names)], hd),
        "hdfs_merged": (wc_split, [(HDFS, "/e3/merged/all.txt", s, BLOCK, total_merged, WORKERS[k % 3])
                                   for k, s in enumerate(range(0, total_merged, BLOCK))], hd),
        "gs_cloud": (wc_file, [(GS_CLOUD, f"/e3/files/{n}", None) for n in names], gc),
        "gs_local": (wc_file, [(GS_LOCAL, f"/e3/files/{n}", None) for n in names], gl),
    }
    e3, ref = [], None
    for run in range(1, RUNS + 1):          # mỗi task đã là một tiến trình mới nên không chạy lượt làm nóng
        for name, (fn, tasks, ofs) in layouts.items():
            el, cnt = run_job(fn, tasks, ofs, f"/e3/out/{name}_{run}.txt")
            if ref is None:
                ref = cnt
            same = (cnt == ref)
            row = {"layout": name, "run": run, "job_s": el, "map_tasks": len(tasks), "same_result": same}
            print(json.dumps(row), flush=True)
            if run > 0:
                e3.append(row)
    with open(f"{OUT}/e3_smallfiles.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["layout", "run", "job_s", "map_tasks", "same_result"])
        w.writeheader()
        w.writerows(e3)
    json.dump(log, open(f"{OUT}/e3_log.json", "w"), indent=1)
    gl.delete("/e3")
    purge_trash()
    c.stop()


if __name__ == "__main__":
    keep = None
    try:
        keep = main()
    finally:
        if keep != "keep":
            os.system(os.path.join(ROOT, "killcluster.sh"))
