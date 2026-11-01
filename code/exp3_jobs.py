import os
ROOT = os.path.dirname(os.path.abspath(__file__))
"""Thí nghiệm 3 (phần job): chạy WordCount trên 5 cách bố trí tệp, dùng lại cụm và dữ liệu
mà exp23_smallfiles.py đã nạp (cụm mini-HDFS cổng 8021, bucket-e23).
Tiến trình điều khiển được giữ gọn nhẹ để chi phí fork mỗi task không bị phình theo RAM."""
import ast
import csv
import json
import os
import sys
import time

sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "minihdfs"))
import exp23_smallfiles as e                    # noqa: E402
from fsadapter import HdfsFS, ObjFS             # noqa: E402
from objstore import purge_trash                # noqa: E402

OUT = os.path.join(ROOT, "results")
RUNS = 3


def main():
    hd = HdfsFS(8021, None)
    gl = ObjFS("bucket-e23", 0.0, 0.0)
    gc = ObjFS("bucket-e23", 0.030, 0.020)
    log = {}
    for line in open(f"{OUT}/e23.log"):
        if line.startswith("ingest "):
            log.update(ast.literal_eval(line[len("ingest "):]))
        if line.startswith("build "):
            log.update(ast.literal_eval(line[len("build "):]))
        if line.startswith("objects "):
            a, b = line[len("objects "):].split(" memory ")
            log["objects"], log["memory"] = ast.literal_eval(a), ast.literal_eval(b)

    names = [e.fname(i) for i in range(e.N_FILES)]
    total_merged = hd.info("/e3/merged/all.txt")["size"]
    W = e.WORKERS
    layouts = {
        "hdfs_loose": (e.wc_file, [(e.HDFS, f"/e3/files/{n}", W[i % 3]) for i, n in enumerate(names)], hd),
        "hdfs_har": (e.wc_har, [(e.HDFS, "/e3/archive.har", n, W[i % 3]) for i, n in enumerate(names)], hd),
        "hdfs_merged": (e.wc_split, [(e.HDFS, "/e3/merged/all.txt", s, e.BLOCK, total_merged, W[k % 3])
                                     for k, s in enumerate(range(0, total_merged, e.BLOCK))], hd),
        "gs_cloud": (e.wc_file, [(e.GS_CLOUD, f"/e3/files/{n}", None) for n in names], gc),
        "gs_local": (e.wc_file, [(e.GS_LOCAL, f"/e3/files/{n}", None) for n in names], gl),
    }
    rows, ref = [], None
    for run in range(1, RUNS + 1):
        for name, (fn, tasks, ofs) in layouts.items():
            el, cnt = e.run_job(fn, tasks, ofs, f"/e3/out/{name}_{run}.txt")
            if ref is None:
                ref = cnt
            row = {"layout": name, "run": run, "job_s": el, "map_tasks": len(tasks), "same_result": cnt == ref}
            print(json.dumps(row), flush=True)
            rows.append(row)
    with open(f"{OUT}/e3_smallfiles.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["layout", "run", "job_s", "map_tasks", "same_result"])
        w.writeheader()
        w.writerows(rows)
    # đo lại thời gian dựng HAR và tệp gộp (ghi vào đường dẫn tạm rồi xóa)
    t = time.time()
    parts, idx, off = [], [], 0
    for n in names:
        d = hd.read(f"/e3/files/{n}")
        parts.append(d); idx.append(f"{n} part-0 {off} {len(d)}"); off += len(d)
    hd.write("/tmp_build/a.har/part-0", b"".join(parts))
    hd.write("/tmp_build/a.har/_index", "\n".join(idx).encode())
    hd.write("/tmp_build/a.har/_masterindex", b"3\n")
    log["build_har"] = time.time() - t
    t = time.time()
    hd.write("/tmp_build/merged.txt", b"".join(parts))
    log["build_merged_write_only"] = time.time() - t
    del parts
    hd.delete("/tmp_build")
    print("build", log["build_har"], log["build_merged_write_only"], flush=True)

    json.dump(log, open(f"{OUT}/e3_log.json", "w"), indent=1)
    gl.delete("/e3")
    purge_trash()


if __name__ == "__main__":
    try:
        main()
    finally:
        os.system(os.path.join(ROOT, "killcluster.sh"))
