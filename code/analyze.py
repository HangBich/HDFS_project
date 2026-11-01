import os
ROOT = os.path.dirname(os.path.abspath(__file__))
"""Tổng hợp kết quả: trung vị 3 lần đo cho từng thí nghiệm."""
import csv
import json
import os
import statistics as st

R = os.path.join(ROOT, "results")


def med(xs):
    return st.median(xs)


def e1():
    if not os.path.exists(f"{R}/e1_terasort.csv"):
        return
    rows = list(csv.DictReader(open(f"{R}/e1_terasort.csv")))
    keys = ["teragen_tasks", "teragen_commit", "teragen", "terasort_sample", "terasort_map",
            "terasort_reduce", "terasort_commit", "terasort", "teravalidate"]
    print("== E1 (giây, trung vị)")
    out = {}
    for b in ["hdfs", "gs_local", "gs_cloud"]:
        rs = [r for r in rows if r["backend"] == b]
        out[b] = {k: med([float(r[k]) for r in rs]) for k in keys}
        out[b]["total"] = out[b]["teragen"] + out[b]["terasort"] + out[b]["teravalidate"]
        out[b]["valid"] = all(r["valid"] == "True" for r in rs)
        out[b]["n"] = len(rs)
        print(b, {k: round(v, 2) if isinstance(v, float) else v for k, v in out[b].items()})
        print("   các lần:", {k: [round(float(r[k]), 2) for r in rs] for k in ["teragen", "terasort", "teravalidate"]})
    for b in ["gs_local", "gs_cloud"]:
        print("tỉ số", b, "/hdfs:", {k: round(out[b][k] / out["hdfs"][k], 2) for k in keys + ["total"]
                                     if out["hdfs"][k] > 0.01})
    req = json.load(open(f"{R}/e1_requests.json"))
    for k in ["hdfs_1", "gs_cloud_1"]:
        if k in req:
            print("yêu cầu", k, {ph: sum(v.values()) for ph, v in req[k].items()}, req[k])
    json.dump(out, open(f"{R}/sum_e1.json", "w"), indent=1)


def e2():
    if not os.path.exists(f"{R}/e2_metadata.csv"):
        return
    rows = list(csv.DictReader(open(f"{R}/e2_metadata.csv")))
    print("== E2 (giây, trung vị)")
    out = {}
    for b in ["hdfs", "gs_local", "gs_cloud"]:
        rs = [r for r in rows if r["backend"] == b]
        out[b] = {k: med([float(r[k]) for r in rs]) for k in ["count_s", "lsR_s", "mv_s", "mv_back_s"]}
        out[b]["mv_all"] = med([float(r[k]) for r in rs for k in ("mv_s", "mv_back_s")])
        out[b]["requests"] = json.loads(rs[0]["requests"])
        print(b, out[b])
    for b in ["gs_local", "gs_cloud"]:
        print("tỉ số", b, {k: round(out[b][k] / out["hdfs"][k], 1) for k in ["count_s", "lsR_s", "mv_all"]})
    json.dump(out, open(f"{R}/sum_e2.json", "w"), indent=1)


def e3():
    if not os.path.exists(f"{R}/e3_smallfiles.csv"):
        return
    rows = list(csv.DictReader(open(f"{R}/e3_smallfiles.csv")))
    log = json.load(open(f"{R}/e3_log.json"))
    print("== E3")
    out = {}
    for l in ["hdfs_loose", "hdfs_har", "hdfs_merged", "gs_local", "gs_cloud"]:
        rs = [r for r in rows if r["layout"] == l]
        out[l] = {"job_s": med([float(r["job_s"]) for r in rs]), "runs": [round(float(r["job_s"]), 1) for r in rs],
                  "map_tasks": int(rs[0]["map_tasks"]), "same": all(r["same_result"] == "True" for r in rs)}
        print(l, out[l])
    print("objects", log["objects"])
    print("memory", log["memory"])
    print("ingest/build", {k: round(v, 1) for k, v in log.items() if isinstance(v, float)})
    out["log"] = log
    json.dump(out, open(f"{R}/sum_e3.json", "w"), indent=1)


def e4():
    if not os.path.exists(f"{R}/e4_ec.csv"):
        return
    rows = list(csv.DictReader(open(f"{R}/e4_ec.csv")))
    print("== E4")
    out = {}
    for cfg in ["repl2", "repl3", "ec_xor21"]:
        rs = [r for r in rows if r["config"] == cfg]
        keys = ["write_s", "raw_bytes", "overhead", "read_s", "degraded_s", "recover_s", "lost_bytes",
                "recover_read_bytes", "failover", "degraded_groups"]
        out[cfg] = {k: med([float(r[k]) for r in rs]) for k in keys}
        out[cfg]["ok"] = all(r["ok"] == "True" for r in rs)
        out[cfg]["runs_degraded"] = [round(float(r["degraded_s"]), 2) for r in rs]
        out[cfg]["runs_read"] = [round(float(r["read_s"]), 2) for r in rs]
        out[cfg]["amplification"] = out[cfg]["recover_read_bytes"] / max(1, out[cfg]["lost_bytes"])
        print(cfg, {k: round(v, 3) if isinstance(v, float) else v for k, v in out[cfg].items()})
    json.dump(out, open(f"{R}/sum_e4.json", "w"), indent=1)


def e5():
    if not os.path.exists(f"{R}/e5_balancer.json"):
        return
    rows = json.load(open(f"{R}/e5_balancer.json"))
    print("== E5")
    out = {k: med([r[k] for r in rows]) for k in ["scan_2w_s", "scan_added_s", "scan_balanced_s",
                                                  "bal_iterations", "bal_moved_bytes", "bal_seconds"]}
    out["snap"] = []
    for i in range(4):
        lab = rows[0]["snapshots"][i]["label"]
        nodes = sorted(rows[0]["snapshots"][i]["util"])
        util = {n: med([r["snapshots"][i]["util"].get(n, 0) for r in rows]) for n in nodes}
        out["snap"].append({"label": lab, "util": util,
                            "stdev": med([r["snapshots"][i]["stdev"] for r in rows]),
                            "spread": med([r["snapshots"][i]["spread"] for r in rows]),
                            "avg": med([r["snapshots"][i]["avg"] for r in rows])})
    out["loc"] = [r["loc_added"] for r in rows], [r["loc_bal"] for r in rows]
    out["runs"] = [[round(r[k], 1) for k in ("scan_2w_s", "scan_added_s", "scan_balanced_s")] for r in rows]
    print(json.dumps(out, indent=1, ensure_ascii=False))
    json.dump(out, open(f"{R}/sum_e5.json", "w"), indent=1, ensure_ascii=False)


if __name__ == "__main__":
    e1(); e2(); e3(); e4(); e5()
