"""HDFS Balancer thu nhỏ: di chuyển bản sao từ node dùng nhiều sang node dùng ít
cho tới khi mức dùng của mọi node nằm trong (trung bình +- ngưỡng)."""
import time


def report(nn):
    rep = {d: v for d, v in nn.call("dn_report").items() if v["alive"]}
    util = {d: 100.0 * v["used"] / v["capacity"] for d, v in rep.items()}
    avg = 100.0 * sum(v["used"] for v in rep.values()) / sum(v["capacity"] for v in rep.values())
    return rep, util, avg


def balance(nn, threshold=10.0, max_iter=20, settle=2.5):
    t0 = time.time()
    moved_bytes = moved_blocks = iters = 0
    history = []
    while iters < max_iter:
        rep, util, avg = report(nn)
        history.append({k: round(v, 2) for k, v in util.items()})
        over = [d for d in util if util[d] > avg + threshold]
        above = [d for d in util if avg < util[d] <= avg + threshold]
        below = [d for d in util if avg - threshold <= util[d] < avg]
        under = [d for d in util if util[d] < avg - threshold]
        if not over and not under:
            break
        give = {d: (util[d] - avg) / 100 * rep[d]["capacity"] for d in over + above}
        take = {d: (avg - util[d]) / 100 * rep[d]["capacity"] for d in under + below}
        bm = nn.call("block_map")
        on = {}
        for bid, (size, locs, _) in bm.items():
            for d in locs:
                on.setdefault(d, []).append((bid, size))
        planned = []
        used_blocks = set()
        for srcs, dsts in ((over, under), (over, below), (above, under)):
            for s in srcs:
                for t in dsts:
                    amount = min(give.get(s, 0), take.get(t, 0))
                    if amount <= 0:
                        continue
                    for bid, size in on.get(s, []):
                        if amount <= 0:
                            break
                        if bid in used_blocks or t in bm[bid][1]:
                            continue
                        if nn.call("move_block", bid, s, t):
                            planned.append((bid, s, t, size))
                            used_blocks.add(bid)
                            amount -= size
                            give[s] -= size
                            take[t] -= size
        if not planned:
            break
        deadline = time.time() + 600
        while time.time() < deadline:            # chờ các lần di chuyển hoàn tất
            bm = nn.call("block_map")
            if all(t in bm.get(b, (0, [t]))[1] and s not in bm.get(b, (0, []))[1] for b, s, t, _ in planned):
                break
            time.sleep(0.3)
        moved_bytes += sum(p[3] for p in planned)
        moved_blocks += len(planned)
        iters += 1
        time.sleep(settle)                       # đợi heartbeat cập nhật dung lượng
    rep, util, avg = report(nn)
    history.append({k: round(v, 2) for k, v in util.items()})
    return {"iterations": iters, "moved_bytes": moved_bytes, "moved_blocks": moved_blocks,
            "seconds": time.time() - t0, "history": history}
