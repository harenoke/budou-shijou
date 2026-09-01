# -*- coding: utf-8 -*-
"""東京都中央卸売市場の日報から、ぶどうの品種別相場を収集してダッシュボードを作る。

GitHub Actions（インターネット制限なし）で毎日実行される想定。標準ライブラリのみ。

  raw_history.csv        日報から取った生データ（追記・重複排除ずみ）
  budou_prices.csv       円/kg に換算した正規化データ
  site/index.html        公開用ダッシュボード（GitHub Pages）
  site/data.json         グラフ用の圧縮データ
"""
import csv, io, json, os, random, sys, time, urllib.request, urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

BASE_URL   = "https://www.shijou-nippo.metro.tokyo.lg.jp"
DAYS_BACK  = int(os.environ.get("DAYS_BACK", "12"))
DAYS_SKIP  = int(os.environ.get("DAYS_SKIP", "0"))  # 何日前から始めるか（まとめ取りを分割する用）
WORKERS    = int(os.environ.get("WORKERS", "4"))   # 出典サーバーは同時接続が多いと接続を切る
RETRIES    = int(os.environ.get("RETRIES", "4"))
MARKET_IDS = range(1, 10)          # K1..K9（K0 は全市場計＝価格なし）
JST        = timezone(timedelta(hours=9))

ROOT  = os.path.dirname(os.path.abspath(__file__))
RAW   = os.path.join(ROOT, "raw_history.csv")
NORM  = os.path.join(ROOT, "budou_prices.csv")
SITE  = os.path.join(ROOT, "site")
TPL   = os.path.join(ROOT, "template.html")
ROSTER = os.path.join(ROOT, "varieties.json")   # これまでに出てきた品種の台帳

RAW_HEAD = ["date","market","qty_total_kg","method","qty_kg","variety","origin","unit_kg","high","mid","low"]
KEY      = ("date","market","method","variety","origin","unit_kg")
DASH     = "－"                # 日報で「値なし」を表す全角ダッシュ

# 出典側は品種名を7文字で切るため、確実に判別できるものだけ元の名前に戻す。
# 未知の切れ方はそのまま通し、実行ログの「品種の顔ぶれ」に出るので、
# 見慣れない表記が出たらここに足していく。
VARIETY_FIX = {
    "シャインマスカ":   "シャインマスカット",
    "シャインマスカッ": "シャインマスカット",
    "アレキ":           "アレキサンドリア",
    "瀬戸ジャイアン":   "瀬戸ジャイアンツ",
    "ロザリオビアン":   "ロザリオビアンコ",
    "マスカットベー":   "マスカットベーリーA",
    "オーロラブラッ":   "オーロラブラック",
    "オーロラブラ":     "オーロラブラック",
    "クイーンルージ":   "クイーンルージュ",
    "ハウスデラウェ":   "ハウスデラウェア",
    "ハウスシャイン":   "ハウスシャインマスカット",
    "ネオマスカッ":     "ネオマスカット",
    "サニールージ":     "サニールージュ",
}

# ちょうど7文字だが「切れていない」と確認ずみの品種。
# 新しい品種が出ると 7文字のものは「要確認」として報告されるので、
# 中身を確かめたうえでここに足すと以後は静かになる。
VARIETY_OK = {
    "シャインマスカット",   # VARIETY_FIX で復元ずみ
}


# --------------------------------------------------------------- 取得
FAILURES = []          # 404 ではなく「取りに行けなかった」ページ


def fetch(url):
    """404 は休市・未掲載として確定。それ以外の失敗は待って数回やり直す。

    ここで黙って None を返すと、通信エラーが「その日は休みだった」と
    同じ扱いになりデータに穴が開く。だから区別して FAILURES に記録する。
    """
    req = urllib.request.Request(url, headers={"User-Agent": "budou-shijou-collector/1.0"})
    last = None
    for attempt in range(RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                if r.status == 200:
                    return r.read().decode("cp932", errors="replace")
                last = "HTTP %s" % r.status
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None                     # 休市・未掲載（確定）
            last = "HTTP %s" % e.code
        except Exception as e:
            last = type(e).__name__
        time.sleep(1.0 * (attempt + 1) + random.random())
    FAILURES.append((url, last))
    return None


def market_name(first_line):
    """'販売結果（青果・大田）' -> '大田'"""
    if "・" in first_line and "）" in first_line:
        return first_line.split("・")[-1].split("）")[0].strip()
    return ""


def parse_market_csv(text, date):
    """日報CSVから ぶどう の行だけ取り出す。上位セルが空欄の行は直前の値を引き継ぐ。"""
    lines = text.splitlines()
    if not lines:
        return []
    mkt = market_name(lines[0])
    if not mkt:
        return []
    carry = ["", "", "", ""]
    out = []
    for row in csv.reader(lines):
        if len(row) < 10:
            continue
        for i in range(4):
            if row[i].strip():
                carry[i] = row[i].strip()
        item, qty_total, method, qty = carry
        if item != "ぶどう":
            continue
        variety, origin, unit, high, mid, low = [c.strip() for c in row[4:10]]
        if high == DASH and mid == DASH and low == DASH:
            continue
        out.append([date, mkt, qty_total, method, qty, variety, origin, unit, high, mid, low])
    return out


def collect():
    """DAYS_SKIP 日前から DAYS_BACK 日分 × 9市場を、控えめな並列度で取りに行く。"""
    today = datetime.now(JST).date()
    jobs = []
    for back in range(DAYS_SKIP + 1, DAYS_SKIP + DAYS_BACK + 1):
        ymd = (today - timedelta(days=back)).strftime("%Y%m%d")
        for k in MARKET_IDS:
            jobs.append((ymd, k))

    def one(job):
        ymd, k = job
        text = fetch("%s/SN/%s/%s/Sei/Sei_K%d.csv" % (BASE_URL, ymd[:6], ymd, k))
        return parse_market_csv(text, ymd) if text is not None else None

    rows, ok, miss = [], 0, 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for got in pool.map(one, jobs):
            if got is None:
                miss += 1
            else:
                ok += 1
                rows += got
    rows.sort()
    print("取得: %d ページ / 休市・未掲載 %d ページ / 取得失敗 %d ページ / ぶどう %d 行"
          % (ok, miss - len(FAILURES), len(FAILURES), len(rows)))
    if ok == 0:
        sys.exit("取得できたページが 0 件でした。サイト構成が変わった可能性があります。")
    if FAILURES:
        for url, why in FAILURES[:20]:
            print("  ! %s %s" % (why, url))
        if len(FAILURES) > 20:
            print("  ! ほか %d 件" % (len(FAILURES) - 20))
        if len(FAILURES) > len(jobs) * 0.05:
            sys.exit("取得失敗が多すぎます（%d / %d）。WORKERS を下げて数分後にやり直してください。"
                     % (len(FAILURES), len(jobs)))
    return rows


# --------------------------------------------------------------- 保存・正規化
def read_csv(path):
    if not os.path.exists(path):
        return []
    with io.open(path, encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def merge(new_rows):
    existing = read_csv(RAW)
    incoming = [dict(zip(RAW_HEAD, r)) for r in new_rows]
    seen, merged = set(), []
    for r in existing + incoming:
        k = tuple((r.get(c) or "").strip() for c in KEY)
        if k in seen:
            continue
        seen.add(k)
        merged.append(r)
    merged.sort(key=lambda r: (r["date"], r["market"], r["variety"], r["origin"], r["method"]))
    with io.open(RAW, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RAW_HEAD)
        w.writeheader()
        for r in merged:
            w.writerow({c: r.get(c, "") for c in RAW_HEAD})
    print("生データ: %d 行（今回の取得で +%d 行）" % (len(merged), len(merged) - len(existing)))
    return merged


def num(v):
    if v is None:
        return None
    v = v.replace(",", "").replace(DASH, "").replace("-", "").strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def normalize(merged):
    out = []
    for r in merged:
        d, unit = r["date"], num(r["unit_kg"])
        rec = {
            "date": "%s-%s-%s" % (d[0:4], d[4:6], d[6:8]),
            "market": r["market"],
            "variety": VARIETY_FIX.get(r["variety"], r["variety"]),
            "origin": r["origin"],
            "method": r["method"],
            "unit_kg": unit,
            "qty_kg": num(r["qty_kg"]),
            "qty_total_kg": num(r["qty_total_kg"]),
        }
        for name in ("high", "mid", "low"):
            p = num(r[name])
            rec[name + "_yen"] = p
            rec[name + "_yen_per_kg"] = round(p / unit) if (p is not None and unit) else None
        out.append(rec)

    cols = ["date","market","variety","origin","method","unit_kg","qty_kg","qty_total_kg",
            "high_yen","mid_yen","low_yen","high_yen_per_kg","mid_yen_per_kg","low_yen_per_kg"]
    with io.open(NORM, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for rec in out:
            w.writerow({c: ("" if rec[c] is None else rec[c]) for c in cols})
    return out


def build_site(norm, new_varieties=(), needs_check=()):
    def index(vals):
        u = sorted(set(vals))
        return u, {v: i for i, v in enumerate(u)}

    dates, di = index(r["date"] for r in norm)
    mkts,  mi = index(r["market"] for r in norm)
    vars_, vi = index(r["variety"] for r in norm)
    orgs,  oi = index(r["origin"] for r in norm)
    meths, ei = index(r["method"] for r in norm)

    data = [[di[r["date"]], mi[r["market"]], vi[r["variety"]], oi[r["origin"]], ei[r["method"]],
             r["unit_kg"], r["qty_kg"], r["qty_total_kg"],
             r["high_yen_per_kg"], r["mid_yen_per_kg"], r["low_yen_per_kg"]] for r in norm]

    payload = {
        "generated": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        "dates": dates, "markets": mkts, "varieties": vars_, "origins": orgs, "methods": meths,
        "cols": ["d","m","v","o","e","unit","qty","qtyTotal","hi","mid","lo"],
        "rows": data,
        "newVarieties": list(new_varieties),
        "needsCheck": list(needs_check),
    }
    os.makedirs(SITE, exist_ok=True)
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    io.open(os.path.join(SITE, "data.json"), "w", encoding="utf-8").write(blob)

    # テンプレートは Artifact 用に <head> 相当と本文が地続きなので、
    # 単体ページとして出すときは </style> の位置で head / body に分ける。
    tpl = io.open(TPL, encoding="utf-8").read().replace("__DATA__", blob)
    split = tpl.index("</style>") + len("</style>")
    html = ('<!doctype html>\n<html lang="ja">\n<head>\n<meta charset="utf-8">\n'
            '<meta name="viewport" content="width=device-width,initial-scale=1">\n'
            + tpl[:split] + "\n</head>\n<body>\n" + tpl[split:] + "\n</body>\n</html>\n")
    io.open(os.path.join(SITE, "index.html"), "w", encoding="utf-8").write(html)

    print("最新営業日: %s / %d 営業日 / %d 行" % (dates[-1], len(dates), len(data)))
    print("-> site/index.html")


def track_varieties(norm):
    """品種の台帳を更新し、前回になかった品種を返す。

    品種は決め打ちしていないので新顔は自動でデータに入る。問題は
    「入ったことに誰も気づかない」ことなので、台帳と突き合わせて報告する。
    """
    prev = {}
    first_time = not os.path.exists(ROSTER)
    if not first_time:
        try:
            prev = json.load(io.open(ROSTER, encoding="utf-8")).get("varieties", {})
        except Exception:
            prev = {}

    cur = {}
    for r in norm:
        v, d = r["variety"], r["date"]
        e = cur.get(v)
        if e is None:
            cur[v] = {"first": d, "last": d, "rows": 1}
        else:
            e["first"] = min(e["first"], d)
            e["last"] = max(e["last"], d)
            e["rows"] += 1

    # 台帳がまだ無い初回は、全部が新顔になってしまうので基準作りに徹する
    new = [] if first_time else sorted(set(cur) - set(prev), key=lambda v: cur[v]["first"])
    check = sorted(v for v in cur if len(v) == 7 and v not in VARIETY_OK)

    with io.open(ROSTER, "w", encoding="utf-8") as f:
        json.dump({"updated": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
                   "varieties": dict(sorted(cur.items(), key=lambda kv: -kv[1]["rows"]))},
                  f, ensure_ascii=False, indent=1, sort_keys=False)

    lines = ["", "品種の顔ぶれ（%d 種）%s" % (len(cur), "  ※台帳を新規作成" if first_time else "")]
    for v, e in sorted(cur.items(), key=lambda kv: -kv[1]["rows"]):
        tag = ""
        if v in new:
            tag = "  ★新登場"
        elif v in check:
            tag = "  ← 名前を要確認（7文字で切れている可能性）"
        lines.append("  %-24s %5d行  %s 〜 %s%s" % (v, e["rows"], e["first"], e["last"], tag))
    if new:
        lines += ["", "★ 新しい品種が %d 件: %s" % (len(new), "、".join(new))]
    if check:
        lines += ["", "名前が切れているかもしれない品種: %s" % "、".join(check),
                  "  正しければ collect.py の VARIETY_OK に、切れていれば VARIETY_FIX に追加してください。"]
    text = "\n".join(lines)
    print(text)

    # Actions の実行サマリーにも出す（ログを開かなくても気づけるように）
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with io.open(summary, "a", encoding="utf-8") as f:
                if new:
                    f.write("## ★ 新しい品種が出ました\n\n")
                    for v in new:
                        f.write("- **%s** — 初出 %s（%d行）\n" % (v, cur[v]["first"], cur[v]["rows"]))
                    f.write("\n")
                f.write("```\n%s\n```\n" % text.strip())
        except Exception:
            pass

    return new, check


if __name__ == "__main__":
    _norm = normalize(merge(collect()))
    _new, _check = track_varieties(_norm)
    build_site(_norm, _new, _check)
