#!/usr/bin/env python3
"""
質問者（議員）の政党を特定するため、非担当VP/PS答弁が含まれる会議の
全発言を取得し、直前の議員発言者を特定する。

Usage:
    python src/fetch_questioners.py
"""
import csv
import json
import os
import time
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_SCORED = BASE_DIR / "data" / "scored"
API_BASE = "https://kokkai.ndl.go.jp/api/speech"

RULING_PARTIES = {
    "自由民主党", "公明党", "自由民主党・無所属の会",
    "自由民主党・国民の声", "自由民主党・無所属クラブ",
}

COMMITTEES = {
    "厚生労働": "",
    "総務": "_総務",
    "内閣": "_内閣",
    "経済産業": "_経済産業",
    "国土交通": "_国土交通",
}


def fetch_meeting_speeches(issue_id):
    """会議IDの全発言を取得（議員含む）"""
    # issueIDからsession, meetingを推定してフィルタなしで取得
    all_recs = []
    start = 1
    while True:
        params = {
            "issueID": issue_id,
            "maximumRecords": 100,
            "startRecord": start,
            "recordPacking": "json",
        }
        query = urllib.parse.urlencode(params)
        url = f"{API_BASE}?{query}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            break
        recs = data.get("speechRecord", [])
        all_recs.extend(recs)
        nxt = data.get("nextRecordPosition")
        if not nxt or nxt <= start:
            break
        start = nxt
        time.sleep(0.5)
    return all_recs


def is_government_speaker(position):
    if not position:
        return False
    gov_kw = ["大臣", "副大臣", "政務官", "政府参考人", "参考人",
              "審議官", "局長", "課長", "部長", "事務局", "委員長"]
    return any(kw in position for kw in gov_kw)


def find_questioner(speech_order, meeting_speeches):
    """speech_orderより前の、最も近い非政府発言者を返す"""
    for rec in sorted(meeting_speeches, key=lambda r: r.get("speechOrder", 0), reverse=True):
        if rec.get("speechOrder", 0) >= speech_order:
            continue
        if not is_government_speaker(rec.get("speakerPosition", "")):
            return {
                "questioner": rec.get("speaker", ""),
                "questioner_group": rec.get("speakerGroup", ""),
                "questioner_position": rec.get("speakerPosition", ""),
            }
    return {"questioner": "不明", "questioner_group": "不明", "questioner_position": ""}


def main():
    # 全委員会のscored dataから、必要な会議IDを収集
    all_speech_ids = {}  # speech_id -> (issueID, speechOrder, committee, scored_row)

    for name, suffix in COMMITTEES.items():
        scored_path = DATA_SCORED / f"scored_speeches{suffix}.csv"
        if not scored_path.exists():
            continue
        with open(scored_path, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                all_speech_ids[r["speech_id"]] = (name, r)

    # rawデータからspeechID -> issueID, speechOrder マッピング
    id_to_issue = {}
    for name in COMMITTEES:
        raw_path = DATA_RAW / f"speeches_raw_{name}.json"
        if not raw_path.exists():
            continue
        with open(raw_path, "r", encoding="utf-8") as f:
            for r in json.load(f):
                sid = r.get("speechID")
                if sid in all_speech_ids:
                    id_to_issue[sid] = (r.get("issueID"), r.get("speechOrder", 0))

    # ユニークなissueIDを収集
    needed_issues = set()
    for sid, (iid, order) in id_to_issue.items():
        needed_issues.add(iid)

    print(f"Total unique meetings to fetch: {len(needed_issues)}")

    # 会議ごとの全発言をキャッシュ
    cache_path = DATA_RAW / "meeting_speeches_cache.json"
    if cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            meeting_cache = json.load(f)
        print(f"Loaded cache: {len(meeting_cache)} meetings")
    else:
        meeting_cache = {}

    # 未取得の会議を並列フェッチ
    to_fetch = [iid for iid in needed_issues if iid not in meeting_cache]
    print(f"Need to fetch: {len(to_fetch)} meetings")

    if to_fetch:
        completed = 0
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(fetch_meeting_speeches, iid): iid for iid in to_fetch}
            for future in as_completed(futures):
                iid = futures[future]
                try:
                    recs = future.result()
                    meeting_cache[iid] = recs
                except Exception as e:
                    print(f"  Error fetching {iid}: {e}")
                completed += 1
                if completed % 20 == 0:
                    print(f"  [{completed}/{len(to_fetch)}] fetched")
                    # 中間保存
                    with open(cache_path, "w", encoding="utf-8") as f:
                        json.dump(meeting_cache, f, ensure_ascii=False)
                time.sleep(0.3)

        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(meeting_cache, f, ensure_ascii=False)
        print(f"Saved cache: {len(meeting_cache)} meetings")

    # 質問者を特定して結果を出力
    results = []
    for sid, (committee, scored_row) in all_speech_ids.items():
        if sid not in id_to_issue:
            continue
        iid, order = id_to_issue[sid]
        meeting_recs = meeting_cache.get(iid, [])
        q = find_questioner(order, meeting_recs)
        results.append({
            "speech_id": sid,
            "committee": committee,
            "speaker": scored_row.get("speaker", ""),
            "speaker_position": scored_row.get("speaker_position", ""),
            "role_level": scored_row.get("role_level", ""),
            "is_responsible": scored_row.get("is_responsible", ""),
            "responsibility_tag": scored_row.get("responsibility_tag", ""),
            "score": scored_row.get("score", ""),
            "questioner": q["questioner"],
            "questioner_group": q["questioner_group"],
            "questioner_position": q["questioner_position"],
        })

    # CSV出力
    out_path = DATA_SCORED / "speeches_with_questioners.csv"
    if results:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
    print(f"\nSaved {len(results)} records to {out_path}")

    # 与野党分析
    print("\n=== 与野党別・前向き度分析 ===\n")
    for committee in COMMITTEES:
        c_rows = [r for r in results if r["committee"] == committee]
        print(f"--- {committee}委員会 ---")
        for group_label, filter_fn in [
            ("担当大臣", lambda r: r["is_responsible"].lower() in ("true","1") and r["role_level"]=="大臣"),
            ("担当VP/PS", lambda r: r["is_responsible"].lower() in ("true","1") and r["role_level"] in ("副大臣","大臣政務官")),
            ("非担当VP/PS", lambda r: r["is_responsible"].lower() in ("false","0") and r["role_level"] in ("副大臣","大臣政務官") and "内閣総理大臣" not in r["speaker_position"]),
        ]:
            target = [r for r in c_rows if filter_fn(r)]
            by_party = {"与党": [], "野党": []}
            for r in target:
                s = r.get("score", "-1")
                if s in ("-1", ""):
                    continue
                s = int(s)
                qg = r.get("questioner_group", "")
                if qg in RULING_PARTIES:
                    by_party["与党"].append(s)
                elif qg and qg != "不明":
                    by_party["野党"].append(s)
            r_n, r_avg = len(by_party["与党"]), (sum(by_party["与党"])/len(by_party["与党"]) if by_party["与党"] else 0)
            o_n, o_avg = len(by_party["野党"]), (sum(by_party["野党"])/len(by_party["野党"]) if by_party["野党"] else 0)
            print(f"  {group_label}: 与党 n={r_n} avg={r_avg:.2f} | 野党 n={o_n} avg={o_avg:.2f}")
        print()


if __name__ == "__main__":
    main()
