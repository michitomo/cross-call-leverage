#!/usr/bin/env python3
"""5委員会を並列でfetch。各委員会を別スレッドで実行。"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading

BASE_DIR = Path(__file__).parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
API_BASE = "https://kokkai.ndl.go.jp/api/speech"

COMMITTEES = [
    "厚生労働委員会",
    "総務委員会",
    "内閣委員会",
    "経済産業委員会",
    "国土交通委員会",
]

SESSIONS = list(range(201, 221))
lock = threading.Lock()


def is_political_appointee(pos):
    if not pos:
        return False
    if "官房" in pos:
        return False
    return "副大臣" in pos or "大臣政務官" in pos or "大臣" in pos


def fetch_page(params):
    query = urllib.parse.urlencode(params)
    url = f"{API_BASE}?{query}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_one_committee(committee):
    prefix = committee.replace("委員会", "")
    raw_path = DATA_RAW / f"speeches_raw_{prefix}.json"
    pol_path = DATA_RAW / f"speeches_political_{prefix}.json"

    if raw_path.exists():
        with open(raw_path, "r", encoding="utf-8") as f:
            existing = json.load(f)
        existing_ids = {r.get("speechID") for r in existing}
    else:
        existing = []
        existing_ids = set()

    all_records = list(existing)
    new_total = 0

    for session in SESSIONS:
        params = {
            "nameOfHouse": "衆議院",
            "nameOfMeeting": committee,
            "sessionFrom": session,
            "sessionTo": session,
            "speakerPosition": "大臣",
            "maximumRecords": 100,
            "startRecord": 1,
            "recordPacking": "json",
        }

        try:
            first = fetch_page(params)
        except Exception as e:
            time.sleep(1)
            continue

        total = first.get("numberOfRecords", 0)
        if total == 0:
            continue

        records = first.get("speechRecord", [])
        new_recs = [r for r in records if r.get("speechID") not in existing_ids]

        if not new_recs and records:
            continue  # already fetched

        all_records.extend(new_recs)
        for r in new_recs:
            existing_ids.add(r.get("speechID"))
        new_total += len(new_recs)

        next_pos = first.get("nextRecordPosition")
        while next_pos and next_pos <= total:
            time.sleep(0.8)
            params["startRecord"] = next_pos
            try:
                data = fetch_page(params)
            except:
                time.sleep(2)
                break
            recs = data.get("speechRecord", [])
            nr = [r for r in recs if r.get("speechID") not in existing_ids]
            all_records.extend(nr)
            for r in nr:
                existing_ids.add(r.get("speechID"))
            new_total += len(nr)
            next_pos = data.get("nextRecordPosition")

        time.sleep(0.8)

    # Save
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    political = [r for r in all_records if is_political_appointee(r.get("speakerPosition", ""))]
    with open(pol_path, "w", encoding="utf-8") as f:
        json.dump(political, f, ensure_ascii=False, indent=2)

    with lock:
        print(f"[DONE] {committee}: {len(all_records)} raw (+{new_total} new), {len(political)} political")

    return committee, len(all_records), len(political)


def main():
    print(f"=== Parallel fetch: {len(COMMITTEES)} committees x {len(SESSIONS)} sessions ===")

    # Run 3 committees at a time (to be gentle on the API)
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_one_committee, c): c for c in COMMITTEES}
        for future in as_completed(futures):
            c = futures[future]
            try:
                result = future.result()
            except Exception as e:
                print(f"[ERROR] {c}: {e}")


if __name__ == "__main__":
    main()
