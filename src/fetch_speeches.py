#!/usr/bin/env python3
"""
Phase 1: 国会会議録APIからデータ取得

対象: 衆議院厚生労働委員会の政務三役（大臣・副大臣・大臣政務官）答弁
国会回次: データが存在する回次を動的に検出（216〜220回を優先）

Usage:
    python src/fetch_speeches.py
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request

BASE_URL = "https://kokkai.ndl.go.jp/api/speech"
DATA_RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
MAX_RECORDS_PER_REQUEST = 100
SLEEP_BETWEEN_REQUESTS = 1.5  # APIレート制限遵守


def fetch_page(params: dict) -> dict:
    """1ページ分のAPIリクエストを実行"""
    query = urllib.parse.urlencode(params)
    url = f"{BASE_URL}?{query}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_all_speeches(
    name_of_meeting: str,
    session_from: int,
    session_to: int,
) -> list[dict]:
    """指定条件の全答弁を取得（ページネーション対応）

    speakerPosition='大臣' は部分一致で副大臣・大臣政務官もヒットする。
    取得後に政務三役以外（大臣官房審議官等）をフィルタする。
    """
    params = {
        "nameOfHouse": "衆議院",
        "nameOfMeeting": name_of_meeting,
        "sessionFrom": session_from,
        "sessionTo": session_to,
        "speakerPosition": "大臣",
        "maximumRecords": MAX_RECORDS_PER_REQUEST,
        "startRecord": 1,
        "recordPacking": "json",
    }

    all_records = []
    page = 1

    # まず総件数を確認
    first_page = fetch_page(params)
    total = first_page.get("numberOfRecords", 0)
    print(f"  Total records for {name_of_meeting} (session {session_from}-{session_to}): {total}")

    if total == 0:
        return []

    all_records.extend(first_page.get("speechRecord", []))
    next_pos = first_page.get("nextRecordPosition")

    while next_pos and next_pos <= total:
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        params["startRecord"] = next_pos
        page += 1
        print(f"    Fetching page {page} (record {next_pos}/{total})...")
        data = fetch_page(params)
        all_records.extend(data.get("speechRecord", []))
        next_pos = data.get("nextRecordPosition")

    return all_records


def is_political_appointee(position: str) -> bool:
    """政務三役（大臣・副大臣・大臣政務官）かどうかを判定

    大臣官房審議官、大臣官房参事官等は除外する。
    """
    if not position:
        return False
    # 大臣官房系の官僚を除外
    if "官房" in position:
        return False
    # 「大臣」「副大臣」「大臣政務官」のいずれかを含む
    if "副大臣" in position or "大臣政務官" in position:
        return True
    if "大臣" in position:
        return True
    return False


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--committee", default="厚生労働委員会", help="委員会名")
    args = parser.parse_args()

    os.makedirs(DATA_RAW_DIR, exist_ok=True)

    # データが存在する可能性のある国会回次
    target_sessions = [216, 217, 218, 219, 220]
    # 不足時に拡張する候補
    fallback_sessions = [211, 212, 213, 214, 215]

    committee = args.committee
    all_speeches = []

    print(f"=== Fetching speeches from {committee} ===")

    # まず対象回次を個別に取得
    for session in target_sessions:
        print(f"\nSession {session}:")
        records = fetch_all_speeches(committee, session, session)
        if records:
            all_speeches.extend(records)
            print(f"  -> Got {len(records)} records")
        else:
            print(f"  -> No data")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 政務三役のみにフィルタ
    political = [r for r in all_speeches if is_political_appointee(r.get("speakerPosition", ""))]
    print(f"\n=== Filter: political appointees only ===")
    print(f"  Before filter: {len(all_speeches)} records")
    print(f"  After filter:  {len(political)} records")

    # 非担当省庁の数を確認
    non_mhlw = [r for r in political if "厚生労働" not in r.get("speakerPosition", "")]
    print(f"  Non-MHLW appointees: {len(non_mhlw)} records")

    # 20件未満ならfallbackセッションを追加
    if len(non_mhlw) < 20:
        print(f"\n=== Non-MHLW < 20, expanding to fallback sessions ===")
        for session in fallback_sessions:
            print(f"\nSession {session}:")
            records = fetch_all_speeches(committee, session, session)
            if records:
                all_speeches.extend(records)
                new_political = [r for r in records if is_political_appointee(r.get("speakerPosition", ""))]
                political.extend(new_political)
                new_non_mhlw = [r for r in new_political if "厚生労働" not in r.get("speakerPosition", "")]
                non_mhlw.extend(new_non_mhlw)
                print(f"  -> Got {len(records)} records ({len(new_political)} political, {len(new_non_mhlw)} non-MHLW)")
            else:
                print(f"  -> No data")
            time.sleep(SLEEP_BETWEEN_REQUESTS)

            if len(non_mhlw) >= 20:
                print(f"\n  Non-MHLW count reached {len(non_mhlw)}, stopping expansion")
                break

    # 委員会名からファイル名プレフィックスを生成
    prefix = committee.replace("委員会", "")

    # 生データを保存
    raw_path = os.path.join(DATA_RAW_DIR, f"speeches_raw_{prefix}.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(all_speeches, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(all_speeches)} raw records to {raw_path}")

    # 政務三役のみのデータを保存
    filtered_path = os.path.join(DATA_RAW_DIR, f"speeches_political_{prefix}.json")
    with open(filtered_path, "w", encoding="utf-8") as f:
        json.dump(political, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(political)} political appointee records to {filtered_path}")

    # サマリー
    print(f"\n=== Summary ===")
    from collections import Counter
    pos_counts = Counter(r.get("speakerPosition", "") for r in political)
    for pos, count in pos_counts.most_common():
        is_mhlw = "厚生労働" in pos
        tag = "担当" if is_mhlw else "非担当"
        print(f"  [{tag}] {pos}: {count}")

    print(f"\nTotal political appointee speeches: {len(political)}")
    print(f"  MHLW (担当): {len(political) - len(non_mhlw)}")
    print(f"  Non-MHLW (非担当): {len(non_mhlw)}")

    if len(non_mhlw) < 20:
        print(f"\n⚠ WARNING: Non-MHLW speeches ({len(non_mhlw)}) < 20. Consider expanding to more committees.")
        sys.exit(1)


if __name__ == "__main__":
    main()
