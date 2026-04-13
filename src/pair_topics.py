#!/usr/bin/env python3
"""
Phase 3: 同一テーマのペアリング

同一会議（issueID）内で、同一テーマについて担当大臣と非担当副大臣・政務官が
答弁しているケースをペアリングする。

テーマの同一性は、同一会議の近接する発言（speechOrder が近い）を同一テーマとみなす。

Usage:
    python src/pair_topics.py
"""

import csv
import os

DATA_SCORED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "scored")


def load_scored_data(path: str) -> list[dict]:
    """スコアリング済みデータを読み込み"""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def create_pairs(scored_data: list[dict]) -> list[dict]:
    """同一会議内で担当大臣と非担当副大臣・政務官のペアを作成

    同一issueID内での答弁を、担当/非担当でグルーピングし、
    テーマの近さ（speechOrder）でペアリングする。
    """
    # issueID でグルーピング
    by_issue = {}
    for rec in scored_data:
        issue = rec.get("issue", "") + "_" + rec.get("date", "")
        if issue not in by_issue:
            by_issue[issue] = []
        by_issue[issue].append(rec)

    pairs = []
    for issue_key, records in by_issue.items():
        # 担当大臣の答弁
        responsible_minister = [
            r for r in records
            if r.get("is_responsible", "").lower() in ("true", "1")
            and r.get("role_level") == "大臣"
        ]
        # 非担当副大臣・政務官の答弁
        non_responsible = [
            r for r in records
            if r.get("is_responsible", "").lower() in ("false", "0", "")
            and r.get("role_level") in ("副大臣", "大臣政務官")
        ]

        if not responsible_minister or not non_responsible:
            continue

        # 非担当の各答弁に対して、最も近い担当大臣答弁をペアにする
        for nr in non_responsible:
            nr_order = int(nr.get("speech_id", "0").split("_")[-1] or "0")
            best_match = None
            best_dist = float("inf")
            for rm in responsible_minister:
                rm_order = int(rm.get("speech_id", "0").split("_")[-1] or "0")
                dist = abs(nr_order - rm_order)
                if dist < best_dist:
                    best_dist = dist
                    best_match = rm

            if best_match:
                pairs.append({
                    "issue_key": issue_key,
                    "non_resp_speaker": nr.get("speaker"),
                    "non_resp_position": nr.get("speaker_position"),
                    "non_resp_ministry": nr.get("ministry"),
                    "non_resp_role": nr.get("role_level"),
                    "non_resp_score": nr.get("score", ""),
                    "non_resp_attitude_tags": nr.get("attitude_tags", ""),
                    "non_resp_speech_text": nr.get("speech_text", "")[:300],
                    "resp_speaker": best_match.get("speaker"),
                    "resp_position": best_match.get("speaker_position"),
                    "resp_score": best_match.get("score", ""),
                    "resp_attitude_tags": best_match.get("attitude_tags", ""),
                    "resp_speech_text": best_match.get("speech_text", "")[:300],
                    "speech_order_distance": best_dist,
                    "date": nr.get("date"),
                    "session": nr.get("session"),
                })

    return pairs


def main():
    # スコア付きデータとタグ付きデータを読み込み
    scored_path = os.path.join(DATA_SCORED_DIR, "scored_speeches.csv")
    tagged_path = os.path.join(DATA_SCORED_DIR, "tagged_speeches.csv")

    # スコアとタグをマージ
    scored = load_scored_data(scored_path)
    tagged = load_scored_data(tagged_path)

    if scored:
        # タグデータをspeech_idでマッピング
        tag_map = {r["speech_id"]: r for r in tagged} if tagged else {}
        for rec in scored:
            tag_rec = tag_map.get(rec.get("speech_id"), {})
            rec["attitude_tags"] = tag_rec.get("attitude_tags", "")
            rec["content_tags"] = tag_rec.get("content_tags", "")
        data = scored
    elif tagged:
        data = tagged
    else:
        print("No scored or tagged data found.")
        return

    pairs = create_pairs(data)
    print(f"=== Pairing Complete ===")
    print(f"Total pairs: {len(pairs)}")

    if pairs:
        pairs_path = os.path.join(DATA_SCORED_DIR, "topic_pairs.csv")
        fieldnames = list(pairs[0].keys())
        with open(pairs_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(pairs)
        print(f"Saved to {pairs_path}")

        # ペアの統計
        for pair in pairs[:5]:
            print(f"\n  [{pair['date']}] {pair['non_resp_speaker']}({pair['non_resp_position']}) "
                  f"score={pair['non_resp_score']} vs "
                  f"{pair['resp_speaker']}({pair['resp_position']}) "
                  f"score={pair['resp_score']}")


if __name__ == "__main__":
    main()
