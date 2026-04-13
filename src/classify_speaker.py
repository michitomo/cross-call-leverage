#!/usr/bin/env python3
"""
Phase 1: 答弁者の省庁・役職・担当/非担当を分類

speakerPosition から省庁名と役職レベルを抽出し、
委員会の所管省庁と照合して担当/非担当フラグを付与する。

Usage:
    python src/classify_speaker.py
"""

import csv
import json
import os
import re

DATA_RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
DATA_PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")

# 委員会 → 所管省庁キーワードのマッピング
COMMITTEE_MINISTRY_MAP = {
    "厚生労働委員会": ["厚生労働"],
    "経済産業委員会": ["経済産業"],
    "総務委員会": ["総務"],
    "法務委員会": ["法務"],
    "外務委員会": ["外務"],
    "財務金融委員会": ["財務", "金融"],
    "文部科学委員会": ["文部科学"],
    "農林水産委員会": ["農林水産"],
    "国土交通委員会": ["国土交通"],
    "環境委員会": ["環境"],
    "内閣委員会": ["内閣"],
}


def extract_role_level(position: str) -> str:
    """役職レベルを判定: 大臣 / 副大臣 / 大臣政務官"""
    if not position:
        return "不明"
    if "大臣政務官" in position:
        return "大臣政務官"
    if "副大臣" in position:
        return "副大臣"
    if "大臣" in position:
        return "大臣"
    return "不明"


def extract_ministry(position: str) -> str:
    """speakerPosition から省庁名を抽出

    例:
      "厚生労働大臣" → "厚生労働省"
      "厚生労働副大臣" → "厚生労働省"
      "内閣府大臣政務官" → "内閣府"
      "デジタル大臣・内閣府特命担当大臣（消費者及び食品安全・デジタル改革）" → "デジタル庁"
      "内閣府大臣政務官・復興大臣政務官" → "内閣府/復興庁"
    """
    if not position:
        return "不明"

    # 特殊ケース
    if "内閣総理大臣" in position:
        return "内閣"
    if "デジタル大臣" in position or "デジタル" in position:
        return "デジタル庁"

    # 一般パターン: 「○○大臣」「○○副大臣」「○○大臣政務官」
    # 複数兼務の場合は・で区切られている
    parts = position.split("・")
    ministries = []
    for part in parts:
        part = part.strip()
        # 「○○大臣政務官」「○○副大臣」「○○大臣」のパターン
        m = re.match(r"^(.+?)(?:大臣政務官|副大臣|大臣)", part)
        if m:
            ministry_name = m.group(1)
            # 「国務」大臣は省庁名が含まれない → 特命担当の括弧内を確認
            if ministry_name == "国務":
                paren = re.search(r"[（(](.+?)[）)]", position)
                if paren:
                    ministries.append(paren.group(1))
                else:
                    ministries.append("内閣府")
                continue
            # 「内閣府特命担当」のパターン
            if "特命担当" in ministry_name:
                ministries.append("内閣府")
                continue
            # 通常パターン
            if ministry_name.endswith("省") or ministry_name.endswith("庁") or ministry_name.endswith("府"):
                ministries.append(ministry_name)
            else:
                ministries.append(ministry_name + "省")

    if ministries:
        return "/".join(dict.fromkeys(ministries))  # 重複除去しつつ順序保持
    return "不明"


def is_responsible_ministry(position: str, committee: str) -> bool:
    """当該委員会の所管省庁かどうかを判定"""
    keywords = COMMITTEE_MINISTRY_MAP.get(committee, [])
    for kw in keywords:
        if kw in position:
            return True
    return False


def clean_speech_text(speech: str) -> str:
    """答弁テキストから冒頭の呼称を除去

    例: "○福岡国務大臣　ただいまの..." → "ただいまの..."
    """
    # ○発言者名\u3000本文 のパターン
    m = re.match(r"^○.+?[　\s]", speech)
    if m:
        return speech[m.end():]
    return speech


def get_questioner_from_context(speech_id: str, all_raw_speeches: list[dict]) -> dict:
    """答弁の直前の発言者（質問者）を推定

    同一issueID内でspeechOrderが1つ前のレコードを探す。
    """
    # speechIDから issueID と speechOrder を推定
    target = None
    for rec in all_raw_speeches:
        if rec.get("speechID") == speech_id:
            target = rec
            break
    if not target:
        return {"questioner": "不明", "questioner_group": "不明"}

    issue_id = target.get("issueID")
    speech_order = target.get("speechOrder", 0)

    # 1つ前のspeechOrderを探す
    prev = None
    for rec in all_raw_speeches:
        if rec.get("issueID") == issue_id and rec.get("speechOrder") == speech_order - 1:
            prev = rec
            break

    if prev:
        return {
            "questioner": prev.get("speaker", "不明"),
            "questioner_group": prev.get("speakerGroup", "不明"),
        }
    return {"questioner": "不明", "questioner_group": "不明"}


def classify_and_save(committee: str = "厚生労働委員会"):
    """政務三役答弁を分類してCSVに保存"""
    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)

    prefix = committee.replace("委員会", "")

    # 生データ読み込み
    political_path = os.path.join(DATA_RAW_DIR, f"speeches_political_{prefix}.json")
    if not os.path.exists(political_path):
        print(f"Error: {political_path} not found. Run fetch_speeches.py first.")
        return

    with open(political_path, "r", encoding="utf-8") as f:
        political_speeches = json.load(f)

    # 全生データ（質問者特定用）
    raw_path = os.path.join(DATA_RAW_DIR, f"speeches_raw_{prefix}.json")
    all_raw = []
    if os.path.exists(raw_path):
        with open(raw_path, "r", encoding="utf-8") as f:
            all_raw = json.load(f)

    classified = []
    for rec in political_speeches:
        position = rec.get("speakerPosition", "")
        speech_text = rec.get("speech", "")
        cleaned_text = clean_speech_text(speech_text)

        # 短すぎる答弁は除外（議事進行等）
        if len(cleaned_text) < 30:
            continue

        role_level = extract_role_level(position)
        ministry = extract_ministry(position)
        is_responsible = is_responsible_ministry(position, committee)

        # 質問者を推定
        q_info = get_questioner_from_context(rec.get("speechID", ""), all_raw)

        classified.append({
            "speech_id": rec.get("speechID", ""),
            "session": rec.get("session", ""),
            "committee": rec.get("nameOfMeeting", ""),
            "date": rec.get("date", ""),
            "issue": rec.get("issue", ""),
            "speaker": rec.get("speaker", ""),
            "speaker_position": position,
            "role_level": role_level,
            "ministry": ministry,
            "is_responsible": is_responsible,
            "responsibility_tag": "担当" if is_responsible else "非担当",
            "questioner": q_info["questioner"],
            "questioner_group": q_info["questioner_group"],
            "speech_text": cleaned_text,
            "speech_url": rec.get("speechURL", ""),
        })

    # CSV出力
    csv_path = os.path.join(DATA_PROCESSED_DIR, f"classified_speeches_{prefix}.csv")
    if classified:
        fieldnames = list(classified[0].keys())
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(classified)

    print(f"=== Classification Complete ===")
    print(f"Total classified speeches: {len(classified)}")

    # 統計
    responsible = [r for r in classified if r["is_responsible"]]
    non_responsible = [r for r in classified if not r["is_responsible"]]

    print(f"\n担当省庁 ({prefix}): {len(responsible)}")
    for level in ["大臣", "副大臣", "大臣政務官"]:
        count = sum(1 for r in responsible if r["role_level"] == level)
        if count > 0:
            print(f"  {level}: {count}")

    print(f"\n非担当省庁: {len(non_responsible)}")
    from collections import Counter
    ministry_counts = Counter(r["ministry"] for r in non_responsible)
    for ministry, count in ministry_counts.most_common():
        levels = Counter(r["role_level"] for r in non_responsible if r["ministry"] == ministry)
        level_str = ", ".join(f"{l}:{c}" for l, c in levels.most_common())
        print(f"  {ministry} ({level_str}): {count}")

    print(f"\nSaved to {csv_path}")
    return classified


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--committee", default="厚生労働委員会", help="委員会名")
    args = parser.parse_args()
    classify_and_save(args.committee)
