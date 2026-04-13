#!/usr/bin/env python3
"""
Phase 2 補完: 答弁の複数選択タグ分類

5段階スコアだけでなく、LLMが判断しやすい複数選択タグで答弁を分類する。
タグは相互排他ではなく、該当するものすべてを選択させる。

カテゴリ:
  A. 答弁の姿勢タグ（複数選択可）
    - COMMIT: 具体的施策・時期・数値に言及
    - POSITIVE: 前向きな姿勢・意欲を表明
    - CONSIDER: 検討・議論する姿勢
    - EXPLAIN: 事実・制度の説明に終始
    - DEFER: 先送り・注視・見守り
    - REFUSE: 明確な拒否・否定

  B. 答弁の内容タグ（複数選択可）
    - SPECIFIC_PLAN: 具体的な計画・数値目標あり
    - OWN_OPINION: 答弁者個人の見解・判断を含む
    - BUREAUCRATIC: 官僚的・定型的な答弁
    - CROSS_MINISTRY: 他省庁との連携に言及
    - BUDGET: 予算・財源に言及
    - TIMELINE: 時期・期限に言及

Usage:
    python src/tag_speeches.py
"""

import csv
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
DATA_SCORED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "scored")

DEEPINFRA_API_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
MODEL = "google/gemma-4-31B-it"

TAGGING_PROMPT = """あなたは国会答弁を分析する専門家です。

以下の答弁テキストを読み、該当するタグをすべて選んでください。

## 姿勢タグ（該当するものすべて選択）
- COMMIT: 「実施する」「予算を確保」「法案を提出」など具体的行動を約束
- POSITIVE: 「しっかり取り組む」「重要と認識」「前向きに検討」など積極的姿勢
- CONSIDER: 「検討する」「議論を進める」「研究する」など検討段階
- EXPLAIN: 制度説明・現状報告など事実の説明に終始
- DEFER: 「注視する」「見守る」「現時点では」など実質的先送り
- REFUSE: 「困難」「考えていない」「慎重にならざるを得ない」など拒否

## 内容タグ（該当するものすべて選択）
- SPECIFIC_PLAN: 具体的な数値・計画・施策名に言及
- OWN_OPINION: 答弁者自身の見解・判断・感想を含む（「私としては」「大変重要だと思う」等）
- BUREAUCRATIC: 定型句が多い・官僚的な表現（「～と承知しております」「～の趣旨で」）
- CROSS_MINISTRY: 他省庁との連携・協力に言及
- BUDGET: 予算・財源・費用に言及
- TIMELINE: 具体的な時期・期限・スケジュールに言及

## 出力形式
以下のJSON形式のみを出力してください。
{"attitude": ["タグ1", "タグ2"], "content": ["タグ1", "タグ2"]}

例: {"attitude": ["POSITIVE", "CONSIDER"], "content": ["OWN_OPINION", "CROSS_MINISTRY"]}

## 答弁テキスト
"""


def tag_speech(text: str, max_retries: int = 3) -> dict:
    """1件の答弁をタグ分類"""
    truncated = text[:2000] if len(text) > 2000 else text

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "user", "content": TAGGING_PROMPT + truncated}
        ],
        "temperature": 0.1,
        "max_tokens": 150,
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPINFRA_API_KEY}",
    }

    for attempt in range(max_retries):
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(DEEPINFRA_URL, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))

            content = result["choices"][0]["message"]["content"].strip()
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                m = re.search(r'\{[^}]*\}', content, re.DOTALL)
                if m:
                    parsed = json.loads(m.group())
                else:
                    raise ValueError(f"Cannot parse: {content}")

            attitude_tags = parsed.get("attitude", [])
            content_tags = parsed.get("content", [])

            # バリデーション
            valid_attitude = {"COMMIT", "POSITIVE", "CONSIDER", "EXPLAIN", "DEFER", "REFUSE"}
            valid_content = {"SPECIFIC_PLAN", "OWN_OPINION", "BUREAUCRATIC", "CROSS_MINISTRY", "BUDGET", "TIMELINE"}
            attitude_tags = [t for t in attitude_tags if t in valid_attitude]
            content_tags = [t for t in content_tags if t in valid_content]

            return {
                "attitude_tags": ",".join(attitude_tags),
                "content_tags": ",".join(content_tags),
                "raw_tag_response": content,
            }

        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"    Retry {attempt + 1}: {e} (waiting {wait}s)")
                time.sleep(wait)
            else:
                print(f"    FAILED: {e}")
                return {"attitude_tags": "ERROR", "content_tags": "ERROR", "raw_tag_response": ""}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--committee", default="厚生労働委員会", help="委員会名")
    args = parser.parse_args()

    prefix = args.committee.replace("委員会", "")

    if not DEEPINFRA_API_KEY:
        print("Error: DEEPINFRA_API_KEY not set")
        sys.exit(1)

    os.makedirs(DATA_SCORED_DIR, exist_ok=True)

    csv_path = os.path.join(DATA_PROCESSED_DIR, f"classified_speeches_{prefix}.csv")
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found")
        sys.exit(1)

    with open(csv_path, "r", encoding="utf-8") as f:
        speeches = list(csv.DictReader(f))

    total = len(speeches)
    print(f"=== Tagging {total} speeches ({args.committee}) ===")

    # 途中再開対応
    suffix = f"_{prefix}" if prefix != "厚生労働" else ""
    tagged_path = os.path.join(DATA_SCORED_DIR, f"tagged_speeches{suffix}.csv")
    tagged_ids = set()
    existing = []
    if os.path.exists(tagged_path):
        with open(tagged_path, "r", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
            tagged_ids = {r["speech_id"] for r in existing}
        print(f"Resuming: {len(tagged_ids)} already tagged\n")

    results = list(existing)

    pending = [(i, s) for i, s in enumerate(speeches) if s["speech_id"] not in tagged_ids]
    print(f"Pending: {len(pending)} speeches to tag\n")

    if not pending:
        print("All speeches already tagged.")
    else:
        PARALLEL_WORKERS = 10

        def _process(item):
            idx, speech = item
            tags = tag_speech(speech["speech_text"])
            return {**speech, **tags}

        completed = 0
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            futures = {executor.submit(_process, item): item for item in pending}
            for future in as_completed(futures):
                entry = future.result()
                results.append(entry)
                completed += 1
                if completed % 20 == 0:
                    print(f"  [{completed}/{len(pending)}] tagged...")
                if completed % 100 == 0:
                    _save(results, tagged_path)
                    print(f"  [Checkpoint] Saved {len(results)} records")

    _save(results, tagged_path)

    # サマリー
    from collections import Counter
    att_counter = Counter()
    con_counter = Counter()
    for r in results:
        for t in r.get("attitude_tags", "").split(","):
            if t and t != "ERROR":
                att_counter[t] += 1
        for t in r.get("content_tags", "").split(","):
            if t and t != "ERROR":
                con_counter[t] += 1

    print(f"\n=== Tagging Complete ===")
    print(f"Total tagged: {len(results)}")
    print(f"\nAttitude tag distribution:")
    for tag, count in att_counter.most_common():
        print(f"  {tag}: {count}")
    print(f"\nContent tag distribution:")
    for tag, count in con_counter.most_common():
        print(f"  {tag}: {count}")


def _save(results, path):
    if not results:
        return
    fieldnames = list(results[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    main()
