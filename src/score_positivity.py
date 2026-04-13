#!/usr/bin/env python3
"""
Phase 2: LLMによる答弁の前向き度スコアリング

DeepInfra API (OpenAI互換) を使用して各答弁テキストを5段階で評価する。

スコアリング基準:
  1 = 拒否的: 「対応する考えはない」「困難」「慎重に」
  2 = 消極的: 「現時点では」「引き続き注視」「状況を見守る」
  3 = 中立:   事実説明のみ、評価を含まない
  4 = 前向き: 「検討してまいりたい」「重要な課題と認識」「しっかり取り組む」
  5 = コミット: 具体的施策・時期への言及、「実施する」「予算を確保」

注意: 「検討」は文脈次第。「前向きに検討」は4、「慎重に検討」は2。

Usage:
    python src/score_positivity.py
"""

import csv
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
DATA_SCORED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "scored")

DEEPINFRA_API_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
MODEL = "google/gemma-4-31B-it"

SCORING_PROMPT = """あなたは国会答弁の「前向き度」を評価する専門家です。

以下の答弁テキストを読み、政策実現への積極性を1〜5の整数で評価してください。

## 評価基準

| スコア | ラベル | 判定基準 |
|--------|--------|----------|
| 1 | 拒否的 | 「対応する考えはない」「困難である」「慎重にならざるを得ない」など、明確な拒否・否定 |
| 2 | 消極的 | 「現時点では」「引き続き注視」「状況を見守る」「慎重に検討」など、実質的な先送り |
| 3 | 中立 | 事実説明・制度説明のみで、政策判断や評価を含まない |
| 4 | 前向き | 「検討してまいりたい」「重要な課題と認識」「しっかり取り組む」「前向きに検討」など、積極的姿勢 |
| 5 | コミット | 具体的な施策・時期・予算への言及、「実施する」「予算を確保した」「法案を提出する」など |

## 注意事項
- 「検討」は文脈次第：「前向きに検討」→4、「慎重に検討」→2
- 複数の要素が混在する場合は、答弁全体のトーンで判断
- 質問への直接的な回答部分を重視し、前置きの一般論は軽視

## 出力形式
以下のJSON形式のみを出力してください。他のテキストは不要です。
{"score": <1-5の整数>, "reason": "<20文字以内の判定根拠>"}

## 答弁テキスト
"""


def score_speech(text: str, max_retries: int = 3) -> dict:
    """1件の答弁テキストをLLMでスコアリング"""
    # テキストが長すぎる場合は先頭を使用
    truncated = text[:2000] if len(text) > 2000 else text

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "user", "content": SCORING_PROMPT + truncated}
        ],
        "temperature": 0.1,
        "max_tokens": 100,
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
                m = re.search(r'\{[^}]+\}', content)
                if m:
                    parsed = json.loads(m.group())
                else:
                    raise ValueError(f"Cannot parse LLM response: {content}")

            score = int(parsed["score"])
            if score < 1 or score > 5:
                raise ValueError(f"Score out of range: {score}")

            return {
                "score": score,
                "reason": parsed.get("reason", ""),
                "raw_response": content,
            }

        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"    Retry {attempt + 1} after error: {e} (waiting {wait}s)")
                time.sleep(wait)
            else:
                print(f"    FAILED after {max_retries} attempts: {e}")
                return {"score": -1, "reason": f"ERROR: {e}", "raw_response": ""}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--committee", default="厚生労働委員会", help="委員会名")
    args = parser.parse_args()

    prefix = args.committee.replace("委員会", "")

    if not DEEPINFRA_API_KEY:
        print("Error: DEEPINFRA_API_KEY environment variable not set")
        sys.exit(1)

    os.makedirs(DATA_SCORED_DIR, exist_ok=True)

    # 分類済みデータ読み込み
    csv_path = os.path.join(DATA_PROCESSED_DIR, f"classified_speeches_{prefix}.csv")
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found. Run classify_speaker.py first.")
        sys.exit(1)

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        speeches = list(reader)

    total = len(speeches)
    print(f"=== Scoring {total} speeches ({args.committee}) ===")

    # コスト概算
    est_input_tokens = total * 800
    est_output_tokens = total * 50
    print(f"Estimated tokens: ~{est_input_tokens:,} input + ~{est_output_tokens:,} output")
    print(f"(DeepInfra pricing applies)\n")

    # 既存のスコアリング結果があれば途中から再開
    suffix = f"_{prefix}" if prefix != "厚生労働" else ""
    scored_path = os.path.join(DATA_SCORED_DIR, f"scored_speeches{suffix}.csv")
    scored_ids = set()
    existing_scored = []
    if os.path.exists(scored_path):
        with open(scored_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_scored = list(reader)
            scored_ids = {r["speech_id"] for r in existing_scored}
        print(f"Resuming: {len(scored_ids)} already scored\n")

    results = list(existing_scored)

    # 未処理のみ抽出
    pending = [(i, s) for i, s in enumerate(speeches) if s["speech_id"] not in scored_ids]
    print(f"Pending: {len(pending)} speeches to score\n")

    if not pending:
        print("All speeches already scored.")
    else:
        PARALLEL_WORKERS = 10

        def _process(item):
            idx, speech = item
            result = score_speech(speech["speech_text"])
            return {**speech, **result}

        completed = 0
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            futures = {executor.submit(_process, item): item for item in pending}
            for future in as_completed(futures):
                idx, speech = futures[future]
                scored_entry = future.result()
                results.append(scored_entry)
                completed += 1
                if completed % 20 == 0:
                    print(f"  [{completed}/{len(pending)}] scored...")
                if completed % 100 == 0:
                    _save_results(results, scored_path)
                    print(f"  [Checkpoint] Saved {len(results)} records")

    # 最終保存
    _save_results(results, scored_path)

    # サマリー
    valid = [r for r in results if int(r.get("score", -1)) > 0]
    print(f"\n=== Scoring Complete ===")
    print(f"Total scored: {len(valid)} / {total}")
    print(f"Failed: {total - len(valid)}")

    if valid:
        from collections import Counter
        score_dist = Counter(int(r["score"]) for r in valid)
        print(f"\nScore distribution:")
        for s in range(1, 6):
            count = score_dist.get(s, 0)
            bar = "#" * (count // 5)
            print(f"  {s}: {count:4d} {bar}")


def _save_results(results: list[dict], path: str):
    """結果をCSVに保存"""
    if not results:
        return
    fieldnames = list(results[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    main()
