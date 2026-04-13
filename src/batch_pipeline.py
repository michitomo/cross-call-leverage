#!/usr/bin/env python3
"""
一括パイプライン: 複数委員会×過去20回国会のデータ取得・分類・スコアリング・タグ付け

Usage:
    python src/batch_pipeline.py --phase fetch
    python src/batch_pipeline.py --phase classify
    python src/batch_pipeline.py --phase score
    python src/batch_pipeline.py --phase tag
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROC = BASE_DIR / "data" / "processed"
DATA_SCORED = BASE_DIR / "data" / "scored"

API_BASE = "https://kokkai.ndl.go.jp/api/speech"
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_API_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
MODEL = "google/gemma-4-31B-it"

COMMITTEES = [
    "厚生労働委員会",
    "総務委員会",
    "内閣委員会",
    "経済産業委員会",
    "国土交通委員会",
]

SESSIONS = list(range(201, 221))

COMMITTEE_MINISTRY_MAP = {
    "厚生労働委員会": ["厚生労働"],
    "総務委員会": ["総務"],
    "内閣委員会": ["内閣"],
    "経済産業委員会": ["経済産業"],
    "国土交通委員会": ["国土交通"],
    "環境委員会": ["環境"],
    "文部科学委員会": ["文部科学"],
    "法務委員会": ["法務"],
    "財務金融委員会": ["財務", "金融"],
    "農林水産委員会": ["農林水産"],
    "外務委員会": ["外務"],
}

SCORING_PROMPT = """あなたは国会答弁の「前向き度」を評価する専門家です。
以下の答弁テキストを読み、政策実現への積極性を1〜5の整数で評価してください。

## 評価基準
| スコア | ラベル | 判定基準 |
|--------|--------|----------|
| 1 | 拒否的 | 明確な拒否・否定 |
| 2 | 消極的 | 実質的な先送り・「慎重に検討」 |
| 3 | 中立 | 事実説明のみ |
| 4 | 前向き | 「検討してまいりたい」「前向きに検討」 |
| 5 | コミット | 具体的施策・時期・予算への言及 |

## 注意: 「検討」は文脈次第。「前向きに検討」→4、「慎重に検討」→2
## 出力: 以下のJSON形式のみ。
{"score": <1-5>, "reason": "<20文字以内>"}

## 答弁テキスト
"""

TAGGING_PROMPT = """あなたは国会答弁を分析する専門家です。該当するタグをすべて選んでください。

## 姿勢タグ
- COMMIT: 具体的行動を約束 / POSITIVE: 積極的姿勢 / CONSIDER: 検討段階
- EXPLAIN: 事実説明に終始 / DEFER: 先送り / REFUSE: 拒否

## 内容タグ
- SPECIFIC_PLAN: 具体的計画 / OWN_OPINION: 個人見解 / BUREAUCRATIC: 官僚的
- CROSS_MINISTRY: 他省庁連携 / BUDGET: 予算言及 / TIMELINE: 時期言及

## 出力: JSONのみ。
{"attitude": ["タグ1"], "content": ["タグ1"]}

## 答弁テキスト
"""


def prefix(committee):
    return committee.replace("委員会", "")


# ========== Phase: Fetch ==========

def fetch_page(params):
    query = urllib.parse.urlencode(params)
    url = f"{API_BASE}?{query}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def is_political_appointee(pos):
    if not pos:
        return False
    if "官房" in pos:
        return False
    return "副大臣" in pos or "大臣政務官" in pos or "大臣" in pos


def fetch_committee(committee):
    p = prefix(committee)
    raw_path = DATA_RAW / f"speeches_raw_{p}.json"
    pol_path = DATA_RAW / f"speeches_political_{p}.json"

    if raw_path.exists():
        with open(raw_path, "r", encoding="utf-8") as f:
            existing = json.load(f)
        existing_ids = {r.get("speechID") for r in existing}
        print(f"  {committee}: {len(existing)} existing records, checking for new sessions...")
    else:
        existing = []
        existing_ids = set()

    all_records = list(existing)

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
            print(f"    Session {session}: ERROR {e}")
            time.sleep(1)
            continue

        total = first.get("numberOfRecords", 0)
        if total == 0:
            continue

        records = first.get("speechRecord", [])
        new_in_page = [r for r in records if r.get("speechID") not in existing_ids]

        if not new_in_page and len(records) > 0:
            # Session already fetched
            continue

        all_records.extend(new_in_page)
        for r in new_in_page:
            existing_ids.add(r.get("speechID"))

        next_pos = first.get("nextRecordPosition")
        page = 1
        while next_pos and next_pos <= total:
            time.sleep(1)
            params["startRecord"] = next_pos
            page += 1
            try:
                data = fetch_page(params)
            except Exception as e:
                print(f"    Session {session} page {page}: ERROR {e}")
                time.sleep(2)
                break
            recs = data.get("speechRecord", [])
            new_recs = [r for r in recs if r.get("speechID") not in existing_ids]
            all_records.extend(new_recs)
            for r in new_recs:
                existing_ids.add(r.get("speechID"))
            next_pos = data.get("nextRecordPosition")

        added = len(all_records) - len(existing)
        if added > 0:
            print(f"    Session {session}: +{len(new_in_page)} (total in API: {total})")
        time.sleep(1)

    # Save
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    political = [r for r in all_records if is_political_appointee(r.get("speakerPosition", ""))]
    with open(pol_path, "w", encoding="utf-8") as f:
        json.dump(political, f, ensure_ascii=False, indent=2)

    print(f"  {committee}: {len(all_records)} total raw, {len(political)} political")
    return len(political)


# ========== Phase: Classify ==========

def extract_role_level(pos):
    if not pos:
        return "不明"
    if "大臣政務官" in pos:
        return "大臣政務官"
    if "副大臣" in pos:
        return "副大臣"
    if "大臣" in pos:
        return "大臣"
    return "不明"


def extract_ministry(pos):
    if not pos:
        return "不明"
    if "内閣総理大臣" in pos:
        return "内閣"
    if "デジタル大臣" in pos or "デジタル" in pos:
        return "デジタル庁"
    parts = pos.split("・")
    ministries = []
    for part in parts:
        m = re.match(r"^(.+?)(?:大臣政務官|副大臣|大臣)", part.strip())
        if m:
            name = m.group(1)
            if name == "国務":
                paren = re.search(r"[（(](.+?)[）)]", pos)
                ministries.append(paren.group(1) if paren else "内閣府")
                continue
            if "特命担当" in name:
                ministries.append("内閣府")
                continue
            if name.endswith(("省", "庁", "府")):
                ministries.append(name)
            else:
                ministries.append(name + "省")
    return "/".join(dict.fromkeys(ministries)) if ministries else "不明"


def is_responsible(pos, committee):
    kws = COMMITTEE_MINISTRY_MAP.get(committee, [])
    return any(kw in pos for kw in kws)


def clean_speech(text):
    m = re.match(r"^○.+?[　\s]", text)
    return text[m.end():] if m else text


def classify_committee(committee):
    p = prefix(committee)
    pol_path = DATA_RAW / f"speeches_political_{p}.json"
    raw_path = DATA_RAW / f"speeches_raw_{p}.json"
    out_path = DATA_PROC / f"classified_speeches_{p}.csv"

    if not pol_path.exists():
        print(f"  {committee}: no data")
        return 0

    with open(pol_path, "r", encoding="utf-8") as f:
        political = json.load(f)
    all_raw = []
    if raw_path.exists():
        with open(raw_path, "r", encoding="utf-8") as f:
            all_raw = json.load(f)

    # Build index for questioner lookup
    by_issue_order = {}
    for r in all_raw:
        key = (r.get("issueID"), r.get("speechOrder", 0) - 1)
        by_issue_order[key] = r

    classified = []
    for rec in political:
        pos = rec.get("speakerPosition", "")
        text = clean_speech(rec.get("speech", ""))
        if len(text) < 30:
            continue

        prev_key = (rec.get("issueID"), rec.get("speechOrder", 0) - 1)
        prev = by_issue_order.get(prev_key)
        q_name = prev.get("speaker", "不明") if prev else "不明"
        q_group = prev.get("speakerGroup", "不明") if prev else "不明"

        classified.append({
            "speech_id": rec.get("speechID", ""),
            "session": rec.get("session", ""),
            "committee": rec.get("nameOfMeeting", ""),
            "date": rec.get("date", ""),
            "issue": rec.get("issue", ""),
            "speaker": rec.get("speaker", ""),
            "speaker_position": pos,
            "role_level": extract_role_level(pos),
            "ministry": extract_ministry(pos),
            "is_responsible": is_responsible(pos, committee),
            "responsibility_tag": "担当" if is_responsible(pos, committee) else "非担当",
            "questioner": q_name,
            "questioner_group": q_group,
            "speech_text": text,
            "speech_url": rec.get("speechURL", ""),
        })

    DATA_PROC.mkdir(parents=True, exist_ok=True)
    if classified:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(classified[0].keys()))
            w.writeheader()
            w.writerows(classified)

    resp = sum(1 for r in classified if r["is_responsible"])
    non_resp = len(classified) - resp
    print(f"  {committee}: {len(classified)} classified (担当:{resp}, 非担当:{non_resp})")
    return len(classified)


# ========== Phase: Score / Tag ==========

def call_llm(prompt_text, max_retries=3):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPINFRA_API_KEY}",
    }
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt_text}],
        "temperature": 0.1,
        "max_tokens": 150,
    }
    for attempt in range(max_retries):
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(DEEPINFRA_URL, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            return result["choices"][0]["message"]["content"].strip()
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
            else:
                raise


def parse_json_response(content):
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        m = re.search(r'\{[^}]+\}', content, re.DOTALL)
        if m:
            return json.loads(m.group())
        raise ValueError(f"Cannot parse: {content[:100]}")


def score_one(text):
    truncated = text[:2000]
    try:
        resp = call_llm(SCORING_PROMPT + truncated)
        parsed = parse_json_response(resp)
        s = int(parsed["score"])
        if not 1 <= s <= 5:
            raise ValueError(f"score={s}")
        return {"score": s, "reason": parsed.get("reason", "")}
    except Exception as e:
        return {"score": -1, "reason": f"ERROR: {e}"}


def tag_one(text):
    truncated = text[:2000]
    valid_att = {"COMMIT", "POSITIVE", "CONSIDER", "EXPLAIN", "DEFER", "REFUSE"}
    valid_con = {"SPECIFIC_PLAN", "OWN_OPINION", "BUREAUCRATIC", "CROSS_MINISTRY", "BUDGET", "TIMELINE"}
    try:
        resp = call_llm(TAGGING_PROMPT + truncated)
        parsed = parse_json_response(resp)
        att = [t for t in parsed.get("attitude", []) if t in valid_att]
        con = [t for t in parsed.get("content", []) if t in valid_con]
        return {"attitude_tags": ",".join(att), "content_tags": ",".join(con)}
    except Exception as e:
        return {"attitude_tags": "ERROR", "content_tags": "ERROR"}


def process_llm(committee, mode="score"):
    """Score or tag a committee's speeches with parallel LLM calls."""
    p = prefix(committee)
    csv_path = DATA_PROC / f"classified_speeches_{p}.csv"
    if not csv_path.exists():
        print(f"  {committee}: no classified data")
        return

    with open(csv_path, "r", encoding="utf-8") as f:
        speeches = list(csv.DictReader(f))

    suffix = f"_{p}" if p != "厚生労働" else ""
    if mode == "score":
        out_path = DATA_SCORED / f"scored_speeches{suffix}.csv"
    else:
        out_path = DATA_SCORED / f"tagged_speeches{suffix}.csv"

    DATA_SCORED.mkdir(parents=True, exist_ok=True)

    # Resume
    done_ids = set()
    existing = []
    if out_path.exists():
        with open(out_path, "r", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
            done_ids = {r["speech_id"] for r in existing}

    pending = [s for s in speeches if s["speech_id"] not in done_ids]
    print(f"  {committee} [{mode}]: {len(pending)} pending / {len(speeches)} total (resume {len(done_ids)})")

    if not pending:
        return

    results = list(existing)

    def _process(speech):
        if mode == "score":
            res = score_one(speech["speech_text"])
        else:
            res = tag_one(speech["speech_text"])
        return {**speech, **res}

    completed = 0
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(_process, s): s for s in pending}
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1
            if completed % 50 == 0:
                print(f"    [{completed}/{len(pending)}] {mode}d...")
            if completed % 200 == 0:
                _save_csv(results, out_path)

    _save_csv(results, out_path)
    valid = sum(1 for r in results if mode != "score" or str(r.get("score", -1)) != "-1")
    print(f"  {committee} [{mode}]: done ({valid}/{len(results)} valid)")


def _save_csv(rows, path):
    if not rows:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


# ========== Main ==========

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", required=True, choices=["fetch", "classify", "score", "tag"])
    parser.add_argument("--committee", default=None, help="Single committee (default: all)")
    args = parser.parse_args()

    committees = [args.committee] if args.committee else COMMITTEES

    if args.phase == "fetch":
        print("=== Fetching data ===")
        for c in committees:
            print(f"\n{c}:")
            fetch_committee(c)

    elif args.phase == "classify":
        print("=== Classifying speakers ===")
        for c in committees:
            classify_committee(c)

    elif args.phase == "score":
        if not DEEPINFRA_API_KEY:
            print("Error: DEEPINFRA_API_KEY not set")
            sys.exit(1)
        print("=== Scoring ===")
        for c in committees:
            process_llm(c, "score")

    elif args.phase == "tag":
        if not DEEPINFRA_API_KEY:
            print("Error: DEEPINFRA_API_KEY not set")
            sys.exit(1)
        print("=== Tagging ===")
        for c in committees:
            process_llm(c, "tag")


if __name__ == "__main__":
    main()
