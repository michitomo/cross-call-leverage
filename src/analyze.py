#!/usr/bin/env python3
"""
Phase 3: 集計・比較分析

担当大臣 vs 非担当副大臣・政務官の前向き度スコア・タグ分布を比較分析する。
Mann-Whitney U検定、記述統計、タグ出現率の比較を行う。

Usage:
    python src/analyze.py
"""

import csv
import json
import os
from collections import Counter

DATA_SCORED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "scored")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")

ATTITUDE_TAGS = ["COMMIT", "POSITIVE", "CONSIDER", "EXPLAIN", "DEFER", "REFUSE"]
CONTENT_TAGS = ["SPECIFIC_PLAN", "OWN_OPINION", "BUREAUCRATIC", "CROSS_MINISTRY", "BUDGET", "TIMELINE"]


def load_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def merge_score_and_tags(scored: list[dict], tagged: list[dict]) -> list[dict]:
    """スコアデータとタグデータをspeech_idでマージ"""
    tag_map = {r["speech_id"]: r for r in tagged}
    merged = []
    for rec in scored:
        sid = rec["speech_id"]
        tag_rec = tag_map.get(sid, {})
        merged_rec = {**rec}
        merged_rec["attitude_tags"] = tag_rec.get("attitude_tags", "")
        merged_rec["content_tags"] = tag_rec.get("content_tags", "")
        merged.append(merged_rec)
    return merged


def compute_stats(scores: list[int]) -> dict:
    """記述統計"""
    if not scores:
        return {"n": 0, "mean": 0, "median": 0, "std": 0}
    n = len(scores)
    mean = sum(scores) / n
    sorted_s = sorted(scores)
    median = sorted_s[n // 2] if n % 2 == 1 else (sorted_s[n // 2 - 1] + sorted_s[n // 2]) / 2
    variance = sum((x - mean) ** 2 for x in scores) / n
    std = variance ** 0.5
    return {"n": n, "mean": round(mean, 2), "median": median, "std": round(std, 2)}


def mann_whitney_u(x: list[int], y: list[int]) -> dict:
    """Mann-Whitney U検定（scipy不使用の手動実装）"""
    nx, ny = len(x), len(y)
    if nx == 0 or ny == 0:
        return {"U": None, "p_approx": None, "significant": None}

    # 全データを結合してランク付け
    combined = [(val, 'x') for val in x] + [(val, 'y') for val in y]
    combined.sort(key=lambda t: t[0])

    # タイを考慮したランク付け
    ranks = {}
    i = 0
    while i < len(combined):
        j = i
        while j < len(combined) and combined[j][0] == combined[i][0]:
            j += 1
        avg_rank = (i + 1 + j) / 2
        for k in range(i, j):
            if k not in ranks:
                ranks[k] = []
            ranks[k] = avg_rank
        i = j

    # xグループのランク和
    rank_sum_x = sum(ranks[i] for i, (_, group) in enumerate(combined) if group == 'x')

    U_x = rank_sum_x - nx * (nx + 1) / 2
    U_y = nx * ny - U_x
    U = min(U_x, U_y)

    # 正規近似（n >= 20の場合）
    mu = nx * ny / 2
    sigma = (nx * ny * (nx + ny + 1) / 12) ** 0.5
    if sigma == 0:
        return {"U": U, "z": 0, "p_approx": 1.0, "significant": False}
    z = (U - mu) / sigma

    # 正規分布のp値近似（両側検定）
    import math
    p = 2 * (1 - _norm_cdf(abs(z)))

    return {
        "U": round(U, 1),
        "z": round(z, 3),
        "p_approx": round(p, 4),
        "significant_005": p < 0.05,
        "significant_001": p < 0.01,
    }


def _norm_cdf(x):
    """標準正規分布のCDF近似"""
    import math
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def tag_rates(records: list[dict], tag_field: str, valid_tags: list[str]) -> dict:
    """タグの出現率を計算"""
    n = len(records)
    if n == 0:
        return {tag: 0 for tag in valid_tags}
    counts = Counter()
    for rec in records:
        tags = rec.get(tag_field, "").split(",")
        for t in tags:
            t = t.strip()
            if t in valid_tags:
                counts[t] += 1
    return {tag: round(counts[tag] / n * 100, 1) for tag in valid_tags}


def analyze_committee(data: list[dict], committee_name: str) -> dict:
    """1つの委員会のデータを分析"""
    # 内閣総理大臣を除外
    data = [r for r in data if "内閣総理大臣" not in r.get("speaker_position", "")]

    # グループ分け
    resp_minister = [r for r in data
                     if r.get("is_responsible", "").lower() in ("true", "1")
                     and r.get("role_level") == "大臣"]
    resp_vice = [r for r in data
                 if r.get("is_responsible", "").lower() in ("true", "1")
                 and r.get("role_level") in ("副大臣", "大臣政務官")]
    non_resp = [r for r in data
                if r.get("is_responsible", "").lower() in ("false", "0")
                and r.get("role_level") in ("副大臣", "大臣政務官")]

    # スコア分析
    def get_scores(records):
        return [int(r["score"]) for r in records if r.get("score", "-1") not in ("-1", "")]

    scores_rm = get_scores(resp_minister)
    scores_rv = get_scores(resp_vice)
    scores_nr = get_scores(non_resp)

    stats_rm = compute_stats(scores_rm)
    stats_rv = compute_stats(scores_rv)
    stats_nr = compute_stats(scores_nr)

    # Mann-Whitney U: 担当大臣 vs 非担当副大臣・政務官
    mw_rm_nr = mann_whitney_u(scores_rm, scores_nr)
    # 担当副大臣・政務官 vs 非担当副大臣・政務官（役職揃え比較）
    mw_rv_nr = mann_whitney_u(scores_rv, scores_nr)

    # スコア分布
    def score_dist(scores):
        c = Counter(scores)
        return {s: c.get(s, 0) for s in range(1, 6)}

    # タグ分析
    att_rm = tag_rates(resp_minister, "attitude_tags", ATTITUDE_TAGS)
    att_rv = tag_rates(resp_vice, "attitude_tags", ATTITUDE_TAGS)
    att_nr = tag_rates(non_resp, "attitude_tags", ATTITUDE_TAGS)

    con_rm = tag_rates(resp_minister, "content_tags", CONTENT_TAGS)
    con_rv = tag_rates(resp_vice, "content_tags", CONTENT_TAGS)
    con_nr = tag_rates(non_resp, "content_tags", CONTENT_TAGS)

    # 質問者の政党分析
    questioner_group_analysis = {}
    for group_label in ["与党", "野党"]:
        ruling_parties = {"自由民主党", "公明党", "自由民主党・無所属の会"}
        if group_label == "与党":
            filtered = [r for r in non_resp if r.get("questioner_group", "") in ruling_parties]
        else:
            filtered = [r for r in non_resp if r.get("questioner_group", "") not in ruling_parties and r.get("questioner_group", "") != "不明"]
        q_scores = get_scores(filtered)
        questioner_group_analysis[group_label] = {
            "stats": compute_stats(q_scores),
            "attitude_tags": tag_rates(filtered, "attitude_tags", ATTITUDE_TAGS),
        }

    return {
        "committee": committee_name,
        "groups": {
            "担当大臣": {"stats": stats_rm, "dist": score_dist(scores_rm), "attitude_tags": att_rm, "content_tags": con_rm},
            "担当副大臣・政務官": {"stats": stats_rv, "dist": score_dist(scores_rv), "attitude_tags": att_rv, "content_tags": con_rv},
            "非担当副大臣・政務官": {"stats": stats_nr, "dist": score_dist(scores_nr), "attitude_tags": att_nr, "content_tags": con_nr},
        },
        "tests": {
            "担当大臣_vs_非担当副大臣政務官": mw_rm_nr,
            "担当副大臣政務官_vs_非担当副大臣政務官": mw_rv_nr,
        },
        "questioner_analysis": questioner_group_analysis,
        "non_resp_details": [
            {
                "speaker": r.get("speaker"),
                "position": r.get("speaker_position"),
                "ministry": r.get("ministry"),
                "date": r.get("date"),
                "score": r.get("score"),
                "attitude_tags": r.get("attitude_tags"),
                "content_tags": r.get("content_tags"),
                "speech_text": r.get("speech_text", "")[:200],
                "questioner": r.get("questioner"),
                "questioner_group": r.get("questioner_group"),
                "speech_url": r.get("speech_url"),
            }
            for r in non_resp
        ],
    }


def print_analysis(result: dict):
    """分析結果をコンソール出力"""
    print(f"\n{'='*60}")
    print(f"  {result['committee']} 分析結果")
    print(f"{'='*60}")

    for group_name, group_data in result["groups"].items():
        stats = group_data["stats"]
        print(f"\n--- {group_name} (n={stats['n']}) ---")
        print(f"  平均: {stats['mean']}, 中央値: {stats['median']}, 標準偏差: {stats['std']}")
        dist = group_data["dist"]
        print(f"  分布: ", end="")
        for s in range(1, 6):
            print(f"{s}:{dist[s]} ", end="")
        print()

        att = group_data.get("attitude_tags", {})
        if att:
            print(f"  姿勢タグ: ", end="")
            for tag in ATTITUDE_TAGS:
                print(f"{tag}:{att.get(tag, 0)}% ", end="")
            print()

    print(f"\n--- 統計検定 ---")
    for test_name, test_data in result["tests"].items():
        if test_data.get("U") is not None:
            sig = "*" if test_data.get("significant_005") else ""
            print(f"  {test_name}: U={test_data['U']}, z={test_data['z']}, p={test_data['p_approx']}{sig}")
        else:
            print(f"  {test_name}: データ不足")

    print(f"\n--- 質問者政党別（非担当答弁）---")
    for group, analysis in result.get("questioner_analysis", {}).items():
        stats = analysis["stats"]
        print(f"  {group} (n={stats['n']}): 平均={stats['mean']}, 中央値={stats['median']}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 厚労委データ
    scored = load_csv(os.path.join(DATA_SCORED_DIR, "scored_speeches.csv"))
    tagged = load_csv(os.path.join(DATA_SCORED_DIR, "tagged_speeches.csv"))

    results = {}

    if scored:
        merged = merge_score_and_tags(scored, tagged)
        analysis = analyze_committee(merged, "厚生労働委員会")
        print_analysis(analysis)
        results["厚生労働委員会"] = analysis

    # 総務委データ
    scored_soumu = load_csv(os.path.join(DATA_SCORED_DIR, "scored_speeches_総務.csv"))
    tagged_soumu = load_csv(os.path.join(DATA_SCORED_DIR, "tagged_speeches_総務.csv"))

    if scored_soumu:
        merged_soumu = merge_score_and_tags(scored_soumu, tagged_soumu)
        analysis_soumu = analyze_committee(merged_soumu, "総務委員会")
        print_analysis(analysis_soumu)
        results["総務委員会"] = analysis_soumu

    # JSON出力
    analysis_path = os.path.join(OUTPUT_DIR, "analysis_results.json")
    with open(analysis_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nSaved analysis to {analysis_path}")

    return results


if __name__ == "__main__":
    main()
