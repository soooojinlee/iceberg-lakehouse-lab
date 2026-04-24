"""
광고 플랫폼 Lakehouse 실전 설계 - 실습 데이터 생성기

Criteo Attribution Dataset과 유사한 구조의 광고 이벤트 데이터를 생성합니다.
실제 Criteo 데이터셋은 수십 GB이므로, 교육용으로 현실적인 분포를 가진
샘플 데이터를 직접 생성합니다.

1행 = 1 impression (1회차 슬라이드 참고)

사용법:
    python generate_sample_data.py                    # 기본 10만건
    python generate_sample_data.py --events 500000    # 50만건
    python generate_sample_data.py --events 1000 --output ./test  # 소량 테스트
"""

import csv
import os
import random
import argparse
from datetime import datetime, timedelta


# ─── 캠페인 프로필 생성 ─────────────────────────────────
def create_campaign_profiles(num_campaigns, rng):
    """
    캠페인별 성과 프로필을 생성합니다.
    실제 광고 플랫폼에서는 캠페인마다 CTR/CVR이 크게 다릅니다.

    - 고성과 캠페인 (상위 20%): CTR 5-8%, CVR 3-5%
    - 중간 캠페인 (60%): CTR 2-5%, CVR 1-3%
    - 저성과 캠페인 (하위 20%): CTR 0.5-2%, CVR 0.1-1%
    """
    profiles = {}
    for c in range(1, num_campaigns + 1):
        tier = rng.random()
        if tier < 0.2:  # 저성과
            ctr = rng.uniform(0.005, 0.02)
            cvr = rng.uniform(0.001, 0.01)
            cpc = round(rng.uniform(0.05, 0.5), 2)
        elif tier < 0.8:  # 중간
            ctr = rng.uniform(0.02, 0.05)
            cvr = rng.uniform(0.01, 0.03)
            cpc = round(rng.uniform(0.3, 2.0), 2)
        else:  # 고성과
            ctr = rng.uniform(0.05, 0.08)
            cvr = rng.uniform(0.03, 0.05)
            cpc = round(rng.uniform(1.0, 5.0), 2)

        profiles[c] = {
            "base_ctr": ctr,
            "base_cvr": cvr,  # 클릭 대비 전환율
            "cpc": cpc,
            "max_delay_hours": rng.randint(1, 72),  # 전환 지연 최대값
        }
    return profiles


# ─── 시간대별 트래픽 가중치 ──────────────────────────────
def hour_weight(hour):
    """
    시간대별 트래픽 가중치.
    실제 광고 트래픽은 낮 시간대에 집중됩니다.

    새벽 2-6시: 낮음 (0.3)
    오전 7-9시: 증가 (0.7)
    오전 10시-오후 6시: 피크 (1.0)
    저녁 7-10시: 높음 (0.9)
    밤 11시-1시: 감소 (0.5)
    """
    weights = {
        0: 0.5, 1: 0.4, 2: 0.3, 3: 0.3, 4: 0.3, 5: 0.3,
        6: 0.5, 7: 0.7, 8: 0.8, 9: 0.9, 10: 1.0, 11: 1.0,
        12: 1.0, 13: 1.0, 14: 1.0, 15: 1.0, 16: 1.0, 17: 1.0,
        18: 0.9, 19: 0.9, 20: 0.9, 21: 0.8, 22: 0.7, 23: 0.6,
    }
    return weights.get(hour, 0.5)


# ─── 메인 데이터 생성 ────────────────────────────────────
def generate_ad_events(
    num_events=100_000,
    num_users=10_000,
    num_campaigns=50,
    start_date="2026-01-01",
    days=7,
    output_dir="./data",
    seed=42,
):
    rng = random.Random(seed)
    os.makedirs(output_dir, exist_ok=True)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    start_ts = int(start_dt.timestamp())
    total_seconds = days * 86400

    campaigns = create_campaign_profiles(num_campaigns, rng)
    users = [f"user_{i:06d}" for i in range(num_users)]

    # ── 시간대 가중치 기반 타임스탬프 생성 ──
    timestamps = []
    while len(timestamps) < num_events:
        ts = start_ts + rng.randint(0, total_seconds)
        hour = datetime.fromtimestamp(ts).hour
        if rng.random() < hour_weight(hour):
            timestamps.append(ts)
    timestamps.sort()

    # ── 이벤트 생성 ──
    events = []
    for ts in timestamps:
        uid = rng.choice(users)
        campaign_id = rng.randint(1, num_campaigns)
        profile = campaigns[campaign_id]

        click = 1 if rng.random() < profile["base_ctr"] else 0
        conversion = 0
        conversion_ts = ""
        cost = 0.0

        if click:
            cost = profile["cpc"]
            if rng.random() < profile["base_cvr"]:
                conversion = 1
                # 전환 지연: 최소 1분 ~ 캠페인별 최대 시간
                delay_sec = rng.randint(60, profile["max_delay_hours"] * 3600)
                conversion_ts = ts + delay_sec

        events.append({
            "timestamp": ts,
            "uid": uid,
            "campaign": campaign_id,
            "click": click,
            "conversion": conversion,
            "conversion_timestamp": conversion_ts,
            "cost": round(cost, 4),
        })

    # ── CSV 출력 ──
    fieldnames = [
        "timestamp", "uid", "campaign", "click",
        "conversion", "conversion_timestamp", "cost",
    ]

    # 전체 데이터
    full_path = os.path.join(output_dir, "ad_events.csv")
    with open(full_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(events)

    # 소량 샘플 (빠른 테스트용, 1000건)
    sample_path = os.path.join(output_dir, "ad_events_sample.csv")
    with open(sample_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(events[:1000])

    # Batch 2 (2번째 append 실습용, 뒷부분 20%)
    batch2_start = int(len(events) * 0.8)
    batch2_path = os.path.join(output_dir, "ad_events_batch2.csv")
    with open(batch2_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(events[batch2_start:])

    # ── 통계 출력 ──
    total = len(events)
    clicks = sum(1 for e in events if e["click"])
    conversions = sum(1 for e in events if e["conversion"])
    total_cost = sum(e["cost"] for e in events)

    conv_delays = [
        e["conversion_timestamp"] - e["timestamp"]
        for e in events
        if e["conversion"] and e["conversion_timestamp"]
    ]
    avg_delay_hr = (sum(conv_delays) / len(conv_delays) / 3600) if conv_delays else 0

    end_dt = start_dt + timedelta(days=days)

    print("=" * 50)
    print("  실습 데이터 생성 완료")
    print("=" * 50)
    print(f"  기간:        {start_date} ~ {end_dt.strftime('%Y-%m-%d')} ({days}일)")
    print(f"  총 이벤트:   {total:>10,}건")
    print(f"  클릭:        {clicks:>10,}건  (CTR {clicks / total * 100:.2f}%)")
    print(f"  전환:        {conversions:>10,}건  (CVR {conversions / clicks * 100:.2f}% of clicks)")
    print(f"  총 비용:     ${total_cost:>10,.2f}")
    print(f"  평균 CPA:    ${total_cost / conversions:>10,.2f}" if conversions else "")
    print(f"  평균 전환지연: {avg_delay_hr:.1f}시간")
    print(f"  캠페인 수:   {num_campaigns}")
    print(f"  유저 수:     {num_users:,}")
    print("-" * 50)
    print(f"  {full_path:<40} ({os.path.getsize(full_path) / 1024 / 1024:.1f} MB)")
    print(f"  {sample_path:<40} (1,000건 샘플)")
    print(f"  {batch2_path:<40} (2nd append용)")
    print("=" * 50)


# ─── CLI ─────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="광고 이벤트 실습 데이터 생성기"
    )
    parser.add_argument("--events", type=int, default=100_000, help="생성할 이벤트 수 (기본: 100,000)")
    parser.add_argument("--users", type=int, default=10_000, help="유저 수 (기본: 10,000)")
    parser.add_argument("--campaigns", type=int, default=50, help="캠페인 수 (기본: 50)")
    parser.add_argument("--start-date", default="2026-01-01", help="시작일 (기본: 2026-01-01)")
    parser.add_argument("--days", type=int, default=7, help="기간 일수 (기본: 7)")
    parser.add_argument("--output", default="./data", help="출력 디렉토리 (기본: ./data)")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드 (기본: 42)")

    args = parser.parse_args()
    generate_ad_events(
        num_events=args.events,
        num_users=args.users,
        num_campaigns=args.campaigns,
        start_date=args.start_date,
        days=args.days,
        output_dir=args.output,
        seed=args.seed,
    )
