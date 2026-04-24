"""
결합된 광고 데이터를 확장 실습용 3토픽 이벤트로 분리한다.

강의 기본 흐름:
    ad_events.csv -> ad-events (단일 토픽)

확장 흐름:
    ad_events.csv -> (파생)
      events_impressions.csv
      events_clicks.csv
      events_conversions.csv

Criteo는 본래 impression 중심의 결합 데이터이므로, 클릭 시각은 1~30초 지연을
합성하고 conversion은 conversion_timestamp를 그대로 사용한다.
"""

import argparse
import csv
import os
import random


def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def split(input_csv, output_dir, seed=42):
    rng = random.Random(seed)
    os.makedirs(output_dir, exist_ok=True)

    imp_path = os.path.join(output_dir, "events_impressions.csv")
    click_path = os.path.join(output_dir, "events_clicks.csv")
    conv_path = os.path.join(output_dir, "events_conversions.csv")

    imp_fields = [
        "event_id",
        "event_type",
        "timestamp",
        "uid",
        "campaign",
        "cost",
    ]
    click_fields = [
        "event_id",
        "event_type",
        "timestamp",
        "impression_timestamp",
        "uid",
        "campaign",
    ]
    conv_fields = [
        "event_id",
        "event_type",
        "timestamp",
        "impression_timestamp",
        "uid",
        "campaign",
        "conversion_delay_sec",
    ]

    clicks = []
    conversions = []
    impressions_count = 0
    conv_delays = []

    with open(input_csv, "r", newline="") as input_handle, open(
        imp_path, "w", newline=""
    ) as imp_handle:
        reader = csv.DictReader(input_handle)
        imp_writer = csv.DictWriter(imp_handle, fieldnames=imp_fields)
        imp_writer.writeheader()

        for idx, row in enumerate(reader):
            event_id = f"evt_{idx:08d}"
            ts = safe_int(row.get("timestamp"), 0)
            uid = row.get("uid", "")
            campaign = row.get("campaign", "")

            impression = {
                "event_id": event_id,
                "event_type": "impression",
                "timestamp": ts,
                "uid": uid,
                "campaign": campaign,
                "cost": row.get("cost", "0.0") or "0.0",
            }
            imp_writer.writerow(impression)
            impressions_count += 1

            if safe_int(row.get("click"), 0):
                click_delay = rng.randint(1, 30)
                clicks.append(
                    {
                        "event_id": event_id,
                        "event_type": "click",
                        "timestamp": ts + click_delay,
                        "impression_timestamp": ts,
                        "uid": uid,
                        "campaign": campaign,
                    }
                )

            if safe_int(row.get("conversion"), 0):
                conv_ts = safe_int(row.get("conversion_timestamp"), 0)
                if conv_ts > ts:
                    conversions.append(
                        {
                            "event_id": event_id,
                            "event_type": "conversion",
                            "timestamp": conv_ts,
                            "impression_timestamp": ts,
                            "uid": uid,
                            "campaign": campaign,
                            "conversion_delay_sec": conv_ts - ts,
                        }
                    )
                    conv_delays.append(conv_ts - ts)

    clicks.sort(key=lambda row: row["timestamp"])
    conversions.sort(key=lambda row: row["timestamp"])

    write_csv(click_path, click_fields, clicks)
    write_csv(conv_path, conv_fields, conversions)

    avg_delay_hr = (sum(conv_delays) / len(conv_delays) / 3600) if conv_delays else 0

    print("=" * 60)
    print("  이벤트 분리 완료")
    print("=" * 60)
    print()
    print("  주의: 이 결과는 Criteo 결합 데이터를 3토픽으로 파생한 확장 실습용입니다.")
    print("  강의 기본 경로는 단일 토픽 ad-events 입니다.")
    print()
    print(f"  Impressions:  {impressions_count:>8,}건  -> {imp_path}")
    print(f"  Clicks:       {len(clicks):>8,}건  -> {click_path}")
    print(f"  Conversions:  {len(conversions):>8,}건  -> {conv_path}")
    print()
    if impressions_count:
        print(f"  CTR: {len(clicks) / impressions_count * 100:.2f}%")
    if clicks:
        print(f"  CVR: {len(conversions) / len(clicks) * 100:.2f}% (of clicks)")
    print(f"  평균 전환 지연: {avg_delay_hr:.1f}시간")
    print()
    print("  이벤트 생성 규칙:")
    print("    1) 모든 행 -> impression")
    print("    2) click=1 -> click (impression 후 1~30초 합성 지연)")
    print("    3) conversion=1 -> conversion (conversion_timestamp 사용)")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="결합 광고 데이터 -> 3개 이벤트 스트림 분리"
    )
    parser.add_argument(
        "--input",
        default="./data/ad_events.csv",
        help="입력 CSV (기본: ./data/ad_events.csv)",
    )
    parser.add_argument(
        "--output",
        default="./data",
        help="출력 디렉토리 (기본: ./data)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="랜덤 시드 (기본: 42)",
    )
    args = parser.parse_args()
    split(args.input, args.output, seed=args.seed)
