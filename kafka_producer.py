"""
광고 이벤트 스트리밍 시뮬레이터 (Kafka Producer).

배치 CSV 데이터를 실시간 스트림처럼 Kafka로 재생한다.

== 기본 모드 ==
  1개 토픽: ad-events
  Criteo 결합 행을 단일 이벤트로 발행한다.
  3회차 강의의 기본 ingest 실습 경로와 맞춘 모드다.

== Realistic 모드 (--realistic) ==
  3개 토픽으로 실제 운영 환경을 시뮬레이션:
    ad-impressions  <- 노출 이벤트 (즉시)
    ad-clicks       <- 클릭 이벤트 (수 초 후)
    ad-conversions  <- 전환 이벤트 (수 시간 후, 축소 적용)

  Criteo는 impression 중심 결합 데이터이므로, click은 합성 지연,
  conversion은 conversion_timestamp를 이용해 파생한다.
"""

import csv
import json
import random
import time
import threading
import argparse
from datetime import datetime
from queue import Empty, PriorityQueue


class AdEventStreamer:
    """
    CSV 광고 이벤트 데이터를 Kafka로 스트리밍하는 시뮬레이터.

    Simple 모드: 1토픽 (ad-events)
    Realistic 모드: 3토픽 (ad-impressions, ad-clicks, ad-conversions)
    """

    def __init__(
        self,
        bootstrap_servers="localhost:9092",
        speed_multiplier=100,
        conversion_delay_scale=0.01,
        realistic=False,
    ):
        try:
            from kafka import KafkaProducer
        except ImportError:
            print("=" * 50)
            print("  kafka-python-ng 패키지가 필요합니다")
            print("  pip install kafka-python-ng")
            print("=" * 50)
            raise SystemExit(1)

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        self.speed_multiplier = speed_multiplier
        self.conversion_delay_scale = conversion_delay_scale
        self.realistic = realistic
        self.delayed_queue = PriorityQueue()
        self.stats = {
            "impressions": 0,
            "clicks": 0,
            "conversions": 0,
            "clicks_sent": 0,
            "conversions_sent": 0,
            "delayed_pending": 0,
        }
        self._stop = False
        self._rng = random.Random(42)
        self._sequence = 0

    def _delayed_event_worker(self):
        """
        별도 스레드: 지연된 이벤트(click, conversion)를 예약 시간에 맞춰 발행.

        PriorityQueue에서 (send_at, topic, event)를 꺼내고,
        send_at 시간이 되면 해당 토픽으로 발행합니다.
        """
        while not self._stop or self.stats["delayed_pending"] > 0:
            if self.delayed_queue.empty():
                time.sleep(0.1)
                continue

            try:
                send_at, _, topic, event = self.delayed_queue.get(timeout=1)
                while True:
                    remaining = send_at - time.time()
                    if remaining <= 0:
                        break
                    time.sleep(min(remaining, 5))

                self.producer.send(topic, key=event["uid"], value=event)
                self.stats["delayed_pending"] -= 1

                if topic == "ad-clicks":
                    self.stats["clicks_sent"] += 1
                elif topic == "ad-conversions":
                    self.stats["conversions_sent"] += 1

                total_delayed = self.stats["clicks_sent"] + self.stats["conversions_sent"]
                if total_delayed % 50 == 0:
                    print(
                        f"  [delayed] click: {self.stats['clicks_sent']}  "
                        f"conv: {self.stats['conversions_sent']}  "
                        f"대기: {self.stats['delayed_pending']}"
                    )

            except Empty:
                continue
            except Exception as exc:
                if not self._stop:
                    print(f"  [error] 지연 발행 오류: {exc}")

    def stream_from_csv(self, csv_path, max_events=None):
        """CSV 파일을 읽어 Kafka로 스트리밍"""

        worker = None
        if self.realistic:
            worker = threading.Thread(target=self._delayed_event_worker, daemon=True)
            worker.start()

        mode_label = "Realistic (3토픽)" if self.realistic else "Basic (단일 토픽)"
        print("=" * 60)
        print(f"  광고 이벤트 스트리밍 시뮬레이터 [{mode_label}]")
        print("=" * 60)
        print(f"  파일:        {csv_path}")
        print(f"  재생 속도:   {self.speed_multiplier}x")
        if self.realistic:
            print(f"  전환 지연:   {self.conversion_delay_scale}x 축소")
            print(f"  토픽:")
            print(f"    ad-impressions  <- 노출 (즉시)")
            print(f"    ad-clicks       <- 클릭 (수 초 후)")
            print(f"    ad-conversions  <- 전환 (지연 발행)")
        else:
            print(f"  토픽:")
            print(f"    ad-events       <- 결합 이벤트 (즉시)")
        if max_events:
            print(f"  최대 이벤트: {max_events:,}건")
        print("=" * 60)
        print()

        prev_ts = None
        start_time = time.time()

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader):
                if max_events and i >= max_events:
                    break

                ts = int(row["timestamp"])
                event_id = f"evt_{i:08d}"

                # ── 이벤트 간 시간 간격 재현 ──
                if prev_ts is not None and ts > prev_ts:
                    real_delay = (ts - prev_ts) / self.speed_multiplier
                    if 0 < real_delay < 5:
                        time.sleep(real_delay)
                prev_ts = ts

                if self.realistic:
                    self._process_realistic(row, ts, event_id)
                else:
                    self._process_simple(row, ts, event_id)

                # ── 진행 상황 ──
                if (i + 1) % 1000 == 0:
                    elapsed = time.time() - start_time
                    pending = self.stats["delayed_pending"]
                    print(
                        f"  [{elapsed:6.1f}s] {i + 1:>8,}건 | "
                        f"imp: {self.stats['impressions']:,}  "
                        f"click: {self.stats['clicks']:,}  "
                        f"대기: {pending}"
                    )

        self.producer.flush()

        if worker is not None:
            self._stop = True

            if self.stats["delayed_pending"] > 0:
                remaining = self.stats["delayed_pending"]
                print(f"\n  메인 이벤트 완료. 지연 이벤트 {remaining}건 발행 대기...")
                while self.stats["delayed_pending"] > 0:
                    print(f"    남은: {self.stats['delayed_pending']}건", end="\r")
                    time.sleep(1)

            worker.join()
            self.producer.flush()

        # ── 최종 결과 ──
        elapsed = time.time() - start_time
        print()
        print("=" * 60)
        print(f"  스트리밍 완료 ({elapsed:.1f}초)")
        print("=" * 60)
        if self.realistic:
            print(f"  Impression:  {self.stats['impressions']:>10,}건 -> ad-impressions")
            print(f"  Click:       {self.stats['clicks_sent']:>10,}건 -> ad-clicks")
            print(f"  Conversion:  {self.stats['conversions_sent']:>10,}건 -> ad-conversions")
        else:
            print(f"  Events:      {self.stats['impressions']:>10,}건 -> ad-events")
            print(f"  Click rows:  {self.stats['clicks']:>10,}건")
            print(f"  Conv rows:   {self.stats['conversions']:>10,}건")
        print("=" * 60)

    def _process_simple(self, row, ts, event_id):
        """기본 모드: 결합 행을 ad-events 단일 토픽으로 발행."""
        event = {
            "event_id": event_id,
            "event_type": "ad_event",
            "timestamp": ts,
            "event_time": datetime.fromtimestamp(ts).isoformat(),
            "uid": row["uid"],
            "campaign": int(row["campaign"]),
            "click": int(row["click"]),
            "conversion": int(row.get("conversion", 0)),
            "conversion_timestamp": (
                int(row["conversion_timestamp"]) if row.get("conversion_timestamp") else None
            ),
            "cost": float(row["cost"]) if row["cost"] else 0.0,
        }
        self.producer.send("ad-events", key=row["uid"], value=event)
        self.stats["impressions"] += 1

        if int(row["click"]):
            self.stats["clicks"] += 1

        if int(row.get("conversion", 0)):
            self.stats["conversions"] += 1

    def _process_realistic(self, row, ts, event_id):
        """
        Realistic 모드: 3토픽 분리.

        실제 광고 플랫폼처럼:
        - ad-impressions: 노출 즉시 (click/conversion 정보 없음)
        - ad-clicks: 클릭 수 초 후 (event_id로 impression과 연결)
        - ad-conversions: 전환 수 시간~일 후 (event_id로 연결)
        """
        # 1) Impression → 즉시 발행 (click/conversion 정보 없음!)
        impression = {
            "event_id": event_id,
            "event_type": "impression",
            "timestamp": ts,
            "event_time": datetime.fromtimestamp(ts).isoformat(),
            "uid": row["uid"],
            "campaign": int(row["campaign"]),
            "cost": float(row["cost"]) if row["cost"] else 0.0,
        }
        self.producer.send("ad-impressions", key=row["uid"], value=impression)
        self.stats["impressions"] += 1

        # 2) Click → 소량 지연 후 발행 (1~30초, 축소 적용)
        if int(row["click"]):
            self.stats["clicks"] += 1
            click_delay_real = self._rng.randint(1, 30)  # 1~30초
            click_delay_scaled = click_delay_real / self.speed_multiplier

            click_event = {
                "event_id": event_id,
                "event_type": "click",
                "timestamp": ts + click_delay_real,
                "event_time": datetime.fromtimestamp(ts + click_delay_real).isoformat(),
                "uid": row["uid"],
                "campaign": int(row["campaign"]),
            }

            if click_delay_scaled < 0.1:
                # 지연이 너무 작으면 즉시 발행
                self.producer.send("ad-clicks", key=row["uid"], value=click_event)
                self.stats["clicks_sent"] += 1
            else:
                send_at = time.time() + click_delay_scaled
                self._sequence += 1
                self.stats["delayed_pending"] += 1
                self.delayed_queue.put(
                    (send_at, self._sequence, "ad-clicks", click_event)
                )

        # 3) Conversion → 대량 지연 후 발행
        if int(row.get("conversion", 0)) and row.get("conversion_timestamp"):
            conv_ts_val = row["conversion_timestamp"]
            if conv_ts_val:
                conv_ts = int(conv_ts_val)
                original_delay = conv_ts - ts
                self.stats["conversions"] += 1

                conversion_event = {
                    "event_id": event_id,
                    "event_type": "conversion",
                    "timestamp": conv_ts,
                    "event_time": datetime.fromtimestamp(conv_ts).isoformat(),
                    "uid": row["uid"],
                    "campaign": int(row["campaign"]),
                    "conversion_delay_sec": original_delay,
                }

                if self.conversion_delay_scale == 0:
                    self.producer.send(
                        "ad-conversions", key=row["uid"], value=conversion_event
                    )
                    self.stats["conversions_sent"] += 1
                else:
                    scaled_delay = original_delay * self.conversion_delay_scale
                    send_at = time.time() + scaled_delay
                    self._sequence += 1
                    self.stats["delayed_pending"] += 1
                    self.delayed_queue.put(
                        (send_at, self._sequence, "ad-conversions", conversion_event)
                    )


def create_topics(bootstrap_servers, realistic=False):
    """Kafka 토픽 사전 생성"""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic

        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

        if realistic:
            topics = [
                NewTopic(name="ad-impressions", num_partitions=3, replication_factor=1),
                NewTopic(name="ad-clicks", num_partitions=3, replication_factor=1),
                NewTopic(name="ad-conversions", num_partitions=3, replication_factor=1),
            ]
        else:
            topics = [
                NewTopic(name="ad-events", num_partitions=3, replication_factor=1),
            ]

        existing = admin.list_topics()
        new_topics = [t for t in topics if t.name not in existing]
        if new_topics:
            admin.create_topics(new_topics)
            print(f"토픽 생성: {[t.name for t in new_topics]}")
        else:
            print("토픽이 이미 존재합니다.")
        admin.close()
    except Exception as e:
        print(f"토픽 생성 건너뜀 (auto.create 활성화 시 자동 생성됨): {e}")


def main():
    parser = argparse.ArgumentParser(description="광고 이벤트 스트리밍 시뮬레이터")
    parser.add_argument(
        "--csv", default="./data/ad_events.csv",
        help="입력 CSV 파일 (기본: ./data/ad_events.csv)",
    )
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092",
        help="Kafka 브로커 (기본: localhost:9092)",
    )
    parser.add_argument(
        "--speed", type=int, default=100,
        help="재생 속도 배율 (기본: 100x)",
    )
    parser.add_argument(
        "--delay-scale", type=float, default=0.01,
        help="Realistic 모드에서 전환 지연 축소 비율 (기본: 0.01 = 1시간->36초, 0=즉시)",
    )
    parser.add_argument(
        "--max-events", type=int, default=None,
        help="최대 이벤트 수 (기본: 전체)",
    )
    parser.add_argument(
        "--realistic", action="store_true",
        help="3토픽 모드: ad-impressions/ad-clicks/ad-conversions 분리 발행",
    )
    parser.add_argument(
        "--create-topics", action="store_true",
        help="Kafka 토픽을 사전 생성",
    )

    args = parser.parse_args()

    if args.create_topics:
        create_topics(args.bootstrap_servers, realistic=args.realistic)

    streamer = AdEventStreamer(
        bootstrap_servers=args.bootstrap_servers,
        speed_multiplier=args.speed,
        conversion_delay_scale=args.delay_scale,
        realistic=args.realistic,
    )
    streamer.stream_from_csv(args.csv, max_events=args.max_events)


if __name__ == "__main__":
    main()
