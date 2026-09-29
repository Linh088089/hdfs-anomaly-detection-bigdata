import csv
import json
import time
import argparse

from kafka import KafkaProducer


DEFAULT_DATASET = "data/raw/Event_traces.csv"
DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "hdfs-logs"
DEFAULT_DELAY = 5.0


def create_producer(bootstrap_server):
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(
            v,
            ensure_ascii=False
        ).encode("utf-8"),
        acks="all"
    )


def replay_hdfs_logs(
    dataset_path,
    producer,
    topic,
    delay,
    start,
    limit
):
    sent_count = 0
    current_index = 0

    with open(
        dataset_path,
        "r",
        encoding="utf-8",
        newline=""
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        for row in reader:

            if current_index < start:
                current_index += 1
                continue

            message = {
                "BlockId": row["BlockId"],
                "Label": row["Label"],
                "Type": row["Type"],
                "Features": row["Features"],
                "TimeInterval": row["TimeInterval"],
                "Latency": row["Latency"]
            }

            producer.send(
                topic,
                key=row["BlockId"],
                value=message
            )

            producer.flush()

            sent_count += 1
            current_index += 1

            print("==========================================")
            print(f"BLOCK ID THU {sent_count}")
            print("==========================================")
            print("BlockId :", row["BlockId"])
            print("Label   :", row["Label"])
            print("Topic   :", topic)
            print("Chu ky  :", f"{delay} giay / Block ID")
            print("==========================================\n")

            if limit is not None and sent_count >= limit:
                break

            time.sleep(delay)

    print("\n==========================================")
    print("KAFKA PRODUCER HOAN TAT")
    print("==========================================")
    print("Topic      :", topic)
    print("Da gui     :", sent_count)
    print("Bat dau tu :", start)
    print("Dataset    :", dataset_path)
    print("==========================================")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Replay HDFS Block ID to Apache Kafka"
    )

    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET
    )

    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Chu ky gui moi Block ID, mac dinh 5 giay"
    )

    parser.add_argument(
        "--start",
        type=int,
        default=0
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None
    )

    args = parser.parse_args()

    print("==========================================")
    print("HDFS KAFKA PRODUCER - REAL TIME REPLAY")
    print("==========================================")
    print("Dataset      :", args.dataset)
    print("Kafka server :", args.bootstrap_server)
    print("Topic        :", args.topic)
    print("Chu ky       :", args.delay, "giay / Block ID")
    print("Start        :", args.start)
    print("Limit        :", args.limit)
    print("==========================================\n")

    producer = create_producer(
        args.bootstrap_server
    )

    replay_hdfs_logs(
        dataset_path=args.dataset,
        producer=producer,
        topic=args.topic,
        delay=args.delay,
        start=args.start,
        limit=args.limit
    )

    producer.close()
