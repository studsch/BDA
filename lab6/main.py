import re
import csv
import json
import time
import argparse
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer


def get_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def get_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def producer():
    p = get_producer()
    with open("../data/input_data.csv", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter=";")
        for row in r:
            p.send("data", row)

    p.flush()


def transformer():
    c = get_consumer("data")
    p = get_producer()

    regions = {}
    pattern = re.compile(r"Код региона (\d+) соответствует (.+)")
    with open("../data/regions.txt", "r", encoding="utf-8") as f:
        for line in f:
            m = pattern.match(line.strip())
            id = int(m.group(1))
            name = m.group(2).strip()
            regions[id] = name

    for m in c:
        record = m.value

        record["price"] = int(record.get("price") or 0)
        record["level"] = int(record.get("level") or 0)
        record["levels"] = int(record.get("levels") or 0)
        record["rooms"] = int(record.get("rooms") or 0)
        record["area"] = float(record.get("price") or 0.0)
        record["kitchen_area"] = float(record.get("kitchen_area") or 0.0)
        record["geo_lat"] = float(record.get("geo_lat") or 0.0)
        record["geo_lon"] = float(record.get("geo_lon") or 0.0)
        record["building_type"] = int(record.get("building_type") or 0)
        record["object_type"] = int(record.get("object_type") or 0)
        record["postal_code"] = int(record.get("postal_code") or 0)
        record["street_id"] = int(record.get("street_id") or 0)
        record["region_name"] = regions.get(int(record.get("id_region") or 0))
        record["house_id"] = int(record.get("house_id") or 0)

        print(record)

        p.send("transformed_data", record)
        p.flush()


def filter():
    c = get_consumer("transformed_data")
    p = get_producer()

    for m in c:
        record = m.value
        if record.get("rooms") == 1:
            p.send("one_room_apart", record)
            p.flush()


def aggregator():
    c = get_consumer("one_room_apart")
    delay = 10
    buckets = defaultdict(list)
    start_time = time.time()

    for m in c:
        record = m.value
        region = record.get("region_name")
        price = record.get("price")
        buckets[region].append(price)

        if time.time() - start_time >= delay:
            for region, prices in list(buckets.items()):
                avg_price = sum(prices) / len(prices) if prices else 0
                print(
                    f"В регионе {region} средняя цена на однокомнатную квартиру - {avg_price:.2f} рублей"
                )
                buckets.clear()
                start_time = time.time()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="lab6")

    parser.add_argument("--producer", action="store_true", help="Run producer")
    parser.add_argument(
        "--transformer", action="store_true", help="Run transformer"
    )
    parser.add_argument("--filter", action="store_true", help="Run filter")
    parser.add_argument(
        "--aggregator", action="store_true", help="Run aggregator"
    )

    args = parser.parse_args()

    if args.producer:
        producer()
    elif args.transformer:
        transformer()
    elif args.filter:
        filter()
    elif args.aggregator:
        aggregator()
    else:
        print(
            "Выберите одну из опций: --producer | --transformer | --filter | --aggregator"
        )
