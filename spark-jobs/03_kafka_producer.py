import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC  = "music-stream"
CSV_PATH     = "/data/spotify_tracks.csv"
DELAY_SECONDS = 0.01   # ~100 tracks/giây

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)
print("Kafka producer connected")


def load_tracks():
    df = pd.read_csv(CSV_PATH, low_memory=False)
    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")], errors="ignore")
    df = df.dropna(subset=["track_id", "track_name"])

    # Đảm bảo cột year / decade tồn tại
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    if "decade" not in df.columns and "year" in df.columns:
        df["decade"] = (df["year"] // 10 * 10).astype("Int64").astype(str) + "s"

    # Serialize: NaN → None để JSON encode được
    tracks = []
    for _, row in df.iterrows():
        record = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        tracks.append(record)

    print(f"Loaded {len(tracks)} tracks from {CSV_PATH}")
    return tracks


tracks  = load_tracks()
pass_num = 0

# Stream liên tục: mỗi vòng shuffle để thứ tự khác nhau
while True:
    pass_num += 1
    random.shuffle(tracks)
    print(f"\n--- Pass {pass_num}: streaming {len(tracks)} tracks ---")

    for i, track in enumerate(tracks):
        producer.send(
            topic=KAFKA_TOPIC,
            key=str(track.get("track_id", i)),
            value=track,
        )
        if (i + 1) % 500 == 0:
            producer.flush()
            print(f"  sent {i + 1}/{len(tracks)}")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"Pass {pass_num} complete. Sleeping 10s...")
    time.sleep(10)
