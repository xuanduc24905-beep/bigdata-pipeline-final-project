import pandas as pd
import sys

# ──────────────────────────────────────────────────────────────
# Đọc Spotify Dataset 1921-2020 (Kaggle), map schema, tạo decade
# Schema gốc Kaggle: id, name, artists, release_date, year,
#   acousticness, danceability, duration_ms, energy, explicit,
#   instrumentalness, key, liveness, loudness, mode,
#   popularity, speechiness, tempo, valence
# ──────────────────────────────────────────────────────────────

INPUT_PATH  = "/data/spotify_tracks.csv"
OUTPUT_PATH = "/data/spotify_tracks_cleaned.csv"

print(f"Reading {INPUT_PATH} ...")
df = pd.read_csv(INPUT_PATH, low_memory=False)
print(f"  Raw rows: {len(df)}, columns: {list(df.columns)}")

# ── Column mapping ─────────────────────────────────────────────
rename = {}
if "id" in df.columns and "track_id" not in df.columns:
    rename["id"] = "track_id"
if "name" in df.columns and "track_name" not in df.columns:
    rename["name"] = "track_name"
if rename:
    df = df.rename(columns=rename)

# Drop unnamed index column if present
df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")], errors="ignore")

# ── Ensure required numeric columns ───────────────────────────
num_cols = ["popularity", "duration_ms", "danceability", "energy", "key",
            "loudness", "mode", "speechiness", "acousticness",
            "instrumentalness", "liveness", "valence", "tempo"]
for col in num_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Explicit: normalise to boolean string ─────────────────────
if "explicit" in df.columns:
    df["explicit"] = df["explicit"].astype(str).str.strip().str.title()   # "True"/"False"

# ── Year / decade ──────────────────────────────────────────────
if "year" in df.columns:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["decade"] = (df["year"] // 10 * 10).astype("Int64").astype(str) + "s"
    df.loc[df["year"].isna(), "decade"] = None
elif "release_date" in df.columns:
    df["year"] = pd.to_numeric(
        df["release_date"].astype(str).str[:4], errors="coerce"
    )
    df["decade"] = (df["year"] // 10 * 10).astype("Int64").astype(str) + "s"
else:
    print("WARNING: no 'year' or 'release_date' column found — decade will be empty")
    df["year"]   = None
    df["decade"] = None

# ── Drop rows missing essential fields ────────────────────────
df = df.dropna(subset=["track_id", "track_name"])
df = df.drop_duplicates(subset=["track_id"])

print(f"  After cleaning: {len(df)} rows")
print(f"  Decades found : {sorted(df['decade'].dropna().unique())[:10]} ...")
print(f"  Year range    : {df['year'].min():.0f} – {df['year'].max():.0f}")

# ── Save processed CSV ─────────────────────────────────────────
df.to_csv(OUTPUT_PATH, index=False)
print(f"\nSaved {len(df)} tracks → {OUTPUT_PATH}")
print("Columns:", list(df.columns))
