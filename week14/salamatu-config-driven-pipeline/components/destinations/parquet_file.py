from pathlib import Path


def write(df, config):
    path = Path(config["path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Saved {len(df)} rows to {path}")
