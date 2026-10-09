from pathlib import Path

import pandas as pd


class Destination:
    def __init__(self, config):
        self.path = config["path"]
        self.mode = config.get("mode", "overwrite")  # overwrite | append

    def write(self, df):
        path = Path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if self.mode == "append" and path.exists():
            existing = pd.read_parquet(path)
            df = pd.concat([existing, df], ignore_index=True)
        elif self.mode not in ("overwrite", "append"):
            raise ValueError(f"parquet_file destination: unknown mode '{self.mode}'")

        df.to_parquet(path, index=False)
        return {"rows_written": len(df), "path": str(path)}
