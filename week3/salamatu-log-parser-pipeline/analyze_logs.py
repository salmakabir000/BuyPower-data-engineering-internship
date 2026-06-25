import pandas as pd
from pathlib import Path


def main():

    input_file = "output/clean.parquet"

    output_dir = Path("output/analysis")

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    df = pd.read_parquet(input_file)

    # Top 10 IPs

    top_ips = (
        df.groupby("ip")
        .size()
        .reset_index(name="request_count")
        .sort_values(
            "request_count",
            ascending=False
        )
        .head(10)
    )

    print("\nTop 10 IPs\n")
    print(top_ips)

    top_ips.to_parquet(
        output_dir / "top_ips.parquet",
        index=False
    )

    # Top 10 Paths

    top_paths = (
        df.groupby("path")
        .size()
        .reset_index(name="request_count")
        .sort_values(
            "request_count",
            ascending=False
        )
        .head(10)
    )

    print("\nTop 10 Paths\n")
    print(top_paths)

    top_paths.to_parquet(
        output_dir / "top_paths.parquet",
        index=False
    )

    # Hourly Request Volume

    hourly_volume = (
        df.assign(
            hour=df["timestamp"].dt.floor("h")
        )
        .groupby("hour")
        .size()
        .reset_index(name="requests")
    )

    print("\nHourly Request Volume\n")
    print(hourly_volume.head(20))

    hourly_volume.to_parquet(
        output_dir / "hourly_volume.parquet",
        index=False
    )

    print("\nAnalysis Complete!")

    print(
        "\nFiles created:"
        "\n- top_ips.parquet"
        "\n- top_paths.parquet"
        "\n- hourly_volume.parquet"
    )


if __name__ == "__main__":
    main()
