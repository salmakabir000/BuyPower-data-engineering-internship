# Week 11 - Data Lake File Organizer

## What this does
`lake_organize.py` takes raw NYC Yellow Taxi Parquet files and organizes them
into a Hive-style partitioned data lake, partitioned by pickup date:

data_lake/trips/year=YYYY/month=MM/day=DD/part-NNN.parquet

It has two commands:
- `ingest <file> --dataset trips` - reads a raw parquet file, splits it by
  pickup date, and writes it into the partitioned layout. Large partitions
  are automatically split into multiple part files (~96-128MB target).
- `compact <dataset> --year Y --month M` - merges multiple part files inside
  a day partition into fewer, larger files.

## Why year/month/day partitioning (not pickup_zone)
Queries in this dataset are almost always time-based ("give me last week's
trips", "give me January's trips"). Partitioning by date means a query for
one day only has to read one folder, instead of scanning the whole dataset.
Zone-based partitioning would help for "all trips in zone X" queries, but
those are less common here, and zones have very uneven trip volume (some
zones have almost no trips), which would create a lot of tiny, unevenly
sized files.

## How late-arriving data is handled
If a file for a period we've already ingested arrives again (e.g. corrected
or late trip records), we do NOT overwrite or deduplicate. We simply write
a new part file (part-002, part-003, etc.) into that day's partition. This
guarantees no data is ever silently lost. The tradeoff is that a partition
can end up with several small part files over time — that's what the
`compact` command is for. This was tested by re-ingesting the January file
a second time: it created part-002 files alongside the existing part-001
files without touching them, and running compact merged them back down.

## What would change at 1000x scale
- **Small files problem gets much worse.** At 1000x the data, day-level
  partitions would need far more automatic splitting, and without frequent
  compaction we'd end up with thousands of tiny files, which hurts read
  performance (more file-open overhead than actual data reading).
- **Partitioning granularity would need to change.** Day-level partitions
  might become too large individually. We'd likely need to add hour-level
  partitioning, or a secondary partition key like pickup_zone, so no single
  partition holds an unmanageable amount of data.
- **Compaction can't be manual anymore.** Right now we run `compact` by
  hand. At real scale, this needs to run on a schedule (e.g. a nightly job)
  so partitions never build up too many small files between compactions.
- **Metadata tracking becomes necessary.** At this scale we'd want to track
  which files exist in each partition and their sizes, instead of relying
  on scanning the filesystem, so compaction and query planning are fast
