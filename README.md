# Operational Data Analytics with Hive & MapReduce

A repeatable batch analytics workflow using Linux (Linux), Hadoop (Hadoop), HDFS (HDFS), Hive (Hive), and Python MRJob (MRJob) MapReduce (MapReduce).  
This project produces aggregated operational metrics and joins results with metadata for structured reporting.

## Highlights (Resume Mapping)
- Built a repeatable batch analytics workflow on Linux (Linux), Hadoop (Hadoop), and Hive (Hive) to produce aggregated operational metrics.
- Implemented Python (Python) / MRJob (MRJob) MapReduce (MapReduce) jobs to compute and join results with metadata for structured reporting.

## Tech Stack
- Hadoop (Hadoop), HDFS (HDFS)
- Hive (Hive) / HiveQL (HiveQL)
- Python (Python), MRJob (MRJob)
- Linux (Linux)

## Repository Structure
- `src/hive/` – Hive (Hive) external tables + aggregation queries
- `src/mapreduce/` – MRJob (MRJob) MapReduce (MapReduce) jobs + metadata joins
- `data/sample/` – small sample dataset for quick local runs
- `outputs/` – example outputs / logs
- `docs/` – report + resume mapping notes

## How to Run (Local Sample)
> This repo includes a small sample dataset for quick validation.

### Option A: Run MRJob locally (no Hadoop needed)
```bash
python src/mapreduce/top_movies_count.py data/sample/ratings_sample.csv > outputs/top_movies_count.txt
python src/mapreduce/join_movie_titles.py outputs/top_movies_count.txt data/sample/movies_sample.csv > outputs/top_movies_with_titles.txt
