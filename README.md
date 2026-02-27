# 🚀 Operational Data Analytics with Hive & MapReduce

![Hadoop](https://img.shields.io/badge/Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![Hive](https://img.shields.io/badge/Hive-FDEE21?style=for-the-badge&logo=apachehive&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)

A repeatable batch analytics workflow built on **Linux**, **Hadoop (HDFS)**, **Hive**, and **Python (MRJob)**. This project focuses on processing large-scale operational data, generating aggregated metrics, and joining results with metadata for structured reporting.

---

## 🌟 Project Highlights (Resume Mapping)

* **Scalable Batch Workflow**: Engineered a repeatable analytics pipeline on Linux and Hadoop to produce business-critical operational metrics.
* **Data Warehousing**: Designed and managed Hive external tables and executed complex HiveQL queries for data aggregation.
* **Distributed Computing**: Implemented MapReduce jobs using the **Python MRJob** framework to compute large-scale data joins and metadata enrichment.

---

## 🛠 Tech Stack

| Category | Tools |
| :--- | :--- |
| **Distributed Storage** | Hadoop HDFS |
| **Data Warehousing** | Apache Hive, HiveQL |
| **Compute Framework** | MapReduce, Python (MRJob) |
| **Environment** | Linux Shell / Bash |

---

## 📂 Repository Structure

```bash
.
├── src/
│   ├── hive/            # 🍯 Hive external table schemas + aggregation queries
│   └── mapreduce/       # 🐍 Python MRJob scripts + metadata join logic
├── data/
│   ├── raw/             # 📁 MovieLens raw data (u.data, u.item, u.user)
│   └── sample/          # 🧪 Mini-datasets for rapid local validation
├── outputs/
│   ├── hive/            # 📝 Query execution logs & analytical outputs
│   └── mapreduce/       # 📊 MapReduce computation results
└── docs/                # 📖 Technical reports & Resume mapping notes
