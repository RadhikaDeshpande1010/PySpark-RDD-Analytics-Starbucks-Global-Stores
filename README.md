<div align="center">

<img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/PySpark-RDD%20API-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/Google%20Colab-Ready-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white"/>

# ☕ PySpark RDD Analytics — Starbucks Global Stores Dataset

**Domain:** Retail Store Analytics &nbsp;|&nbsp; **Engine:** Apache Spark (RDD API) &nbsp;|&nbsp; **Language:** Python 3

</div>

---

## 📌 Overview

This notebook applies the **Apache Spark RDD API** to the Starbucks Global Store Locations dataset, covering **12 analytical exercises** across core Spark primitives — `map`, `filter`, `reduceByKey`, `sortBy`, `distinct`, `count`, and `take`. The dataset contains **25,600 store records** spanning **73 countries**, and all transformations are built using **low-level RDD operations** (no DataFrames or SQL) to develop a strong foundational understanding of Spark internals.

---

## 🗂️ Repository Structure

```
pyspark-rdd-starbucks-analytics/
│
├── notebook/
│   └── starbucks_rdd_analysis.ipynb       # Main notebook (12 RDD exercises)
├── data/
│   └── starbucks_data.csv                 # Source dataset (25,600 global stores)
└── README.md
```

---

## 📋 Dataset Schema

The dataset is a CSV file with a header row. Each subsequent row represents one Starbucks store location worldwide.

| Field | Index | Type | Description |
|---|---|---|---|
| `Brand` | 0 | str | Always `Starbucks` |
| `Store Number` | 1 | str | Unique store identifier |
| `Store Name` | 2 | str | Name of the store |
| `Ownership Type` | 3 | str | `Company Owned` / `Licensed` / `Joint Venture` / `Franchise` |
| `Street Address` | 4 | str | Full street address |
| `City` | 5 | str | City name |
| `State/Province` | 6 | str | State or province code |
| `Country` | 7 | str | ISO 2-letter country code |
| `Postcode` | 8 | str | Postal / ZIP code |
| `Phone Number` | 9 | str | Store phone number |
| `Timezone` | 10 | str | IANA timezone string |
| `Longitude` | 11 | float | Longitude coordinate |
| `Latitude` | 12 | float | Latitude coordinate |

**Sample record:**
```
Starbucks,47370-257954,"Meritxell, 96",Licensed,"Av. Meritxell, 96",Andorra la Vella,7,AD,AD500,376818720,GMT+1:00 Europe/Andorra,1.53,42.51
```

---

## 📊 Dataset Snapshot

| Metric | Value |
|---|---|
| Total stores | 25,600 |
| Countries covered | 73 |
| Company Owned | 11,932 |
| Licensed | 9,375 |
| Joint Venture | 3,976 |
| Franchise | 317 |
| Top country (US) | 13,608 stores |

---

## 📂 Exercises Overview

| Q# | Question | Column Used | Method |
|---|---|---|---|
| Q1 | Number of Starbucks shops per country | `Country` (7) | `map → reduceByKey → sortBy desc` |
| Q2 | Total number of Licensed shops worldwide | `Ownership Type` (3) | `filter → count` |
| Q3 | How many shops are in the Middle East region? | `Country` (7) | `filter(in list) → count` |
| Q4 | How many shops are in the city of Wien? | `City` (5) | `filter → count` |
| Q5 | Print all unique postcodes in the dataset | `Postcode` (8) | `map → filter → distinct → sortBy` |
| Q6 | How many shops are on Airport Road? | `Street Address` (4) | `filter(contains) → map → count` |
| Q7 | Which country has the highest number of stores? | `Country` (7) | `map → reduceByKey → sortBy → first` |
| Q8 | How many shops are Company Owned worldwide? | `Ownership Type` (3) | `filter → count` |
| Q9 | List all unique ownership types | `Ownership Type` (3) | `map → distinct → sortBy` |
| Q10 | How many stores are available in the US? | `Country` (7) | `filter(== 'US') → count` |
| Q11 | Top 5 countries with the most stores | `Country` (7) | `map → reduceByKey → sortBy → take(5)` |
| Q12 | How many stores operate in each timezone? | `Timezone` (10) | `map → reduceByKey → sortBy desc` |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.x
- PySpark (`pip install pyspark`)
- Google Colab (recommended) or a local Spark environment

### Installation

```bash
pip install pyspark
```

### Running in Google Colab

```python
!pip install pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Starbucks_RDD_Analysis") \
    .getOrCreate()

sc = spark.sparkContext
```

Upload `starbucks_data.csv` to `/content/sample_data/` in your Colab session, then run all cells sequentially.

---

## 💡 Key Concepts Demonstrated

**Loading and parsing a CSV with quoted fields using `csv.reader`**
```python
import csv

raw_rdd = sc.textFile("/content/sample_data/starbucks_data.csv")
header  = raw_rdd.first()

data_rdd = raw_rdd \
    .filter(lambda row: row != header) \
    .map(lambda row: next(csv.reader([row])))
```

**Store count per country — sorted descending (Q1)**
```python
shops_count_per_country_rdd = data_rdd \
    .filter(lambda col: len(col) > 7) \
    .map(lambda col: (col[7], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)
```

**Filter by region using a country code list (Q3)**
```python
middle_east_country_codes = ['AE', 'SA', 'QA', 'KW', 'BH', 'OM', 'JO', 'LB']

middle_east_shops_count = data_rdd \
    .filter(lambda col: len(col) > 7) \
    .filter(lambda col: col[7] in middle_east_country_codes) \
    .count()
```

**Substring search on address field (Q6)**
```python
airport_road_shops_rdd = data_rdd \
    .filter(lambda col: len(col) > 7) \
    .filter(lambda col: 'Airport Road' in col[4]) \
    .map(lambda col: (col[1], col[2], col[4], col[5], col[7]))
```

**Top 5 countries by store count (Q11)**
```python
top_5_countries_by_store_count = data_rdd \
    .filter(lambda col: len(col) > 7) \
    .map(lambda col: (col[7], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(5)
```

**Store count per timezone (Q12)**
```python
stores_per_timezone_rdd = data_rdd \
    .filter(lambda col: len(col) > 10) \
    .filter(lambda col: col[10].strip() != '') \
    .map(lambda col: (col[10].strip(), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)
```

---

## 🛠️ Technologies Used

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-RDD%20API-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google%20Colab-F9AB00?style=flat-square&logo=googlecolab&logoColor=white)

---

<div align="center">

*Built by **Radhika Deshpande** · PySpark RDD exercises on the Starbucks Global Store Locations dataset*

</div>
