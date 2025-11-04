# 🌍 Country-Data-ETL-Pipeline

A Python-based ETL (Extract, Transform, Load) project that retrieves global country data from the REST Countries API, processes it using pandas, and stores it in an SQLite database.
The project performs data cleaning, enrichment, and analysis to generate visual insights such as population distribution and currency usage.

## 🚀 Features
### 🔍 Extraction

- Fetches live country data (name, capital, population, region, currency, flags, etc.) from the REST Countries API.

- Logs all extraction activities with error handling for failed requests.

### 🔄 Transformation

- Normalizes nested JSON data using pandas.

- Cleans missing values and standardizes fields like region and currency.

- Calculates new fields such as population density and world population share.

### 💾 Loading

- Loads processed data into an SQLite database with two relational tables:

- countries → country-level information

- currencies → currency details linked by country code

### 📊 Analysis & Visualization

- Identifies Top 10 most populated countries and Top 5 most common currencies.

- Displays population share by region using pie and bar charts.

- Validates results using direct SQL queries on the database.

### 🧰 Tech Stack

- Language: Python

- Libraries: pandas, matplotlib, requests, sqlite3, logging

- Database: SQLite

- Data Source: [REST Countries API](https://restcountries.com/)


## ⚙️ How to Run

### Clone the repository and navigate to the project folder.

### Install dependencies:

` pip install pandas matplotlib requests` 


### Run the ETL pipeline:

`python main.py`


## Outputs generated:

- Database: data.db
- Log file: etl_pipeline.log

## Charts:

- top10_population_share.png
- population_share_by_region.png
- top5_currencies.png
