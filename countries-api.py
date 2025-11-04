#!/usr/bin/env python3
import pandas as pd
import sqlite3
import os
from pandas import json_normalize
import matplotlib.pyplot as plt
import requests
import logging

Country_URL = "https://restcountries.com/v3.1/all"
Sqlite_DB = "data.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()]
)

def extract():
    fields = "name,capital,region,subregion,population,area,currencies,cca3,flags"
    try:
        logging.info("Started data extraction from the API...")
        response = requests.get(f"{Country_URL}?fields={fields}", timeout=10)
        response.raise_for_status()
        data = response.json() 
        logging.info(f"Successfully fetched {len(data)} records.")
        return data
        
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return []
    
def transform(recd_data):
    logging.info("Starting data transformation...")
    df = json_normalize(recd_data, sep="_" )
    df['capital'] = df['capital'].apply(lambda x: x[0] if isinstance(x, list) and x else None)
    
    columns_to_keep = [
        'name_common', 'name_official',
        'capital', 'region', 'subregion',
        'population', 'area', 'cca3',
        'flags_png', 'flags_svg', 'flags_alt'
    ]
    df = df[columns_to_keep]
    
    # Extracting currency separately as the structure is complex
    def extract_currency(curr):
        if isinstance(curr, dict) and len(curr) > 0:
            code = list(curr.keys())[0]
            details = curr[code]
            return pd.Series({
                'currency_code': code,
                'currency_name': details.get('name'),
                'currency_symbol': details.get('symbol')
            })
        else:
            return pd.Series({
                'currency_code': None,
                'currency_name': None,
                'currency_symbol': None
            })
    
    # building currency df from the original data
    curr_list = [c['currencies'] for c in recd_data]
    currency_df = pd.DataFrame([extract_currency(c) for c in curr_list])
    
    df = pd.concat([df, currency_df], axis=1)

    # Handling the missing values
    df[['currency_code', 'currency_name', 'currency_symbol']] = df[['currency_code', 'currency_name', 'currency_symbol']].fillna('Unknown')
    df['capital'] = df['capital'].fillna('Unknown')
    df.loc[df['region'] == 'Antarctic', 'subregion'] = 'Antarctica'
    
    # Calculating population density
    df['population_density'] = df['population'] / df['area']

    # Calculating percentage of world population in the country
    world_pop = df['population'].sum()
    df['population_share'] = (df['population'] / world_pop * 100).round(4)
    logging.info("Data transformation complete.")

    return df

def load(df):
    try:
        logging.info("Loading data into an SQLite database...")
        conn = sqlite3.connect(Sqlite_DB)
        
        # Creating countries table
        countries_df = df[[
                'cca3','name_common','name_official','capital','region','subregion',
                'population','area','population_density','population_share',
                'flags_png','flags_svg','flags_alt'
            ]].drop_duplicates(subset=['cca3'])
        
        countries_df.to_sql(
            'countries', conn,
            if_exists='replace',
            index=False
        )
        
        # Creating currencies table
        curr_df = df[['currency_code','currency_name','currency_symbol','cca3']].copy()
        curr_df.rename(columns={'cca3':'country_cca3'}, inplace=True)
        curr_df.to_sql(
            'currencies', conn,
            if_exists='replace',
            index=False
        )

    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")

    finally:
        conn.close()
        logging.info("Database connection closed.")

def analyze(df):
    conn = sqlite3.connect(Sqlite_DB)
    logging.info("Starting analysis and visualization...")
    
    print("\n-- Top 10 most populated countries --\n")
    top10_pop = df[['name_common', 'population', 'population_share']].sort_values(by='population', ascending=False).head(10)
    print(top10_pop) 

    # Validating the above output using SQL query
    print("\n-- Top 10 most populated countries (SQL Validation) --\n")
    query = """
        SELECT name_common, population, population_share
        FROM countries
        ORDER BY population DESC
        LIMIT 10;
    """
    sql_top10 = pd.read_sql_query(query, conn)
    print(sql_top10)
    
    print("\n-- Top 5 most common currencies --\n")
    top_currencies = df['currency_name'].value_counts().head(5)
    print(top_currencies,'\n')

    # Validating the above output using SQL query
    print("\n-- Top 5 most common currencies (SQL Validation) --\n")
    query = """
        SELECT currency_name, COUNT(*) as num_countries
        FROM currencies
        GROUP BY currency_name
        ORDER BY num_countries DESC
        LIMIT 5;
    """
    sql_top_currencies = pd.read_sql_query(query, conn)
    print(sql_top_currencies)
    
    # Visualization 1
    plt.figure(figsize=(12, 6))
    plt.barh(top10_pop['name_common'], top10_pop['population_share'], color='skyblue')
    plt.title("Top 10 Countries by Population Share (%)")
    plt.xlabel("Population Share (%)")
    plt.ylabel("Country")
    plt.gca().invert_yaxis() 
    plt.tight_layout()
    plt.savefig("top10_population_share.png")
    plt.close()
    logging.info("Saved chart: top10_population_share.png")
    
    # Visualization 2
    region_share = df.groupby('region')['population_share'].sum()
    plt.figure(figsize=(8, 8))
    plt.pie(region_share, labels=region_share.index, autopct='%1.1f%%')
    plt.title("World Population Share by Region")
    plt.tight_layout()
    plt.savefig("population_share_by_region.png")
    plt.close()
    logging.info("Saved chart: population_share_by_region.png")
    
    # Visualization 3
    top_currencies = df['currency_name'].value_counts().head(5)
    plt.figure(figsize=(8, 6))
    top_currencies.plot(kind='bar', color='lightgreen')
    plt.title("Top 5 Most Common Currencies")
    plt.ylabel("Number of Countries Using")
    plt.xlabel("Currency")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("top5_currencies.png")
    plt.close()

    logging.info("Saved chart: top5_currencies.png")
    conn.close()
    logging.info("Analysis complete.")

if __name__ == "__main__":
    recd_data = extract()
    if recd_data:
        df = transform(recd_data)
        load(df)
        analyze(df)
    else:
        logging.error("Pipeline stopped. No data extracted.")