import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

def scrape_top_films(url):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    films_data = []
    for row in rows[:25]:  # Start from 1 to skip header, limit to 50 rows
        cols = row.find_all('td')
        if cols:
            films_data.append({
                "Film": cols[1].text.strip(),
                "Year": int(cols[2].text.strip()),
                "Rotten Tomatoes' Top 100": cols[3].text.strip(),
            })
    
    return pd.DataFrame(films_data)

def save_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")

def save_to_sqlite(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data saved to {db_name}, table: {table_name}")

def main():
    url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
    db_name = 'Movies.db'
    table_name = 'Top_50'
    csv_path = 'top_50_films.csv'

    df = scrape_top_films(url)
    save_to_csv(df, csv_path)
    save_to_sqlite(df, db_name, table_name)
    mask = df['Year'] >= 2000
    print(df[mask])

if __name__ == "__main__":
    main()