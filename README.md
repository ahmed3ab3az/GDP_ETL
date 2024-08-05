<p align="center">
  <img src="https://cdn-icons-png.flaticon.com/512/6295/6295417.png" width="100" />
</p>
<p align="center">
    <h1 align="center">IBM_PROJECT</h1>
</p>
<p align="center">
    <em><code>GDP</code></em>
</p>
<p align="center">
	<img src="https://img.shields.io/github/last-commit/ahmed3ab3az/IBM_Project?style=flat&logo=git&logoColor=white&color=0080ff" alt="last-commit">
	<img src="https://img.shields.io/github/languages/top/ahmed3ab3az/IBM_Project?style=flat&color=0080ff" alt="repo-top-language">
	<img src="https://img.shields.io/github/languages/count/ahmed3ab3az/IBM_Project?style=flat&color=0080ff" alt="repo-language-count">
<p>
<p align="center">
		<em>Developed with the software and tools below.</em>
</p>
<p align="center">
	<img src="https://img.shields.io/badge/Python-3776AB.svg?style=flat&logo=Python&logoColor=white" alt="Python">
	<img src="https://img.shields.io/badge/JSON-000000.svg?style=flat&logo=JSON&logoColor=white" alt="JSON">
</p>
<hr>

# Country GDP ETL Project

## Overview

This project performs ETL (Extract, Transform, Load) operations on Country GDP data. It extracts GDP information from a Wikipedia page, transforms the data, and loads it into both a CSV file and an SQLite database.

## Features

- Web scraping of GDP data from Wikipedia
- Data transformation from USD millions to USD billions
- CSV file generation
- SQLite database population
- SQL query execution
- Logging of ETL process steps

## Requirements

- Python 3.x
- pandas
- numpy
- requests
- BeautifulSoup4
- sqlite3

## Installation

1. Clone this repository:
2. it clone https://github.com/ahmed3ab3az/IBM_Project.git cd IBM_Project
2. Install the required packages:
pip install pandas numpy requests beautifulsoup4


## Usage

Run the main script to execute the entire ETL process:
python main.py
This will:
1. Extract data from the specified Wikipedia URL
2. Transform the GDP data from millions to billions USD
3. Load the data into a CSV file (`Countries_by_GDP.csv`)
4. Load the data into an SQLite database (`World_Economies.db`)
5. Execute a sample query to retrieve countries with GDP >= 100 billion USD
6. Log all steps in `etl_project_log.txt`

## Project Structure

- `etl.py`: Contains all ETL functions
- `main.py`: Orchestrates the ETL process
- `Countries_by_GDP.csv`: Output CSV file
- `World_Economies.db`: SQLite database
- `etl_project_log.txt`: Log file for ETL process

## Functions

- `extract(url, table_attribs)`: Scrapes GDP data from the given URL
- `transform(df)`: Converts GDP from millions to billions USD
- `load_to_csv(df, csv_path)`: Saves data to a CSV file
- `load_to_db(df, sql_connection, table_name)`: Loads data into SQLite
- `run_query(query_statement, sql_connection)`: Executes SQL query
- `log_progress(message)`: Logs ETL process steps

## Customization

You can modify the following variables in `main.py` to customize the ETL process:

- `url`: Source URL for GDP data
- `table_attribs`: Columns to extract from the source
- `db_name`: Name of the SQLite database
- `table_name`: Name of the table in the database
- `csv_path`: Path for the output CSV file

## License

This project is open-source and available under the MIT License.

## Contributing

Contributions, issues, and feature requests are welcome. Feel free to check [issues page](https://github.com/ahmed3ab3az/IBM_Project/issues) if you want to contribute.

## Author

Ahmed Abdelaziz
