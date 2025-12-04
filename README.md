# Bank ETL Pipeline

ETL (Extract, Transform, Load) pipeline that scrapes data about the world's largest banks by market capitalization from Wikipedia, transforms the data with currency conversions, and loads it into CSV and SQLite database.

## Features

- **Extract**: Web scraping from Wikipedia using BeautifulSoup
- **Transform**: Currency conversion (USD → GBP, EUR, INR)
- **Load**: Exports to CSV and SQLite database
- **Logging**: Timestamped progress tracking

## Requirements
```txt
pandas
beautifulsoup4
requests
```

## Project Structure
```
├── etl_banks.py           # Main ETL script
├── exchange_rate.csv      # Currency exchange rates
├── Largest_banks_data.csv # Output CSV file
├── Banks.db               # SQLite database
└── code_log.txt           # Execution log
```

## Usage

1. Install dependencies:
```bash
pip install pandas beautifulsoup4 requests
```

2. Prepare `exchange_rate.csv` with format:
```
Currency,Rate
GBP,0.8
EUR,0.93
INR,82.95
```

3. Run the script:
```bash
python etl_banks.py
```

## Output

- **CSV**: `Largest_banks_data.csv`
- **Database**: `Banks.db` with table `Largest_banks`
- **Log**: `code_log.txt` with timestamped progress

## Sample Query
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('Banks.db')
df = pd.read_sql("SELECT * FROM Largest_banks LIMIT 5", conn)
print(df)
```

## Author

Vergil - Senior Business Analytics Analyst/Data Engineer
```

### `requirements.txt`
```
pandas>=2.0.0
beautifulsoup4>=4.12.0
requests>=2.31.0
