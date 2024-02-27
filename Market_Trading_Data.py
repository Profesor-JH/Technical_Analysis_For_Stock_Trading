
import mysql.connector
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from sqlalchemy import create_engine
import yaml

# Read the configuration file
with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

# Access database credentials from the configuration
db_host = config['database']['host']
db_name = config['database']['database']
db_user = config['database']['user']
db_password = config['database']['password']

# Establish a connection to the MySQL database
connection = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_name)
cursor = connection.cursor()
connection.commit()

# Create MarketData table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS Trading_Data (
    Date DATE,
    Ticker VARCHAR(10),
    Open FLOAT,
    Low FLOAT,
    High FLOAT,
    Close FLOAT,
    Volume INT,
    PRIMARY KEY (Date, Ticker)
);
"""
cursor.execute(create_table_query)
print("Table created or exists")

# Fetch tickers from your Company_Dim table (replace with your actual table name)
get_tickers_query = "SELECT DISTINCT Ticker FROM Ratios_Tech.Dimension_Company where Ticker in ('AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA');"
cursor.execute(get_tickers_query)
tickers = [row[0] for row in cursor.fetchall()]
ticker_symbols = [ticker for ticker in tickers if ticker is not None]

# Use yfinance.Tickers to fetch data for multiple tickers
tickers = yf.Tickers(" ".join(ticker_symbols))
start_date = datetime.now() - timedelta(days=365)
end_date = datetime.now()

# Initialize an empty DataFrame to store merged data
merged_data = pd.DataFrame()

# Iterate through each ticker
for ticker_symbol in tqdm(ticker_symbols, desc="Fetching Data"):
    ticker = tickers.tickers[ticker_symbol]
    data = ticker.history(start=start_date, end=end_date)

    # Check for NaN values in the data
    if data.isnull().values.any():
        print(f"Skipping ticker {ticker_symbol} due to NaN values.")
        continue

    # Add ticker symbol as a column
    data['Ticker'] = ticker_symbol

    # Append data for current ticker to the merged DataFrame
    merged_data = pd.concat([merged_data, data])

merged_data.to_csv("sample-data.csv", index=False)
# Reset index to make Date a column
merged_data.reset_index(inplace=True)

# Convert Date to datetime format
merged_data['Date'] = merged_data['Date'].dt.date

# Insert merged data into the database
for index, row in tqdm(merged_data.iterrows(), total=len(merged_data), desc="Inserting Data"):
    insert_query = """
    INSERT INTO Trading_Data (Date, Ticker, Open, Low, High, Close, Volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    Open = VALUES(Open),
    Low = VALUES(Low),
    High = VALUES(High),
    Close = VALUES(Close),
    Volume = VALUES(Volume);
    """
    values = (row['Date'], row['Ticker'], row['Open'], row['Low'], row['High'], row['Close'], row['Volume'])
    cursor.execute(insert_query, values)
    connection.commit()
    print(f"Data inserted for {row['Ticker']} on {row['Date']}")

# Close cursor and connection
cursor.close()
connection.close()
