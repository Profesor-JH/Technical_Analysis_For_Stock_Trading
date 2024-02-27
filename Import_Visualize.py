import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
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

# Query to select data from the Market_Data table
query = "SELECT * FROM Trading_Data"

# Import data into a DataFrame
df = pd.read_sql(query, connection)

# Close the database connection
connection.close()

# Data Visualization
# Example: Plotting Close prices for each Ticker over time
for ticker, data in df.groupby('Ticker'):
    plt.plot(data['Date'], data['Close'], label=ticker)

plt.xlabel('Date')
plt.ylabel('Close Price')
plt.title('Close Prices of Different Tickers Over Time')
plt.legend()
plt.show()