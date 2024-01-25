from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session for data processing and S3 interaction
spark = SparkSession.builder \
    .appName("MinioTest") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.15.59:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Variable to hold the content of the webpage
content = ''

# Use Playwright to navigate and retrieve HTML content from the webpage
with sync_playwright() as p:
    # Launch a headless Chromium browser
    browser = p.chromium.launch(headless=True)
    # Open a new page
    page = browser.new_page()
    # Navigate to the target URL and wait for the page to load
    page.goto('https://www.fundsexplorer.com.br/ranking', wait_until='load')
    # Retrieve the HTML content of the page
    content = page.content()

# Use BeautifulSoup to parse the HTML content
soup = BeautifulSoup(content, 'html.parser')

# Extract table headers (column names) from the HTML
ths = soup.find_all('th', attrs={'data-collum': True})
headers = [th.text.strip() for th in ths]

# Extract table rows data from the HTML
trs = soup.find_all('tr', attrs={'scope': True})

# Generate a timestamp for the output file
datetime_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Extract the text from each table data cell, for each row
data = []
for tr in trs:
    tds = tr.find_all('td')
    data.append([td.text.strip() for td in tds])

# Define the schema of the DataFrame based on the table headers
schema = StructType(
    [
        StructField(header, StringType(), True) for header in headers
    ]
)

# Create a DataFrame with the extracted data and defined schema
df = spark.createDataFrame(data, schema=schema)

# Construct the file path for the output CSV file in the S3 bucket
file_path = f's3a://bronze-fii-data/data-fii_{datetime_str}.csv'

# Write the DataFrame to the CSV file in the S3 bucket
df.write.option('header', 'True').csv(file_path)

print('Process completed!')

# Stop the Spark session
spark.stop()