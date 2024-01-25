# Real Estate Investment Funds Data Extraction and Storage

This Python script is a comprehensive solution for extracting, structuring, and storing data related to Real Estate Investment Funds (FII) from the "Funds Explorer" website. It's an end-to-end process that involves web scraping, data parsing, schema definition, and data storage in an S3-compatible bucket using MinIO.

### Features:

- **Web Scraping**: Utilizes Playwright to navigate to "https://www.fundsexplorer.com.br/ranking" and retrieves the HTML content of the page, specifically targeting the tables containing metrics about real estate investment funds.
- **Data Parsing**: Employs BeautifulSoup to parse the HTML content, extract relevant data from the tables, and structure it.
- **Schema Definition**: Uses PySpark to define a schema based on the table headers, ensuring the data is structured and ready for analysis.
- **Data Storage**: Configures a connection to an S3-compatible storage (MinIO in this case) and stores the extracted data in its raw form as a CSV file in a "bronze" bucket for further processing.

### Dependencies:

- **playwright**: For automating and controlling the Chromium browser to scrape data.
- **beautifulsoup4**: For parsing HTML and extracting required information.
- **pyspark**: For data processing, defining schemas, and interacting with the S3 storage.
- **datetime**: For timestamping the data extraction process.

### Usage:

1. **Initialize the Spark Session**: Configure the session with necessary parameters to interact with the S3 storage.
2. **Extract Data from the Webpage**: Launch a Chromium browser using Playwright, navigate to the target webpage, and retrieve the HTML content.
3. **Parse the HTML Content**: Use BeautifulSoup to extract and structure the table headers and row data.
4. **Define the Data Schema**: Utilize PySpark's StructType and StructField to define the schema based on the extracted headers.
5. **Create and Store the DataFrame**: Create a PySpark DataFrame with the structured data and write it as a CSV file to the specified S3 bucket.