# etl-top-10-largest-bank
### Automated System for Quarterly Market Capitalization Reports: Transforming Global Banking Data into Multi-Currency Formats.

This project has been developed to automate the process of compiling, transforming, and storing information on the top 10 largest banks in the world, ranked by market capitalization in billion USD. The primary objective is to ensure that this data is readily available in multiple currencies (USD, GBP, EUR, and INR) and can be accessed by managers from different countries to meet their specific needs.

## Project Overview

This system performs the following key tasks:

1. **Data Compilation**: The system gathers information on the top 10 largest banks in the world by market capitalization in billion USD.
2. **Currency Transformation**: Using exchange rate information provided in a CSV file, the system converts the market capitalization values into GBP, EUR, and INR.
3. **Data Storage**: The processed data is saved both locally in a CSV format and in a database table.
4. **Query Support**: The database table is designed to support queries from managers in different countries, allowing them to extract the list and view market capitalization values in their preferred currency.

## Libraries Used
1. **BeautifulSoup**
2. **Requests**
3. **sqlite3**
4. **Pandas**

