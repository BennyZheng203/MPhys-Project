import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

class AlertScraper:
    def __init__(self, url, data_csv, output_dir):
        """
        Initialize the AlertScraper with a URL, a CSV file path for comparison, and an output directory.

        Parameters:
        - url (str): URL of the website to scrape.
        - data_csv (str): Path to the CSV file for comparison.
        - output_dir (str): Directory path to save the updated data.
        """
        self.url = url
        self.data_csv = data_csv
        self.output_dir = output_dir

    def request_page(self):
        """
        Request the page content from the URL.

        Returns:
        - str or None: HTML content of the page if successful, None if there was an error.
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Check if the request was successful
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {self.url}: {e}")
            return None

    def parse_table_head(self, html):
        """
        Parse the HTML content to extract the first entry from the relevant table.

        Parameters:
        - html (str): HTML content of the page.

        Returns:
        - str or None: The first entry of the table, or None if no data is found.
        """
        bs = BeautifulSoup(html, 'html.parser')
        table = bs.find('table', {'border': '2'})  # Locate the relevant table by border attribute
        if not table:
            print("No table found.")
            return None
        
        first_row = table.find_all('tr')[2]  # Get the first data row (after skipping header rows)
        first_entry = first_row.find_all('td')[0].get_text(strip=True)  # Get the first cell of the first row
        return first_entry

    def get_latest_csv_entry(self):
        """
        Get the first entry from the CSV file for comparison.

        Returns:
        - str or None: The first entry from the CSV, or None if the CSV is empty or not found.
        """
        if not os.path.exists(self.data_csv):
            print("CSV file not found.")
            return None
        
        try:
            df = pd.read_csv(self.data_csv)
            if df.empty:
                print("CSV file is empty.")
                return None
            
            return str(df.iloc[0, 0])  # Return the first entry (first cell of the first row)
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None

    def scrape(self):
        """
        Main method to check for updates and scrape data if the first table entry is different.
        If updated, saves the data as a CSV file.
        """
        html = self.request_page()
        if html:
            current_first_entry = self.parse_table_head(html)
            csv_first_entry = self.get_latest_csv_entry()
            
            if current_first_entry and current_first_entry != csv_first_entry:
                print("New data detected. Updating CSV.")
                data = self.parse_table(html)
                data_path = os.path.join(self.output_dir, 'alert_data.csv')
                data.to_csv(data_path, index=False)
                print("Data saved to CSV.")
            else:
                print("No new data detected.")
        else:
            print("Failed to retrieve page content.")

    def parse_table(self, html):
        """
        Parse the HTML content to extract the entire table's data into a DataFrame.

        Parameters:
        - html (str): HTML content of the page.

        Returns:
        - pd.DataFrame: DataFrame containing relevant columns from the table.
        """
        bs = BeautifulSoup(html, 'html.parser')
        table = bs.find('table', {'border': '2'})  # Locate the relevant table by border attribute

        # Extract header columns, skipping the first two headers
        headers = [th.get_text(strip=True) for th in table.find_all('th', limit=12)[2:]]
        
        data = []
        # Iterate over each row in the table, skipping the first two rows
        for row in table.find_all('tr')[2:]:
            columns = row.find_all('td', limit=10)  # Select up to 10 columns of data
            row_data = [col.get_text(strip=True) for col in columns]
            data.append(row_data)
        
        # Create a DataFrame and filter rows where the 'Rev' column has a value of '1'
        alert_df = pd.DataFrame(data, columns=headers)
        filtered_alert_df = alert_df.loc[alert_df['Rev'] != str(0)].drop(['Rev'], axis=1)
        
        return filtered_alert_df

# Usage
if __name__ == "__main__":
    # Define the URL, CSV file for comparison, and output directory
    url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    data_csv = r'/users/jhzhe/Cloned_Repos/MPhys_Project/output_data/alerts/alert_data.csv'
    output_dir = r'/users/jhzhe/Cloned_Repos/MPhys_Project/output_data/alerts'

    # Initialize and run the scraper
    scraper = AlertScraper(url, data_csv, output_dir)
    scraper.scrape()
