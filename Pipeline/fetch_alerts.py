import requests
from bs4 import BeautifulSoup
import pandas as pd
import hashlib
import os

class AlertScraper:
    def __init__(self, url, data_csv, output_dir):
        """
        Initialize the AlertScraper with a URL and a CSV file path for data storage.

        Parameters:
        - url (str): URL of the website to scrape.
        - data_csv (str): Path to the CSV file where the scraped data will be saved.
        """
        self.url = url
        self.data_csv = data_csv
        self.output_dir = output_dir
        self.last_etag = None  # Stores the last known ETag to check for page updates.
        self.last_hash = None  # Stores the last known hash for content change detection.

    def has_page_updated(self):
        """
        Check if the page has been updated by examining the ETag or content hash.

        Returns:
        - bool: True if the page has been updated, False otherwise.
        """
        # Make a HEAD request to retrieve the headers without the full page content.
        response = requests.head(self.url)
        etag = response.headers.get("ETag")

        # If an ETag is provided, use it to detect changes.
        if etag:
            if etag != self.last_etag:
                print("Page updated (ETag changed).")
                self.last_etag = etag  # Update the stored ETag
                return True
            print("No update detected (ETag unchanged).")
            return False
        else:
            # If no ETag, fall back to content hashing for change detection.
            return self.has_content_hash_changed()

    def has_content_hash_changed(self):
        """
        Fallback method to check for page updates by fetching the content and hashing it.

        Returns:
        - bool: True if the page content has changed (based on hash comparison), False otherwise.
        """
        # Fetch the page content and compute a hash for comparison.
        html = self.request_page()
        if html is None:
            return False

        current_hash = hashlib.md5(html.encode("utf-8")).hexdigest()
        if self.last_hash != current_hash:
            print("Page updated (Content hash changed).")
            self.last_hash = current_hash  # Update the stored hash
            return True
        print("No update detected (Content hash unchanged).")
        return False

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

    def parse_table(self, html):
        """
        Parse the HTML content to extract a specific table's data into a DataFrame.

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
        filtered_alert_df = alert_df.loc[alert_df['Rev'] == str(1)].drop(['Rev'], axis=1)
        
        return filtered_alert_df

    def scrape(self):
        """
        Main method to check for updates and scrape data if the page has been updated.
        If updated, saves the data as a CSV file.
        """
        # Check if the page has been updated
        if self.has_page_updated():
            html = self.request_page()
            if html:
                # Parse the table and save the resulting DataFrame as a CSV
                data = self.parse_table(html)
                data_path = os.path.join(self.output_dir, 'alert_data.csv')
                data.to_csv(data_path, index=False)
                print("Data saved to CSV.")
            else:
                print("Failed to retrieve page content.")
        else:
            print("No new data to scrape.")

# Usage
if __name__ == "__main__":
    # Define the URL and output CSV file path
    url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    data_csv = "alert_data.csv"
    output_dir = r'/users/jhzhe/Cloned_Repos/MPhys_Project/output_data/alerts'
    # Initialize and run the scraper
    scraper = AlertScraper(url, data_csv)
    scraper.scrape()
