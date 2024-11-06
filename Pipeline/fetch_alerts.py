import requests
from bs4 import BeautifulSoup
import pandas as pd
import hashlib

class AlertScraper:
    def __init__(self, url, data_csv):
        self.url = url
        self.data_csv = data_csv
        self.last_etag = None
        self.last_hash = None

    def has_page_updated(self):
        """Check if the page has been updated by ETag or content hash."""
        # First, attempt to use the ETag header for change detection
        response = requests.head(self.url)
        etag = response.headers.get("ETag")

        if etag:
            if etag != self.last_etag:
                print("Page updated (ETag changed).")
                self.last_etag = etag
                return True
            print("No update detected (ETag unchanged).")
            return False
        else:
            # Fallback: Check by hashing the page content
            return self.has_content_hash_changed()

    def has_content_hash_changed(self):
        """Fallback method: fetch the page content and check if the hash has changed."""
        html = self.request_page()
        if html is None:
            return False

        current_hash = hashlib.md5(html.encode("utf-8")).hexdigest()
        if self.last_hash != current_hash:
            print("Page updated (Content hash changed).")
            self.last_hash = current_hash
            return True
        print("No update detected (Content hash unchanged).")
        return False

    def request_page(self):
        """Request the page content."""
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Check status of website
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {self.url}: {e}")
            return None

    def parse_table(self, html):
        '''
        input:
        html: html text from request_page

        output:
        data: dataframe containing relevant columns 
        '''
        bs = BeautifulSoup(html, 'html.parser')
        table = bs.find('table', {'border': '2'})  # locating relevant table

        headers = [th.get_text(strip=True) for th in 
                   table.find_all('th', limit=12)[2:]]
        
        data = []

        for row in table.find_all('tr')[2:]:
            columns = row.find_all('td', limit=10)  # 10 columns of data (td)
            row_data = [col.get_text(strip=True) for col in columns]
            data.append(row_data)
        
        alert_df = pd.DataFrame(data, columns=headers)
        filtered_alert_df = alert_df.loc[alert_df['Rev'] == str(1)].drop(['Rev'], axis=1)
        
        return filtered_alert_df

    def scrape(self):
        """Main method to scrape data if the page has been updated."""
        if self.has_page_updated():
            html = self.request_page()
            if html:
                data = self.parse_table(html)
                # Save or process data as required
                data.to_csv(self.data_csv, index=False)
                print("Data saved to CSV.")
            else:
                print("Failed to retrieve page content.")
        else:
            print("No new data to scrape.")

# Usage
if __name__ == "__main__":
    url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    data_csv = "alert_data.csv"
    scraper = AlertScraper(url, data_csv)
    scraper.scrape()
