import os
import pandas as pd
from fetch_alerts import AlertScraper
from cone_search import CatSearch
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Set up paths and URLs
    alert_url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    alert_csv_path = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/alerts/alert_data.csv'
    alert_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/alerts'
    ned_search_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/ned_search'
    tap_url = "https://ned.ipac.caltech.edu/tap/sync"

    # Step 1: Scrape the alerts
    scraper = AlertScraper(alert_url, alert_csv_path, alert_output_dir)
    scraper.scrape()

    # Step 2: Check if the CSV was updated and read it for cone search
    if os.path.exists(alert_csv_path):
        try:
            alert_df = pd.read_csv(alert_csv_path)
            if not alert_df.empty:
                # Step 3: Run the cone search using the DataFrame
                cat_search = CatSearch(tap_url, ned_search_output_dir, alert_df)
                cat_search.search()
            else:
                logging.warning("The alert CSV file is empty. No data to process for cone search.")
        except Exception as e:
            logging.error(f"An error occurred while reading the alert CSV: {e}")
    else:
        logging.error("Alert CSV file not found. Skipping cone search.")

if __name__ == "__main__":
    main()
