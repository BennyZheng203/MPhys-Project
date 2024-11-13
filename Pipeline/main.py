import os
import pandas as pd
from fetch_alerts import AlertScraper
from cone_search import CatSearch
from atlas_query import Atclean_Query
import logging
import matplotlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Set up paths and URLs
    alert_url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    alert_csv_path = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/alerts/alert_data.csv'
    alert_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/alerts'
    ned_search_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/ned_search'
    data_path = [os.path.join(ned_search_output_dir, csv) for csv in os.listdir(ned_search_output_dir) if csv.endswith(".csv")]
    repo_path = r'/users/jhzhe/Cloned_Repos/ATCleanRepo'
    tap_url = "https://ned.ipac.caltech.edu/tap/sync"

    # Stage 1: Scrape the alerts
    scraper = AlertScraper(alert_url, alert_csv_path, alert_output_dir)
    scraper.scrape()

    if os.path.exists(alert_csv_path):
        try:
            alert_df = pd.read_csv(alert_csv_path)
            alert_df = alert_df.head(3)
            if not alert_df.empty:
                # Stage 2: Run the cone search using the DataFrame
                cat_search = CatSearch(tap_url, ned_search_output_dir, alert_df)
                cat_search.search()
            else:
                logging.warning("The alert CSV file is empty. No data to process for cone search.")
        except Exception as e:
            logging.error(f"An error occurred while reading the alert CSV: {e}")
    else:
        logging.error("Alert CSV file not found. Skipping cone search.")

    # Stage 3: Query ATLAS via ATClean
    atlas_query = Atclean_Query(repo_path=repo_path, data_path=data_path)
    atlas_query.query()

if __name__ == "__main__":
    main()
