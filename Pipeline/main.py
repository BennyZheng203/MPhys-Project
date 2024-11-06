# Main pipeline development

# main.py

from fetch_alerts import AlertScraper

def main():
    url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    data_csv = "alert_data.csv"
    scraper = AlertScraper(url, data_csv)
    scraper.scrape()

if __name__ == "__main__":
    main()
