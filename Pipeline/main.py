import os
import pandas as pd
from fetch_alerts import AlertScraper
from cone_search import CatSearch
from atlas_query import Atclean_Query
import logging
import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def txt_finder(atlas_txt_dir, colour, avg):
    '''
    Finds the appropiate cleaned+averaged txt file for lightcurves
    '''
    for root,dirs,files in os.walk(atlas_txt_dir):
        txt_files = [os.path.join(atlas_txt_dir,file) for file in files if file.endswith(f'{colour}.{avg}.00days.lc.txt')]

    if txt_files:
        return txt_files
    else:
        logging.info('no light curve data processed')

def plotter(txt_files, output_dir):
    '''
    Compiles all plots and outputs to final_output dir
    '''
    # Customize the plot
    plt.figure(figsize=(10, 6))
    plt.title("Flux vs. Time (MJD)")
    plt.xlabel("Time (MJD)")
    plt.ylabel("Flux (uJy)")
    plt.grid(True)

    for file in txt_files:
        df = pd.read_csv(file, delim_whitespace=True)
        filtered_df = df[(df['uJy'].notna()) and (df['MJD'].notna())]
        galaxy_label = next(
            (segment for segment in file.split('/') if "galaxy" in segment), "Unknown"
        )
        plt.plot(filtered_df['MJD'], filtered_df['uJy'], marker='o', linestyle='-', label=galaxy_label)
        plt.legend(loc="upper right")
        plt.savefig(f'compiled_plot_{galaxy_label}')

def main():
    # Set up paths and URLs
    alert_url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    alert_csv_path = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/alerts/alert_data.csv'
    alert_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/alerts'
    ned_search_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/ned_search'
    data_path = [os.path.join(ned_search_output_dir, csv) for csv in os.listdir(ned_search_output_dir) if csv.endswith(".csv")]
    repo_path = r'/users/jhzhe/Cloned_Repos/ATCleanRepo'
    atlas_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/atlas_query/output'
    final_output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/final_output'
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

    # Stage 4: Compiling plots
    c_txt = txt_finder(atlas_txt_dir=atlas_output_dir, colour='c',avg=4)
    o_txt = txt_finder(atlas_txt_dir=atlas_output_dir, colour='o', avg=4)

    plotter(c_txt, output_dir=final_output_dir)
    plotter(o_txt, final_output_dir)

if __name__ == "__main__":
    main()
