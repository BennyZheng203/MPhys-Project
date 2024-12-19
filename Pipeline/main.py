import os
import pandas as pd
from fetch_alerts import AlertScraper
from cone_search import CatSearch
from atlas_query import Atclean_Query
import logging
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def txt_finder(atlas_txt_dir, colour, avg):
    '''
    Finds the appropiate cleaned+averaged txt file for lightcurves
    '''
    txt_files = []
    for root, dirs, files in os.walk(atlas_txt_dir):
        for file in files:
            if file.endswith(f'{colour}.{avg}.00days.lc.txt'):
                txt_files.append(os.path.join(root, file))

    if txt_files:
        return txt_files
    else:
        logging.info('no light curve data processed')

def plot_averaged_SN(avg_sn, name, ax, colour):
    """
    Plot individual galaxy data on the provided axis.

    Args:
        avg_sn (DataFrame): The DataFrame containing averaged SN data.
        name (str): Label for the plot (e.g., galaxy name).
        ax (Axes): The matplotlib axis to plot on.
    """
    ax.errorbar(
        avg_sn["MJD"],
        avg_sn["uJy"],
        yerr=avg_sn['duJy'],
        fmt="none",
        elinewidth=1,
        capsize=1.2,
        alpha=0.5,
        zorder=10,
        colour = colour
    )
    ax.scatter(
        avg_sn["MJD"],
        avg_sn["uJy"],
        marker="o",
        alpha=0.5,
        zorder=10,
        colour = colour,
        label=name
    )


def plotter(txt_files, output_dir, colour, flag):
    """
    Compiles all plots and outputs them to the output directory.
    
    Args:
        txt_files (list): List of file paths for the light curve data.
        output_dir (str): Directory to save the compiled plots.
    """
    grouped_files = {}
    for file in txt_files:
        # Extract the neutrino ID (e.g., neutrino_139939)
        path_parts = file.replace("\\", "/").split("/")
        neutrino_id = next(part for part in path_parts if part.startswith("neutrino") and "_galaxy" not in part)
        if neutrino_id not in grouped_files:
            grouped_files[neutrino_id] = []
        grouped_files[neutrino_id].append(file)
    # create directory for each id
    id_paths = {}
    for id in grouped_files.keys():
        id_path = os.path.join(output_dir, f'{id}')
        id_paths[id] = id_path
        os.makedirs(id_path, exist_ok=True)
    
    if flag:
        # Plot each group of files
        for neutrino_id, files in grouped_files.items():
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.set_title(f"Flux vs. Time for {neutrino_id}")
            ax.set_xlabel("Time (MJD)")
            ax.set_ylabel("Flux (uJy)")
            ax.grid(True)
            for file in files:
                df = pd.read_csv(file, delim_whitespace=True)
                filtered_df = df[(df['uJy'].notna()) & (df['MJD'].notna())]

                # Extract the galaxy name
                dir_name = os.path.basename(os.path.dirname(file))
                galaxy_name = dir_name.split('_')[-1] if 'galaxy' in dir_name else None

                # Plot the data for this galaxy
                plot_averaged_SN(filtered_df, galaxy_name, ax)
            
            ax.legend(loc="upper right", facecolor="white", framealpha=1.0)
            
            # Save the plot
            output_file = os.path.join(id_paths[neutrino_id], f"{neutrino_id}_compiled_plot_{colour}.png")
            plt.savefig(output_file)
            plt.close(fig)
    # plot pdf
    if not flag:
        # Plot each group of files
        for neutrino_id, files in grouped_files.items():
            output_pdf_path = os.path.join(id_paths[neutrino_id], f'{neutrino_id}_all_{colour}.pdf')
            with PdfPages(output_pdf_path) as pdf:
                files = sorted(files, key=lambda x: int(x.split("galaxy")[-1].split('.')[0]))
                for file in files:
                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.set_title(f"Flux vs. Time for {neutrino_id}")
                    ax.set_xlabel("Time (MJD)")
                    ax.set_ylabel("Flux (uJy)")
                    ax.grid(True)
                    df = pd.read_csv(file, sep='\s+')
                    filtered_df = df[(df['uJy'].notna()) & (df['MJD'].notna())]

                    # Extract the galaxy name
                    dir_name = os.path.basename(os.path.dirname(file))
                    galaxy_name = dir_name.split('_')[-1] if 'galaxy' in dir_name else None
                    # Plot the data for this galaxy
                    plot_averaged_SN(filtered_df, galaxy_name, ax, colour)
                
                    ax.legend(loc="upper right", facecolor="white", framealpha=1.0)
                    
                    # Save the plot
                    pdf.savefig(fig)
                    plt.close(fig)

def main():
    # Set up paths and URLs
    alert_url = "https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    alert_csv_path = r'/users/jhzhe/dev/MPhys-Project/output_data/alerts/alert_data.csv'
    alert_output_dir = r'/users/jhzhe/dev/MPhys-Project/output_data/alerts'
    ned_search_output_dir = r'/users/jhzhe/dev/MPhys-Project/output_data/ned_search'
    data_path = [os.path.join(ned_search_output_dir, csv) for csv in os.listdir(ned_search_output_dir) if csv.endswith(".csv")]
    repo_path = r'/users/jhzhe/Cloned_Repos/ATCleanRepoTest'
    atlas_output_dir = r'/users/jhzhe/dev/MPhys-Project/output_data/atlas_query/output'
    final_output_dir = r'/users/jhzhe/dev/MPhys-Project/output_data/final_output'
    tap_url = "https://ned.ipac.caltech.edu/tap/sync"

    # Stage 1: Scrape the alerts
    scraper = AlertScraper(alert_url, alert_csv_path, alert_output_dir)
    scraper.scrape()

    if os.path.exists(alert_csv_path):
        try:
            alert_df = pd.read_csv(alert_csv_path)
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

    plotter(c_txt, output_dir=final_output_dir, colour='c', flag=False)
    plotter(o_txt, final_output_dir, colour='o', flag=False)

if __name__ == "__main__":
    main()
