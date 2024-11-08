import requests
from astroquery.utils.tap.core import TapPlus
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CatSearch():
    """
    A class to search a catalog using a TAP (Table Access Protocol) service and save the results.

    Attributes:
    - url (str): The URL of the TAP service.
    - output_dir (str): The directory where output files will be saved.
    - alert_csv (pd.DataFrame): A DataFrame containing RA, Dec, and error data.
    - table_name (str): The name of the table to query.
    - columns (str): The columns to retrieve from the table.
    - coord_sys (str): The coordinate system used for the query.
    - count (int): A counter for the number of queries performed.
    """

    def __init__(self, url, output_dir, alert_csv):
        self.url = url
        self.output_dir = output_dir
        self.alert_csv = alert_csv
        
        self.table_name = 'objdir'
        self.columns = 'RA,Dec'
        self.coord_sys = 'J2000'
        self.count = 0
    
    def query_tap(self, coord_ra, coord_dec, err):
        """
        Queries the TAP service using the provided coordinates and error radius.

        Args:
        - coord_ra (float): The right ascension in degrees.
        - coord_dec (float): The declination in degrees.
        - err (float): The error radius in arcminutes.

        Returns:
        - pd.DataFrame: The resulting data from the TAP query as a DataFrame.
        """
        try:
            cone = f"CONTAINS(POINT('{self.coord_sys}', RA, Dec), CIRCLE('{self.coord_sys}', {coord_ra}, {coord_dec}, {err}))=1"
            query = f"SELECT TOP 5 {self.columns} FROM {self.table_name} WHERE {cone}"

            ned = TapPlus(url=self.url)
            job = ned.launch_job(query)
            results = job.get_results()
            coord_data = results.to_pandas()

            return coord_data
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error during TAP query: {e}")
        except Exception as e:
            logging.error(f"An error occurred during the TAP query: {e}")
        
        return pd.DataFrame()  # Return an empty DataFrame on failure
    
    def search(self):
        """
        Processes the alert CSV data and performs a TAP search for each set of coordinates.
        Saves the results as CSV files in the specified output directory.
        """
        try:
            alert_coords = self.alert_csv[['RA [deg]', 'Dec [deg]', 'Error90 [arcmin]']]
            ra_list = [float(x) for x in alert_coords['RA [deg]']]
            dec_list = [float(x) for x in alert_coords['Dec [deg]']]
            err_list = [float(x) for x in alert_coords['Error90 [arcmin]']]
        except KeyError as e:
            logging.error(f"Missing expected column in alert CSV: {e}")
            return
        except Exception as e:
            logging.error(f"An error occurred while processing the alert CSV: {e}")
            return

        for i, (ra, dec, err) in enumerate(zip(ra_list, dec_list, err_list)):
            try:
                coord_data = self.query_tap(ra, dec, err)
                os.makedirs(self.output_dir, exist_ok=True)
                if not coord_data.empty:
                    output_path = os.path.join(self.output_dir, f'NED_SEARCH_{i}.csv')
                    coord_data.to_csv(output_path, index=False)
                    logging.info(f"Saved results to {output_path}")
                else:
                    logging.warning(f"Query failed or returned no results for coordinates: RA={ra}, Dec={dec}, Err={err}")
                self.count += 1
            except Exception as e:
                logging.error(f"An unexpected error occurred during the search process: {e}")

if __name__ == '__main__':
    output_dir = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/ned_search'
    url = "https://ned.ipac.caltech.edu/tap/sync"
    try:
        alert_csv = pd.read_csv(r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/alerts/alert_data.csv')
    except FileNotFoundError as e:
        logging.error(f"Alert CSV file not found: {e}")
        exit()
    except pd.errors.EmptyDataError as e:
        logging.error(f"Alert CSV file is empty: {e}")
        exit()
    except Exception as e:
        logging.error(f"An error occurred while reading the alert CSV file: {e}")
        exit()

    catalogue = CatSearch(url, output_dir, alert_csv)
    catalogue.search()
