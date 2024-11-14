import subprocess
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Atclean_Query():
    '''
    Currently, bare bones class but idea we may need to expand for the future
    '''
    def __init__(self, repo_path, data_path) -> None:
        self.repo_path = repo_path
        self.data_path = data_path

    def run_script_with_args(self, script_path, *args):
        """
        Run a Python script with specified arguments.
        
        Parameters:
        - script_path (str): The path to the Python script to run.
        - args: Arguments to pass to the script.
        """
        try:
            # Construct the command as a list
            command = ['python', script_path] + list(args)
            subprocess.run(command, check=True)
            logging.info(f"Successfully ran {script_path} with arguments: {' '.join(args)}")
        except subprocess.CalledProcessError as e:
            logging.info(f"Error occurred while running {script_path}: {e}")

    def query(self):
        download_script = os.path.join(self.repo_path, 'download.py')
        clean_script = os.path.join(self.repo_path, 'clean.py')

        # Iterating through rows and running download.py/clean.py for each galaxy
        for csv in self.data_path:
            data_csv = pd.read_csv(csv)
            for index, (ra, dec, date, event) in data_csv[['ra', 'dec', 'date', 'event']].iterrows():     
                name = f'neutrino_{event}_galaxy{index}'
                # Run `download.py` with flags
                download_args = ['--coords', f'{ra},{dec}', '-o', '-l', '200','--mjd0', f'{date+200}', name]
                self.run_script_with_args(download_script, *download_args)

                # Run `clean.py` with its specific flags (modify as needed)
                clean_args = [name, '-x', '-p', '-o', '-g']
                self.run_script_with_args(clean_script, *clean_args)

if __name__ == "__main__":
    repo_path = r'/users/jhzhe/Cloned_Repos/ATCleanRepoTest'
    ned_path = r'/users/jhzhe/Cloned_Repos/MPhys-Project/output_data/ned_search/'
    data_path = [os.path.join(ned_path, csv) for csv in os.listdir(ned_path) if csv.endswith(".csv")]

    atlas_query = Atclean_Query(repo_path=repo_path, data_path=data_path)
    atlas_query.query()