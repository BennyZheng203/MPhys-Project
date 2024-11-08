import subprocess
import os

def run_script_with_args(script_path, *args):
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
        print(f"Successfully ran {script_path} with arguments: {' '.join(args)}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_path}: {e}")

def main():
    repo_path = r"C:\Users\jhzhe\Cloned_Repos\ATCleanRepo"

    # Full paths to the scripts
    download_script = os.path.join(repo_path, 'download.py')
    clean_script = os.path.join(repo_path, 'clean.py')

    # Run `download.py` with flags
    download_args = ['--coords', '134,-13', '-o', '-l' ,'100', '--mjd', '60539', 'candidate_tde2']
    run_script_with_args(download_script, *download_args)

    # Run `clean.py` with its specific flags (modify as needed)
    #clean_args = ['--clean-flag1', 'value1', '--option2']
    #run_script_with_args(clean_script, *clean_args)

if __name__ == "__main__":
    main()
