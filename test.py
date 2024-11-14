#!/usr/bin/env python

"""
This script cleans and processes light curves of astronomical transients (e.g., supernovae).
It applies various cuts (e.g., uncertainty cuts, chi-square cuts) to the data and outputs the
cleaned and processed results along with diagnostic plots.
"""

from typing import Callable, List
import sys
import argparse
import pandas as pd
import numpy as np
from copy import deepcopy
from lightcurve import (
    DEFAULT_CUT_NAMES,
    Cut,
    CutList,
    LimCutsTable,
    SnInfoTable,
    Supernova,
    AveragedSupernova,
    get_mjd0,
)
from download import (
    Credentials,
    load_config,
    make_dir_if_not_exists,
    parse_comma_separated_string,
)
from plot import PlotPdf

"""
UTILITY FUNCTIONS
"""

def hexstring_to_int(hexstring):
    """
    Convert a hexadecimal string to an integer.

    Args:
        hexstring (str): Hexadecimal string (e.g., "0x1A").

    Returns:
        int: Integer representation of the hexadecimal value.
    """
    return int(hexstring, 16)

"""
CLASS DEFINITIONS
"""

class OutputReadMe:
    """
    Handles the creation and writing of a README.md file summarizing the cleaning
    process for each transient.

    Output Directory:
        The README.md file is saved in the directory corresponding to the transient's
        name, e.g., `output_dir/tnsname/README.md`.
    """
    def __init__(self, output_dir, tnsname, cut_list, num_controls=0):
        """
        Initialize the OutputReadMe object.

        Args:
            output_dir (str): Base output directory for processed results.
            tnsname (str): Name of the transient being processed.
            cut_list (CutList): List of cuts applied to the data.
            num_controls (int, optional): Number of control light curves processed.
        """
        # Define the output README.md file path.
        filename = f"{output_dir}/{tnsname}/README.md"
        print(f"\nOpening README.md file for outputting cut information at {filename}...")
        self.f = open(filename, "w+")
        self.tnsname: str = tnsname
        self.cut_list: CutList = cut_list
        self.begin(num_controls=num_controls)
        print("Success")

    def begin(self, num_controls=0):
        """
        Write the header and general information about the transient's light curve
        processing to the README file.
        """
        badday_cut = self.cut_list.get("badday_cut")
        if badday_cut:
            mjdbinsize = badday_cut.params["mjd_bin_size"]
        else:
            mjdbinsize = 1.0

        self.f.write(f"# SN {self.tnsname} Light Curve Cleaning and Averaging")
        self.f.write(
            f'\n\nThe ATLAS SN light curves are separated by filter (orange and cyan) and labelled as such in the file name. Averaged light curves contain an additional number in the file name that represents the MJD bin size used. Control light curves are located in the "controls" subdirectory and follow the same naming scheme, only with their control index added after the SN name.'
        )

        # List original and cleaned light curve filenames.
        self.f.write(f"\n\nThe following details the file names for each of the light curve versions:")
        self.f.write(f"\n\t- Original SN light curves: {self.tnsname}.o.lc.txt and {self.tnsname}.c.lc.txt")
        self.f.write(f"\n\t- Cleaned SN light curves: {self.tnsname}.o.clean.lc.txt and {self.tnsname}.c.clean.lc.txt")
        if self.cut_list.has("badday_cut"):
            self.f.write(
                f"\n\t- Averaged light curves (for MJD bin size {mjdbinsize:0.2f} days): {self.tnsname}.o.{mjdbinsize:0.2f}days.lc.txt and {self.tnsname}.c.{mjdbinsize:0.2f}days.lc.txt"
            )
        if self.cut_list.has("controls_cut"):
            self.f.write(
                f"\n\t- Control light curves, where X=001,...,{num_controls:03d}: {self.tnsname}_iX.o.lc.txt and {self.tnsname}_iX.c.lc.txt"
            )

    def save(self):
        """
        Close the README.md file after writing.
        """
        self.f.close()

class CleanLoop:
    """
    Manages the main loop for cleaning light curves, applying cuts, and saving results.

    Input Directory:
        Light curve files are loaded from `input_dir`.

    Output Directory:
        Processed light curves, diagnostic plots, and other results are saved to
        `output_dir`.

    Args:
        input_dir (str): Directory containing the input light curve files.
        output_dir (str): Directory where cleaned results and plots are saved.
    """
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        credentials: Credentials,
        sninfo_filename: str = None,
        flux2mag_sigmalimit: float = 3.0,
        overwrite: bool = False,
    ):
        # Initialize paths and parameters for processing.
        self.input_dir: str = input_dir
        self.output_dir: str = output_dir
        self.credentials: Credentials = credentials
        self.flux2mag_sigmalimit: float = flux2mag_sigmalimit
        self.overwrite: bool = overwrite

        # Initialize tables for SN and cut information.
        self.sninfo: SnInfoTable = SnInfoTable(self.output_dir, filename=sninfo_filename)
        self.uncert_est_info: UncertEstTable = UncertEstTable(self.output_dir)

    def clean_lcs(
        self,
        tnsname: str,
        mjd0,
        filt: str,
        apply_uncert_est_function: Callable,
        num_controls: int = 0,
        apply_template_correction: bool = False,
        plot: bool = False,
    ):
        """
        Clean the light curves for a specific transient.

        Args:
            tnsname (str): Name of the transient being processed.
            mjd0 (float): Start date of the transient in MJD.
            filt (str): Filter to process ("o" or "c").
            apply_uncert_est_function (Callable): Function to determine whether to apply uncertainty estimation.
            num_controls (int, optional): Number of control light curves to process.
            apply_template_correction (bool, optional): Whether to apply template correction.
            plot (bool, optional): Whether to generate diagnostic plots.
        """
        print(f"\nCleaning light curves for: {tnsname}, Filter: {filt}")
        # Load light curves from input directory.
        self.sn = Supernova(tnsname=tnsname, mjd0=mjd0, filt=filt)
        self.sn.load_all(self.input_dir, num_controls=num_controls)

        # Save cleaned results to output directory.
        self.sn.save_all(self.output_dir, overwrite=self.overwrite)

        # Optional: Generate and save diagnostic plots.
        if plot:
            self.p = PlotPdf(f"{self.output_dir}/{tnsname}", tnsname, filt=filt)
            self.p.save_pdf()

    def loop(
        self,
        tnsnames: List[str],
        apply_uncert_est_function: Callable,
        cut_list: CutList = None,
        num_controls: int = 0,
        mjd0=None,
        filters: List[str] = ["o", "c"],
        apply_template_correction: bool = False,
        plot: bool = False,
    ):
        """
        Loop over the provided list of transients and process each one.

        Args:
            tnsnames (List[str]): List of transient names to process.
            apply_uncert_est_function (Callable): Function to determine whether to apply uncertainty estimation.
            cut_list (CutList, optional): List of cuts to apply to the light curves.
            num_controls (int, optional): Number of control light curves to process.
            mjd0 (float, optional): Start date for transients in MJD.
            filters (List[str], optional): Filters to process ("o" or "c").
            apply_template_correction (bool, optional): Whether to apply template correction.
            plot (bool, optional): Whether to generate diagnostic plots.
        """
        self.cut_list = cut_list

        for tnsname in tnsnames:
            print(f"\nProcessing transient: {tnsname}")
            make_dir_if_not_exists(f"{self.output_dir}/{tnsname}")
            self.f = OutputReadMe(self.output_dir, tnsname, cut_list, num_controls=num_controls)

            for filt in filters:
                self.clean_lcs(
                    tnsname,
                    mjd0,
                    filt,
                    apply_uncert_est_function,
                    num_controls=num_controls,
                    apply_template_correction=apply_template_correction,
                    plot=plot,
                )

                
def parse_config_filters(args, config):
    if args.filters:
        return parse_comma_separated_string(args.filters)
    else:
        return parse_comma_separated_string(config["convert"]["filters"])


def find_config_custom_cuts(config):
    print("\nSearching config file for custom cuts...")
    custom_cuts = []
    for key in config:
        if key.endswith("_cut") and not key in DEFAULT_CUT_NAMES:
            custom_cuts.append(config[key])
    print(f"Found {len(custom_cuts)}")
    return custom_cuts


def parse_config_cuts(args, config):
    cut_list = CutList()
    if args.custom_cuts:
        config_custom_cuts = find_config_custom_cuts(config)

    print(f"\nProcedures to apply:")

    # always check true uncertainties estimation, but will only apply if args.true_uncert_est
    temp_x2_max_value = float(config["uncert_est"]["temp_x2_max_value"])
    print(
        f"- True uncertainties estimation check: temporary chi-square cut at {temp_x2_max_value}"
    )
    params = {
        "temp_x2_max_value": temp_x2_max_value,
        "uncert_cut_flag": hexstring_to_int(config["uncert_cut"]["flag"]),
    }
    uncert_est = Cut(params=params)
    cut_list.add(uncert_est, "uncert_est")

    if args.uncert_cut:
        uncert_cut = Cut(
            column="duJy",
            max_value=float(config["uncert_cut"]["max_value"]),
            flag=hexstring_to_int(config["uncert_cut"]["flag"]),
        )
        cut_list.add(uncert_cut, "uncert_cut")
        print(f"- Uncertainty cut: {uncert_cut}")

    if args.x2_cut:
        params = {
            "stn_bound": float(config["x2_cut"]["stn_bound"]),
            "min_cut": int(config["x2_cut"]["min_cut"]),
            "max_cut": int(config["x2_cut"]["max_cut"]),
            "cut_step": int(config["x2_cut"]["cut_step"]),
            "use_pre_mjd0_lc": config["x2_cut"]["use_pre_mjd0_lc"] == "True",
        }
        x2_cut = Cut(
            column="chi/N",
            max_value=float(config["x2_cut"]["max_value"]),
            flag=hexstring_to_int(config["x2_cut"]["flag"]),
            params=params,
        )
        cut_list.add(x2_cut, "x2_cut")
        print(f"- Chi-square cut: {x2_cut}")

    if args.controls_cut:
        params = {
            "questionable_flag": hexstring_to_int(
                config["controls_cut"]["questionable_flag"]
            ),
            "x2_max": float(config["controls_cut"]["x2_max"]),
            "x2_flag": hexstring_to_int(config["controls_cut"]["x2_flag"]),
            "stn_max": float(config["controls_cut"]["stn_max"]),
            "stn_flag": hexstring_to_int(config["controls_cut"]["stn_flag"]),
            "Nclip_max": int(config["controls_cut"]["Nclip_max"]),
            "Nclip_flag": hexstring_to_int(config["controls_cut"]["Nclip_flag"]),
            "Ngood_min": int(config["controls_cut"]["Ngood_min"]),
            "Ngood_flag": hexstring_to_int(config["controls_cut"]["Ngood_flag"]),
        }
        controls_cut = Cut(
            flag=hexstring_to_int(config["controls_cut"]["bad_flag"]), params=params
        )
        cut_list.add(controls_cut, "controls_cut")
        print(f"- Control light curve cut: {controls_cut}")

    if args.averaging:
        params = {
            "mjd_bin_size": (
                float(config["averaging"]["mjd_bin_size"])
                if args.mjd_bin_size is None
                else args.args.mjd_bin_size
            ),
            "x2_max": float(config["averaging"]["x2_max"]),
            "Nclip_max": int(config["averaging"]["Nclip_max"]),
            "Ngood_min": int(config["averaging"]["Ngood_min"]),
            "ixclip_flag": hexstring_to_int(config["averaging"]["ixclip_flag"]),
            "smallnum_flag": hexstring_to_int(config["averaging"]["smallnum_flag"]),
        }
        badday_cut = Cut(
            flag=hexstring_to_int(config["averaging"]["flag"]), params=params
        )
        cut_list.add(badday_cut, "badday_cut")
        print(f"- Bad day cut (averaging): {badday_cut}")

    if args.custom_cuts:
        for i in range(len(config_custom_cuts)):
            cut_settings = config_custom_cuts[i]
            try:
                custom_cut = Cut(
                    column=cut_settings["column"],
                    flag=hexstring_to_int(cut_settings["flag"]),
                    min_value=(
                        float(cut_settings["min_value"])
                        if cut_settings["min_value"] != "None"
                        else None
                    ),
                    max_value=(
                        float(cut_settings["max_value"])
                        if cut_settings["max_value"] != "None"
                        else None
                    ),
                )
                cut_list.add(custom_cut, f"custom_cut_{i}")
                print(f"- Custom cut {i}: {custom_cut}")
            except Exception as e:
                print(f"WARNING: Could not parse custom cut {cut_settings}: {str(e)}")

    has_duplicate_flags, duplicate_flags = cut_list.check_for_flag_duplicates()
    if has_duplicate_flags:
        raise RuntimeError(
            f"ERROR: Cuts in the config file contain duplicate flags: {duplicate_flags}."
        )
    return cut_list


# define command line arguments
def define_args(parser=None, usage=None, conflict_handler="resolve"):
    if parser is None:
        parser = argparse.ArgumentParser(usage=usage, conflict_handler=conflict_handler)

    parser.add_argument(
        "tnsnames", nargs="+", help="TNS names of the transients to clean"
    )
    parser.add_argument(
        "--sninfo_file",
        default=None,
        type=str,
        help="file name of .txt file with SN info table",
    )
    parser.add_argument(
        "--config_file",
        default="config.ini",
        type=str,
        help="file name of .ini file with settings for this class",
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        default=False,
        action="store_true",
        help="overwrite existing file with same file name",
    )
    parser.add_argument(
        "--filters",
        type=str,
        default=None,
        help="comma-separated list of filters to clean",
    )
    parser.add_argument(
        "-p",
        "--plot",
        default=False,
        action="store_true",
        help="store a summary PDF file of diagnostic plots",
    )

    # cleaning a single SN and/or controls
    parser.add_argument(
        "--mjd0", type=float, default=None, help="transient start date in MJD"
    )

    # cleaning control light curves
    # parser.add_argument('-c','--controls', default=False, action='store_true', help='clean control light curves in addition to transient light curve')
    parser.add_argument(
        "--num_controls",
        type=int,
        default=None,
        help="number of control light curves to load and clean",
    )

    # possible cuts
    parser.add_argument(
        "-t",
        "--template_correction",
        default=False,
        action="store_true",
        help="apply automatic ATLAS template change correction",
    )
    parser.add_argument(
        "-e",
        "--uncert_est",
        default=False,
        action="store_true",
        help="apply true uncertainty estimation",
    )
    parser.add_argument(
        "-u",
        "--uncert_cut",
        default=False,
        action="store_true",
        help="apply uncertainty cut",
    )
    parser.add_argument(
        "-x",
        "--x2_cut",
        default=False,
        action="store_true",
        help="apply chi-square cut",
    )
    parser.add_argument(
        "-c",
        "--controls_cut",
        default=False,
        action="store_true",
        help="apply control light curve cut",
    )
    parser.add_argument(
        "-g",
        "--averaging",
        default=False,
        action="store_true",
        help="average light curves and cut bad days",
    )
    parser.add_argument(
        "-m",
        "--mjd_bin_size",
        type=float,
        default=None,
        help="MJD bin size in days for averaging",
    )
    parser.add_argument(
        "--custom_cuts",
        default=False,
        action="store_true",
        help="scan config file for custom cuts",
    )

    return parser


if __name__ == "__main__":
    args = define_args().parse_args()
    config = load_config(args.config_file)

    if len(args.tnsnames) < 1:
        raise RuntimeError("ERROR: Please specify at least one TNS name to clean.")
    if len(args.tnsnames) > 1 and not args.mjd0 is None:
        raise RuntimeError(
            f"ERROR: Cannot specify one MJD0 {args.mjd0} for a batch of SNe."
        )
    print(f"\nList of transients to clean: {args.tnsnames}")

    input_dir = config["dir"]["atclean_input"]
    output_dir = config["dir"]["output"]
    sninfo_filename = config["dir"]["sninfo_filename"]
    make_dir_if_not_exists(input_dir)
    make_dir_if_not_exists(output_dir)
    print(f"\nATClean input directory: {input_dir}")
    print(f"Output directory: {output_dir}")

    print(f'TNS ID: {config["credentials"]["tns_id"]}')
    print(f'TNS bot name: {config["credentials"]["tns_bot_name"]}')

    print(f"Overwrite existing files: {args.overwrite}")
    print(f"Save PDF of diagnostic plots: {args.plot}")
    filters = parse_config_filters(args, config)
    print(f"Filters: {filters}")
    flux2mag_sigmalimit = float(config["download"]["flux2mag_sigmalimit"])
    print(f"Sigma limit when converting flux to magnitude: {flux2mag_sigmalimit}")
    if args.mjd0:
        print(f"MJD0: {args.mjd0}")

    # print(f'\nApplyin control light curve cut: {args.controls}')
    num_controls = (
        args.num_controls
        if not args.num_controls is None
        else int(config["download"]["num_controls"])
    )
    print(f"Number of control light curves to clean: {num_controls}")

    cut_list = parse_config_cuts(args, config)

    print()
    credentials = Credentials(
        config["credentials"]["atlas_username"],
        config["credentials"]["atlas_password"],
        config["credentials"]["tns_api_key"],
        config["credentials"]["tns_id"],
        config["credentials"]["tns_bot_name"],
    )
    clean = CleanLoop(
        input_dir,
        output_dir,
        credentials,
        sninfo_filename=sninfo_filename,
        flux2mag_sigmalimit=flux2mag_sigmalimit,
        overwrite=args.overwrite,
    )

    def apply_uncert_est_function():
        return args.uncert_est

    clean.loop(
        args.tnsnames,
        apply_uncert_est_function,
        cut_list=cut_list,
        num_controls=num_controls,
        mjd0=args.mjd0,
        filters=filters,
        apply_template_correction=args.template_correction,
        plot=args.plot,
    )
