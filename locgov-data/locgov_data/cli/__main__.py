# pylint: disable=wildcard-import

"""
Primary script for running locgov_data as a non-coder.
"""

# System libraries
import sys
import os
from pathlib import Path
import plac

# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries
from locgov_data.classes.config import Config
from locgov_data.classes.webarchives import WebArchives
from locgov_data.classes.locgov import LocGovRecords

# Check Python version before proceeding
if sys.version_info <= (3, 9):
    print("Python version 3.9 or higher is required,")
    print(
        "but the current version of Python is "
        + str(sys.version_info.major)
        + "."
        + str(sys.version_info.minor)
        + "."
    )
    sys.exit(6)


@plac.annotations(  # help, kind, abbrev, type, choices, metavar)
    input_type=(
        "Str. 'CSV' or 'search' depending on whether your input is a CSV file "
        "of loc.gov IDs or a loc.gov search url. Default: 'search'",
        "option",
        "t",  # param can be passed as --t or --input-type
        str,
        ["csv", "CSV", "search"],
    ),
    input_path=(
        "Str. loc.gov search URL or path to a CSV file. Default: "
        "https://www.loc.gov/newspapers/?dates=1820/1821&fa=location_state:district+of+columbia",
        "option",
        "i",  # -i
        str,
    ),
    n=("Int. For searches, collect only the top n results.", "option", "n", int),
    output_dir=(
        "Str. Local directory to save outputs. Default is ./output/ .",
        "option",
        "d",
        str,
    ),
    output_prefix=(
        "Str. Prefix to add to all output files.",
        "option",
        "x",
        str,
    ),
    pause=(
        "Int. Base number of seconds to pause between requests. Default: 15",
        "option",
        "p",
        int,
    ),
    env=(
        "Str. Environment to operate in: prod (default), test, or dev. Only prod is publicly available.",
        "option",
        None,
        str,
        ["prod", "dev", "test"],
    ),
    user_agent=(
        "Str. Email address, app URL, or other identifier used to tag traffic.",
        "option",
        "u",
        str,
    ),
    log=("Str. Directory to save log files. Default: ./log/", "option", "l", str),
    log_debug=("Bool. Whether to set file logging to 'debug' level", "flag", "o"),
    verbose=("Bool. Whether to print logs to the terminal ", "flag", "v"),
    is_election=(
        "Bool. Whether this is targetting US Election web archives. If so, MODS "
        "files will be retrieved and parsed into an additional CSV.",
        "flag",
        "e",
    ),
    get_items=(
        "Bool. Whether to also get item-level records. If the input is a CSV list, "
        "this will be true regardless",
        "flag",
        "g",
    ),
)
def main(
    input_type="search",
    input_path=(
        "https://www.loc.gov/newspapers/"
        "?dates=1820/1821&fa=location_state:district+of+columbia"
    ),
    n=0,
    output_dir="./output/",
    output_prefix="",
    pause=15,
    env="prod",
    user_agent=None,
    log="./log/",
    log_debug=False,
    verbose=False,
    is_election=False,
    get_items=False,
):
    """
    Collects loc.gov metadata from a CSV of item ids or a loc.gov search URL.

    This method can retrieve a loc.gov search, loc.gov item records, and US
    Election MODS records, depending on inputs provided. Outputs are a series
    of CSV files.

    Inputs:
     - input_type (str): 'CSV' or 'search' (case insensitive) depending on whether
        your input is a CSV file of loc.gov IDs or a loc.gov search url. Default:
        'search',
     - input_path (str): loc.gov search URL or path to a CSV file. Default:
        'https://www.loc.gov/newspapers/?dates=1820/1821&fa=location_state:district+of+columbia',
     - n (int): If an integer greater than 0 is supplied, only the top n results will be
        fetched. A zero will return all results.
     - output_dir (str): Local directory to save outputs. Default is ./output/
     - output_prefix (str): Prefix to add to all output files. Default is "".
     - pause (int): Base number of seconds to pause between requests. Default: 15
     - env (str): Environment to operate in: prod (default), test, or dev. Only prod is publicly available.
     - user_agent (str): Email address, app URL, or other identifier used to tag
        traffic. No default."
     - log (str): Directory to save log files. Default: ./log/
     - log_debug (bool): Whether to set log level to DEBUG, in place of INFO.
     - verbose (bool): Whether to print logs to the terminal
     - is_election (bool): Whether you are targetting US Election web archives.
        If so, MODS files will be retrieved and parsed into an additional CSV."
     - get_items (bool): Whether to get only the search metadata (False) or also
        retrieve and parse item records (True)

    Returns:

    Saves:
     - errors.json - Record of request errors
     - search.csv - Results of loc.gov search (if input_type is 'search')
     - items.csv - Item-level records (always retrieved unless is_election is True)
     - resources.csv - Resources, extracted from items.csv
     - segment_filess.csv - Segments, extracted from items.csv
     - resource_files.csv - Top-level resource files that do not belong to segments (such
        as PDF and full text files), extracted from item.csv
     - metadata.csv - Publishable metadata.csv file for US Election web archives
        (if is_election is True)
    """

    # Normalize input type to lowercase
    input_type = input_type.lower()

    # Set up Config object
    config = Config(
        debug=log_debug,
        log=log,
        verbose=verbose,
        pause=pause,
        user_agent=user_agent,
    )

    # Check if using default input
    if (
        input_path
        == "https://www.loc.gov/newspapers/?dates=1820/1821&fa=location_state:district+of+columbia"
    ) & (input_type == "search"):
        config.logger.warning(
            "You haven't supplied an `input_path` input. The script will use the "
            "demo input: "
            "https://www.loc.gov/newspapers/?dates=1820/1821&fa=location_state:district+of+columbia"
        )

    # Override the get_items flag, in certain cases
    if is_election is True:
        get_items = False
    elif input_type == "csv":
        get_items = True

    # Perform loc.gov search and optionally request item records.
    # If CSV contains resource_is column, resources will also be fetched, to obtain
    # item ids.
    locgov_records = LocGovRecords(
        input_type=input_type,
        input_path=input_path,
        env=env,
        config=config,
        output_dir=output_dir,
        output_prefix=output_prefix,
    )
    locgov_records.get_locgov_records(get_items=get_items, n=n, save=True)

    # gets MODS XML files and parses metadata from those records.
    if is_election is True:
        uselection_records = WebArchives(locgov_records)
        uselection_records.get_mods_uselection()
        if uselection_records.make_metadata_csv() is True:
            metadata_csv_path = os.path.join(locgov_records.output_dir, "metadata.csv")
            uselection_records.metadata_csv.to_csv(metadata_csv_path, index=False)
            config.logger.info("Metadata.csv saved to %s", metadata_csv_path)

    config.logger.info("Done!")


def pyproject_entry(*args, **kwargs):
    """Entry point for scripts from the [project.scripts] section of pyproject.toml"""
    plac.call(main)


if __name__ == "__main__":
    plac.call(main)
