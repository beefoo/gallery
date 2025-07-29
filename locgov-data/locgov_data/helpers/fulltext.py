# pylint: disable=broad-except

"""
Helper functions and classes related to loc.gov
"""

# System libraries
from pathlib import Path
import sys
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries
from locgov_data.helpers.general import make_request
from locgov_data.classes.config import Config


def altoxml_to_df(alto_url, session=None, config=None) -> pd.DataFrame:
    """
    Takes a single ALTO XML file and outputs a pandas dataframe of strings.
    Ignores white spaces. Can return an empty dataframe.

    Inputs:
     - alto_url (str): ALTO XML file URL
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
     - config ([None, classes.general.Config]): Config object.

    Returns:
    - pd.Dataframe. Dataframe, one row per word. Columns:
            - string
            - string_id
            - string_hpos
            - string_vpos
            - string_width
            - string_height
            - string_wc
            - string_cc
            - string_styleregs
            - textline_id
            - textline_hpos
            - textline_vpos
            - textline_width
            - textline_height
            - textblock_id
            - textblock_hpos
            - textblock_vpos
            - textblock_width
            - textblock_height
            - textblock_stylerefs
            - softwareName
            - softwareVersion
            - fileName
            - alto_url

    """
    if config is None:
        config = Config()
    if session is None:
        session = requests.Session()
    message = f"Converting Alto to dataframe: {alto_url}"
    config.logger.debug(message)
    _, result = make_request(alto_url, session=session, config=config)
    try:
        soup = BeautifulSoup(result.text, "xml")
    except Exception as e:
        message = (
            f"Could not parse {alto_url} as XML. Response: "
            f"{str(result)[0:100]}... . Error message: {e}"
        )
        config.logger.error(message)

    rows = []
    software_name = (
        soup.find("softwareName").string if soup.find("softwareName") else None
    )
    software_version = (
        soup.find("softwareVersion").string if soup.find("softwareVersion") else None
    )
    file_name = soup.find("fileName").string if soup.find("fileName") else None
    for textblock in soup.find_all("TextBlock"):
        textblock_id = textblock.get("ID")
        textblock_hpos = textblock.get("HPOS")
        textblock_vpos = textblock.get("VPOS")
        textblock_width = textblock.get("WIDTH")
        textblock_height = textblock.get("HEIGHT")
        textblock_stylerefs = textblock.get("STYLEREFS")

        # Traverse through all <TextLine> elements in the current TextBlock
        for textline in textblock.find_all("TextLine"):
            textline_id = textline.get("ID")
            textline_hpos = textline.get("HPOS")
            textline_vpos = textline.get("VPOS")
            textline_width = textline.get("WIDTH")
            textline_height = textline.get("HEIGHT")

            # Traverse through all <String> elements in the current TextLine
            for string in textline.find_all("String"):
                # Extract string-specific attributes
                string_id = string.get("ID")
                string_content = string.get("CONTENT")
                string_hpos = string.get("HPOS")
                string_vpos = string.get("VPOS")
                string_width = string.get("WIDTH")
                string_height = string.get("HEIGHT")
                string_wc = string.get("WC")
                string_cc = string.get("CC")
                string_stylerefs = string.get("STYLEREFS")

                # Append the extracted data as a row
                rows.append(
                    {
                        "string": string_content,
                        "string_id": string_id,
                        "string_hpos": string_hpos,
                        "string_vpos": string_vpos,
                        "string_width": string_width,
                        "string_height": string_height,
                        "string_wc": string_wc,
                        "string_cc": string_cc,
                        "string_styleregs": string_stylerefs,
                        "textline_id": textline_id,
                        "textline_hpos": textline_hpos,
                        "textline_vpos": textline_vpos,
                        "textline_width": textline_width,
                        "textline_height": textline_height,
                        "textblock_id": textblock_id,
                        "textblock_hpos": textblock_hpos,
                        "textblock_vpos": textblock_vpos,
                        "textblock_width": textblock_width,
                        "textblock_height": textblock_height,
                        "textblock_stylerefs": textblock_stylerefs,
                        "softwareName": software_name,
                        "softwareVersion": software_version,
                        "fileName": file_name,
                        "alto_url": alto_url,
                    }
                )  # TODO also capture the styleref attributes?
        if len(rows) == 0:
            message = f"No text found in {alto_url}."
            config.logger.warning(message)

    # Create the DataFrame
    try:
        df = pd.DataFrame(rows)
        message = "Alto successfully converted to a dataframe."
        config.logger.debug(message)
        return df
    except Exception as e:
        message = (
            f"Alto not converted to a dataframe. Message: {e}. "
            f"Returning an empty dataframe . . . "
        )
        config.error.debug(message)
        return pd.DataFrame()


def altoxmls_to_df(alto_urls, session=None, config=None) -> pd.DataFrame:
    """
    Takes a list of ALTO XML urls and converts into a single dataframe for all.

    Inputs:
     - alto_urls (list): List of ALTO XML URLs
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - pd.DataFrame: Aggregated dataframe combining words from all ALTO XML files.

    """
    if session is None:
        session = requests.Session()
    dfs = []
    for alto_url in alto_urls:
        df = altoxml_to_df(alto_url, session=session, config=config)
        dfs.append(df)

    try:
        output_df = pd.concat(dfs)
        return output_df
    except Exception as e:
        message = (
            f"Alto dataframes failed to concatenate into a single "
            f"dataframe. Message: {e}. Returning an empty dataframe . . . "
        )
        config.logger.error(message)
