# pylint: disable=broad-except

"""
Helper functions for retrieving and parsing MARC XML records from loc.gov.
 This script is based on DCMTools' marcxmltools.py
"""

# System Libraries
import pymarc
from io import StringIO
from pathlib import Path
import sys
from bs4 import BeautifulSoup
import pandas as pd
import pymarc
import requests
from typing import Union, Tuple
import xml.etree.ElementTree as ET

# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries
from locgov_data.classes.config import Config
from locgov_data.helpers.general import make_request, flatten_json


def get_marcxml_record(
    lccn,
    beautifulsoup=False,
    is_blocked=False,
    session=None,
    config=None,
) -> Tuple[bool, Union[ET.Element, BeautifulSoup]]:
    """
    Pull a record via permalink for a given LCCN and return the MARCXML

    Inputs:
     - lccn (str): LCCN string. Remove spaces.
     - beautifulsoup (bool): If you would like successful output delivered as a
        BeautifulSoup object rather than an ElementTree object.
     - is_blocked (bool): For use in loops. True means a 429 was encountered and
        this request should be skipped.
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
     - config (classes.config.Config): Config object.

    Returns:
     - Union[ET.Element, BeautifulSoup]: A single MARCXML record or an error string from MARCXML_ERRORS
    """
    ERROR_RECORD_NOT_FOUND = (
        "RECORD NOT FOUND"  # LCCN returned a valid "record not found" response
    )
    ERROR_INVALID_LCCN = "ERROR - INVALID LCCN STRING"  # the string is not a valid LCCN according to permalinks rules (like, '1234' returns an invalid response for being too short)
    ERROR_INVALID_XML = "ERROR - INVALID XML RETURNED"  # Data was returned but the XML could not be parsed (incomplete or corrupted) - very rare but sometimes happens with systems issues. Script will attempt to retry this error 10 times before bailing out
    ERROR_FAILED_REQUEST = "ERROR - FAILED TO RETRIEVE RECORD"  # Neither a record nor a valid Not Found could be returned, likely from a system outage. Script will attempt to retry this 10 times and wait progressively longer each time, but if the system is fully down it will move on
    ERROR_MULTIPLE_RESULTS = "ERROR - MULTIPLE RECORDS RETURNED"  # a MARCXML collection was returned instead of a single record, most likely because the LCCN is in another record 010z. Currently the script treats this as an error even if one of those was valid. This could be changed but since P1 also fails on ETL for an 010z just treating it as an error for now
    ERROR_GENERAL = "ERROR - GENERAL"
    MARCXML_ERRORS = {
        "record_not_found": ERROR_RECORD_NOT_FOUND,
        "invalid_lccn": ERROR_INVALID_LCCN,
        "invalid_xml": ERROR_INVALID_XML,
        "failed_request": ERROR_FAILED_REQUEST,
        "multiple_results": ERROR_MULTIPLE_RESULTS,
        "general": ERROR_GENERAL,
    }
    if config is None:
        config = Config()
    if session is None:
        session = requests.Session()

    marcxmlurl = f"https://lccn.loc.gov/{lccn}/marcxml"
    config.logger.debug("Pulling MARCXML: %s", marcxmlurl)

    is_blocked, result = _pull_marcxml(
        marcxmlurl,
        lccn,
        MARCXML_ERRORS,
        is_blocked=is_blocked,
        session=session,
        config=config,
    )  # TODO shift is_blocked to config

    # Error - MARC XML not successfully fetched
    if (isinstance(result, str)) & (str(result).startswith("ERROR - ")):
        return is_blocked, result  # return error message string
    # Success, further QC needed
    elif isinstance(result, ET.Element):
        root = result
        try:
            check_multi = root.findall("{http://www.loc.gov/MARC21/slim}record")
            # Currently this returns an error for any return of multiple results
            # Theoretically, it should check and identify if one or more have 010a, ignoring 010z
            # However, P1 ETL breaks with 010z anyway so maybe need to know that
            if check_multi is not None and len(check_multi) > 1:
                config.logger.error(ERROR_MULTIPLE_RESULTS)
                return ERROR_MULTIPLE_RESULTS
        except Exception as e:
            config.logger.error("Unknown error parsing XML with ElementTree: %s", e)
            return is_blocked, ERROR_GENERAL

        config.logger.info("Record found for: %s", lccn)
        if beautifulsoup is True:
            config.logger.debug()
            xml_string = ET.tostring(root, encoding="unicode")
            soup = BeautifulSoup(xml_string, "xml")
            root = soup
        return is_blocked, root
    else:
        config.logger.error("Unknown error fetching or parsing XML: %s", marcxmlurl)
        return is_blocked, ERROR_GENERAL


def marcxml_to_sdf(xml_data: str, config=None) -> pd.DataFrame:
    """
    Takes MARC XML data as a string and outputs a simplified version of the
    the dataframe than marcxml_to_df() would output. Blank indicators are shown as "_"
    and subfield labels (e.g., "$a") are included as a single string with values. This
    way, the CSV appears similar to how a MARC record might render in the Catalog "MARC
    tags" interface.

    Inputs:
     - xml_data (str): XML data, as a string
     - config (classes.config.Config): Config object.

    Returns:
     - pd.DataFrame: Dataframe representing the MARC XML data. One row per record
        (usually one row)

    """
    if config is None:
        config = Config()

    config.logger.debug("Converting MARC XML to dataframe . . .")

    try:
        marcxml_file = StringIO(xml_data)
        reader = pymarc.parse_xml_to_array(marcxml_file)
    except Exception as e:
        config.logger.error(
            "pymarc could not read data as XML. Returning blank dataframe. Message: %s",
            e,
        )
        return pd.DataFrame()

    try:
        rows = []
        for record in reader:
            record_dict = record.as_dict()
            leader = record_dict["leader"]
            fields = record_dict["fields"]
            row = {"leader": leader}
            field_log = {}
            for field in fields:
                field_key = next(iter(field))
                field_val_raw = field[field_key]
                if isinstance(field_val_raw, dict):
                    ind1 = field_val_raw.get("ind1")
                    if ind1 == " ":
                        ind1 = "_"
                    ind2 = field_val_raw.get("ind2")
                    if ind2 == " ":
                        ind2 = "_"
                    subfields = field_val_raw["subfields"]  # list of dictionaries
                    field_val = " ".join(
                        [ind1, ind2]
                        + [
                            f"${key} {value}"
                            for d in subfields
                            for key, value in d.items()
                        ]
                    )
                else:
                    field_val = field_val_raw
                if not field_log.get(field_key):
                    field_log[field_key] = 1
                else:
                    field_log[field_key] += 1
                row[f"{field_key}_{field_log[field_key]}"] = field_val
            rows.append(row)
        df = pd.DataFrame(rows)

        # Sort columns
        first_col = "leader"
        other_columns = [col for col in df.columns if col != first_col]
        df = df[[first_col] + sorted(other_columns)]

        config.logger.debug("MARC XML converted to dataframe.")
        return df
    except Exception as e:
        config.logger.error(
            "Could not parse the pymarc data. Returning blank dataframe. Message: %s",
            e,
        )
        return pd.DataFrame()


def marcxml_to_df(xml_data: str, config=None) -> pd.DataFrame:
    """
    Takes MARC XML data as a string and outputs a single-row dataframe (assuming
    the XML is a single record).

    Inputs:
     - xml_data (str): XML data, as a string
     - config (classes.config.Config): Config object.

    Returns:
     - pd.DataFrame: Dataframe representing the MARC XML data. One row per record
        (usually one row)

    """
    if config is None:
        config = Config()

    single_value_fields = ["001", "005", "008"]

    config.logger.debug("Converting MARC XML to dataframe . . .")

    try:
        marcxml_file = StringIO(xml_data)
        reader = pymarc.parse_xml_to_array(marcxml_file)
    except Exception as e:
        config.logger.error(
            "pymarc could not read data as XML. Returning blank dataframe. Message: %s",
            e,
        )
        return pd.DataFrame()

    try:
        rows = []
        for record in reader:
            record_dict = record.as_dict()
            leader = record_dict["leader"]
            fields = record_dict["fields"]
            row = {"leader": leader}
            for field in fields:
                field_key = next(iter(field))
                field_val = str(field[field_key])
                if field_key in single_value_fields:
                    row[field_key] = field_val
                elif not row.get(field_key):  # add field as list, if not present
                    row[field_key] = [field_val]
                else:  # append to field value, if already present
                    row[field_key].append(field_val)
            rows.append(row)
        df = pd.DataFrame(rows)
        config.logger.debug("MARC XML converted to dataframe.")
        return df
    except Exception as e:
        config.logger.error(
            "Could not parse the pymarc data. Returning blank dataframe. Message: %s",
            e,
        )
        return pd.DataFrame()


def _pull_marcxml(
    marcxmlurl, lccn, marcxml_errors, is_blocked=False, session=None, config=None
):
    """
    Functionality: Used by get_marcxml_record to pull a record from permalink.
    Should not be called directly - use get_marcxml_record
    Parameters:
     - marcxmlurl (str):
     - lccn (str):
    Returns:
     - XML from permalink, error from MARCXML_ERRORS, or None
        None return will trigger retries up to MARCXML_RETRIES times
    """
    if config is None:
        config = Config()
    try:
        is_blocked, result = make_request(
            marcxmlurl, session=session, is_blocked=is_blocked
        )
        if (isinstance(result, str)) & (str(result).startswith("ERROR - ")):
            return is_blocked, result  # return error message

        # Decode
        xmlstr = result.content.decode("utf-8")

        # No record found by that LCCN
        if "<error>Record not found" in xmlstr:
            return is_blocked, marcxml_errors["record_not_found"]

        # LCCN is not within allowable size or characters for an LCCN
        if "Library of Congress LCCN Permalink Error " in xmlstr:
            return is_blocked, marcxml_errors["invalid_lccn"]

        tree = ET.ElementTree(ET.fromstring(xmlstr))
        root = tree.getroot()

        # Very rarely, permalink may return the wrong record. This checks that the LCCN matches the requested LCCN
        # In the case of multiple records, this will pass if any record has the 010a but still return the collection
        for marc_010a in root.findall(
            ".//{http://www.loc.gov/MARC21/slim}datafield[@tag='010']/{http://www.loc.gov/MARC21/slim}subfield[@code='a']"
        ):
            if marc_010a.text.replace(" ", "") == lccn:
                return is_blocked, root

        # No match found by LCCN in 010a, or it is not a valid MARCXML record
        return is_blocked, marcxml_errors["general"]

    # Both of these errors are likely due to transitory issues with permalink that may work on a retry, or work later
    except ET.ParseError as e:
        print(f"Error while parsing XML: {e}")
        return is_blocked, marcxml_errors["invalid_xml"]


def get_marc_field(df: pd.DataFrame, field: str, config=None) -> pd.DataFrame:
    """
    Takes the output of `get_marc_df()` and a field, and returns a new dataframe
    with one row per value from that field. Output columns include: field_name,
    field_value, lccn, item_id, 001

    Inputs:
     - df (pd.DataFrame): Pandas dataframe, output of get_marc_df()
     - field (str): field (e.g., '985') to isolate and parse
     - config (classes.config.Config): Config object.

    Returns:
     - pd.DataFrame: Dataframe with one row per instance of requested field.
     Columns will be split into subfields and indicators. For repeat instances of
     the same subfield, these will appear numbered from 1 as in "subfield.a_1",
     "subfield.a_2"

    """
    if config is None:
        config = Config()

    if field not in df.columns:
        config.logger.error(
            "%s is not a column in your dataframe. Returning a blank dataframe."
        )
        return pd.DataFrame()

    config.logger.info("Creating a dataframe for field %s . . . ", field)

    try:
        df_exploded = df[["item_id", "lccn", "001", field]].explode(
            field, ignore_index=True
        )
        config.logger.debug("DF exploded into one row per 540 entry")

        df_split = pd.json_normalize(df_exploded[field].apply(flatten_json))

        # Flatten_json can create an empty column named "" that doesn't drop easily.
        # These two lines will remove it.
        if "" in df_split.columns:
            if df_split[""].isna().all():
                df_split = df_split[
                    [column for column in df_split.columns if column != ""]
                ]

        df_cleaned_cols = _rearrange_marc_cols(df_split)

        # df_split.columns = [
        #     re.sub(r"subfields\.\d+\.(.+)", r"\1", column)
        #     for column in df_split.columns
        # ]
        # df_split = df_split.fillna("None")
        # df_split = df_split.replace(" ", None)
        # df_split = df_split.replace({float("nan"): None})
        # config.logger.debug("DF %s column JSON normalized", field)

        # df_lists = pd.DataFrame()
        # for col in sorted(set(df_split.columns)):
        #     df_lists[col] = list(df_split[col].values)

        # if "ind2" in df_lists.columns:
        #     # Move ind2 to front of columns
        #     ind2 = df_lists["ind2"]
        #     df_lists.drop(columns="ind2", inplace=True)
        #     df_lists.insert(0, "ind2", ind2)
        # if "ind1" in df_lists.columns:
        #     # Move ind1 to front of columns
        #     ind1 = df_lists["ind1"]
        #     df_lists.drop(columns="ind1", inplace=True)
        #     df_lists.insert(0, "ind1", ind1)

        # config.logger.debug("DF multi-instance subfields combined into lists")
        config.logger.debug("Dataframe created for field %s", field)
        df_final = pd.concat([df_exploded, df_cleaned_cols], axis=1)
        return df_final
    except Exception as e:
        breakpoint()
        config.logger.error(
            "Encountered an error parsing field %s. Message: %s", field, e
        )


def _rearrange_marc_cols(df):
    # Group columns by the suffix (i.e., the last part of the name after '.')
    suffix_groups = {}
    subfields = [column for column in df.columns if "subfield" in column]
    for col in subfields:
        suffix = col.split(".")[-1]
        if suffix not in suffix_groups:
            suffix_groups[suffix] = []
        suffix_groups[suffix].append(col)

    # For each group of columns with the same suffix, combine the values
    for suffix, cols in suffix_groups.items():

        new_values = [
            [value for value in row if value is not None] for row in df[cols].values
        ]
        new_cols_df = pd.DataFrame(new_values)

        # for each of the old cols in `cols`, locate their index.
        col_indxs = []
        for col in cols:
            col_indx = df.columns.get_loc(col)
            col_indxs.append(col_indx)
        # choose the largest index number
        insert_at = max(col_indxs)
        # for each new col in new_cols, insert it starting at that largest index number and incrementing up for each added col
        for col in new_cols_df.columns:
            df.insert(insert_at, f"subfield.{suffix}_{col+1}", new_cols_df[col])
            insert_at += 1
        # drop both cols from `cols`
        df = df.drop(columns=cols)
    return df
