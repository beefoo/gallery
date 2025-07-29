# pylint: disable=broad-except

"""
General helper functions
"""

# System Libraries
import difflib
import os
from pathlib import Path
import re
import sys
import time
import urllib
import mimetypes
import pandas as pd
import requests
from typing import Union


# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries
from locgov_data.classes.config import Config


def _locgov_timedout(locgov_record, config=None) -> Union[bool, None]:
    """
    Checks a loc.gov record to see if a timeout occurred and the
    results are partial.
    Returns True/False for timeout encountered, or None if the check could
    not be performed.
    """
    if config is None:
        config = Config()
    if not isinstance(locgov_record, dict):
        config.logger.warning(
            "_search_timedout() was called, but the input was not a properly "
            "formatted dictionary. Skipping the requested check for loc.gov "
            "timeout."
        )
        return None
    is_partial = locgov_record.get("options.is_partial")
    if not is_partial:
        return False
    elif is_partial is True:
        return True
    else:
        return False


def express_search(
    search_url: str,
    c="",
    n=0,
    at="results,pagination,options.is_partial",
    headers=None,
    session=None,
    only_items=True,
    config=None,
) -> list:
    """
    Get all results for a loc.gov search URL, iterating through pages of results.

    Optionally can filter out non-items from results (e.g., events, collection
    pages, research centers).

    Optionally can return only the top n results, sorted by relevance.

    Each page is checked for a timeout error. If a timeout is encountered, the
    script waits 5 seconds and re-requests the page.

    Loc.gov cuts off searches after 100,000 results. Thus, if the search returns
    more than 100,000 results, the script will pause and ask the user if they want
    to continue and capture only the available results. The script will not continue
    without user input.

    If search result pagination is cut off and fewer results are returned than
    expected, an error is logged and printed to the terminal. Script continues.

    Note: If the `c` parameter is too large, JSON response may be cut off and
    non-valid.

    This function is based on work from the Library of Congress's Data
    Transformation Services experiment code, developed by AVP (weareavp.com).

    Inputs:
     - search_url (str) : Search query, such as https://loc.gov/search/?q=knickerbocker
     - c (str or int): Number of results per page. Max is 1000, as described at
        https://www.loc.gov/apis/json-and-yaml/working-within-limits/, but large
        requests are not very performative and the JSON may be cut off. Default
        is blank, which will fall back to the default set by loc.gov (which
        varies by format/collection).
     - n (int): If an integer greater than 0 is supplied, only the top n results will be
        fetched. A zero will return all results.
     - at (str): Will return only the listed keys of the requested record. If a key
        doesn't exist, loc.gov skips that key without error. Default:
        "results,pagination"
     - headers (dict): HTTPS headers including User_Agent. Default becomes {}.
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
     - only_items (bool): Whether to filter out non-items. Default = True
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - List of search results, e.g., 'results'
           list from https://www.loc.gov/search/?q=cook-book&fo=json&at=results
     - Returns empty list if errors or no results, with error message.

    Example usage:
        express_search(
            "https://www.loc.gov/search/?q=cook-book",
            c=200,
            headers={"User_Agent":"myemail@loc.gov"},
        )
    """
    if config is None:
        config = Config()
    if session is None:
        session = requests.Session()

    # Make API calls requesting new pages of results until there's a bad response
    results = []
    params = {"fo": "json", "at": at, "c": str(c)}
    if headers is None:
        headers = {}
    if config.user_agent != "":
        headers.update({"User-Agent": config.user_agent})
    readable_params = "&".join([f"{key}={value}" for key, value in params.items()])
    readable_url = f"{search_url}?{readable_params}"
    config.logger.info("Search query: %s", readable_url)

    is_blocked = False
    i = 1

    # Paginated requests
    while True:
        # Log the request
        params.update({"sp": i})
        readable_params = "&".join([f"{key}={value}" for key, value in params.items()])
        readable_url = f"{search_url}?{readable_params}"
        config.logger.info("Requesting page %s (%s)...", i, readable_url)

        # Make the request
        is_blocked, response = make_request(
            search_url,
            params=params,
            headers=headers,
            session=session,
            locgov_json=True,
            is_blocked=is_blocked,
            config=config,
        )

        # If first page, note the total number of expected pages and results.
        if i == 1:
            try:
                records_expected = response["pagination"]["of"]
                config.logger.info(
                    "Page %s indicates %s total records to collect.",
                    i,
                    records_expected,
                )
                if records_expected > 100000:
                    message = (
                        f"Your search has {records_expected} results, which "
                        "is above the system limit of 100,000 documented at"
                        "https://www.loc.gov/apis/json-and-yaml/working-within-limits/."
                    )
                    config.logger.error(message)
                    proceed = (
                        input(
                            "Do you wish to proceed anyway? "
                            "Type 'Y' to proceed or 'N' to skip search: "
                        )
                        .strip()
                        .upper()
                    )
                    if proceed != "Y":
                        return []
            except Exception as e:
                records_expected = 0
                config.logger.error(
                    "Could not parse expectd record count from pagination "
                    "section. Error message: %s",
                    e,
                )
        # Loop stops requesting pages when it hits an error. For too-long
        # searches, this will happen early.
        if str(response).startswith("ERROR - "):
            records_collected = len(results)

            # If search was cut off early, logs an error and exits loop.
            if (records_collected < records_expected) & (records_expected != 0):
                config.logger.error(
                    "%s results expected, but was only able to collect %s. Search: %s",
                    records_expected,
                    records_collected,
                    search_url,
                )
            break

        # Add this page's results to the list of results
        try:
            results.extend(response["results"])
        except:
            config.logger.warning(
                "Due to unknown error, failed to record results from page %s: %s",
                i,
                readable_url,
            )

        # If n results have been collected, stop loop. We'll add a buffer for now, to account for
        # framework pages that will be removed in a future step.
        buffer = 10
        if (n > 0) & (len(results) >= n + buffer):
            config.logger.info(
                "Search haulting after gathering the requested number of results (plus buffer of %s): %s",
                buffer,
                n,
            )
            break

        # Typically loop stops here, based on info from the pagination section
        try:
            pages_expected = response["pagination"]["total"]
            if i >= pages_expected:
                break
        except Exception as e:
            config.logger.error(
                "Could not parse expected page count from pagination section. Error message: %s",
                e,
            )
        i += 1

    # Filter out non-items (optional)
    before_removing = len(results)
    if only_items is True:
        config.logger.info(
            "Returning only loc.gov items. Filtering out remainder . . ."
        )
        results = [
            r for r in results if "/item/" in r["url"] or "/resource/" in r["url"]
        ]
        after_removing = len(results)
    else:
        after_removing = before_removing
    num_removed = before_removing - after_removing

    # If n > 0, clip to n rows. Also warn user if the total available results was less than n.
    if n > 0:
        if len(results) < n:
            config.logger.warning(
                "The top %s results were requested, but the search resulted in only %s results after filtering. No further results will be cut.",
                n,
                len(results),
            )
        elif len(results) > n:
            results = results[0:n]

    # Log results
    if n > 0:
        config.logger.info("Capped search results at user-requested limit: %s", n)
    config.logger.info(
        "Collected %s results of %s available. Applied user limit of %s results. Further removed %s non-item/resources. Final total: %s. ",
        before_removing,
        records_expected,
        n,
        num_removed,
        len(results),
    )
    if len(results) == 0:
        config.logger.error("Search returned no results: %s", readable_url)

    # Returns list of dictionaries
    return results


def make_request(
    url: str,
    params=None,
    headers=None,
    session=None,
    max_attempts=10,
    timeout=60,
    pause=5,
    locgov_json=False,
    json=False,
    is_blocked=False,
    config=None,
) -> tuple[bool, Union[str, bytes, list, dict]]:
    """
    Makes a request for a URL.

    Inputs:
     - url (str): URL, to request.
     - params (dict): Dictionary of parameters to pass to requests.get(). Used mostly
        for API requests.
     - headers (dict): HTTP headers to pass with request, such as User-Agent. Library
        of Congress asks that, whenever possible, requests to Library of Congress URLs should
        include a contact email address in the User-Agent value. Note that
        "DCMS/locgov-api-python (locgov_data)" will be appended to the end of all
        User-Agent strings. For example, if you enter headers = {"User-Agent":"myname@gmail.com"},
        the User-Agent value will be "myname@gmail.com DCMS/locgov-api-python (locgov_data)".
        The string "DCMS/locgov-api-python (locgov_data)" lets Library of Congress know that the
        traffic is originating from this library, locgov_data.
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
     - max_attempts (int): Maximum number of attempts before returning a general
        error. Default = 10.
     - timeout (int): Number of seconds before requets.get() times out.
     - pause (int): Number of baseline seconds between requests. This will increase
        on retries. If a pause is passed in the config, this pause will be overwritten
        by the config one.
     - locgov_json (bool): True means that the response is a loc.gov JSON record
     - other_json (bool): True means that the response is a JSON record (this is
        ignored if locgov_json = True)
     - is_blocked (bool): True means that the server has already returned a 429.
        This is for use in loops where you'd like to hault all requests in the
        event of a 429 status code.
     - config ([None, classes.general.Config]): Config object.

    Returns:
     A tuple of two values:
     - is_blocked (bool): True means that the server has already returned a 429.
        This is for use in loops where you'd like to hault all requests in the
        event of a 429 status code.
     - result
        - JSON-like object if locgov_json or json are True (i.g., dict
            or list);
        - binary if successful and not anticipating JSON; or
        - a string error message beginning "ERROR -".

    Error handling:
        - Pause and retry:
            - Network/DNS issue (requests.get() error)
            - status_code 5##
            - loc.gov JSON with 'status' 5##
            - loc.gov JSON partial record (e.g., solr timeout)
        - Returns `False, "ERROR - NO RECORD"`:
            - status code 404
        - Returns `False, 'ERROR - INVALID JSON'`:
            - invalid loc.gov JSON
            - invalid JSON
        - Returns `False,'ERROR - GENERAL'`:
            - on final attempt, returns 5## status_code or loc.gov JSON 'status'.
            - on final attempt, requests.get() error occurs
            - status code 403
        - Returns `True, 'ERROR - BLOCKED'`:
            - status_code 429 (too many requests, blocked by server)
    """
    if config is None:
        config = Config(pause=pause)
    pause = config.pause
    if params is None:
        params = {}
    if session is None:
        session = requests.Session()
    if headers is None:
        headers = {
            "User-Agent": "Library of Congress (DCMS/locgov-api-python locgov_data)"
        }
    elif not isinstance(headers, dict):
        config.logger.error(
            "The submitted header value is not a dictionary (%s). The input will be ignored",
            headers,
        )
        headers = {
            "User-Agent": "Library of Congress (DCMS/locgov-api-python locgov_data)"
        }
    elif not headers.get("User-Agent"):
        headers["User-Agent"] = (
            "Library of Congress (DCMS/locgov-api-python locgov_data)"
        )
    else:
        headers["User-Agent"] = (
            f"Library of Congress (DCMS/locgov-api-python locgov_data) {headers['User-Agent']}"
        )
    i = 0
    no_record = False, "ERROR - NO RECORD"  # URLs that don't exist
    invalid = False, "ERROR - INVALID JSON"  # invalid JSON
    blocked = True, "ERROR - BLOCKED"  # after receiving a 429
    error = False, "ERROR - GENERAL"  # value to return for all other errors
    if is_blocked is True:
        config.logger.error("Blocked due to too many requests. Skipping {url} {params}")
        return error
    while i < max_attempts:
        iter_pause = pause * (i + 1)
        retry_msg = f"Trying again in {iter_pause} seconds . . ."
        if i > 0:
            config.logger.info(retry_msg)
        time.sleep(iter_pause)
        message = f"Making request. Attempt #{i+1} for: {url} {params}"
        config.logger.info(message)

        try:
            response = session.get(url, params=params, timeout=timeout, headers=headers)
        except ConnectionError as e:
            config.logger.error("Connection error (%s): %s %s.", e, url, params)
            i += 1
            continue
        except requests.exceptions.Timeout:
            config.logger.error(
                "Timeout limit reached: %s seconds, %s.",
                timeout,
                url,
            )
            i += 1
            continue
        except Exception as e:
            config.logger.error("requests.get() failed (%s): %s %s.", e, url, params)
            i += 1
            continue

        # 429 too many requests
        if response.status_code == 429:
            message = f"Too many requests (429). Skipping: {url} {params}."
            config.logger.critical(message)
            return blocked

        # 500 - 599 server error
        elif (500 <= response.status_code) & (600 > response.status_code):
            message = f"Server error ({response.status_code}): {url} {params}."
            config.logger.info(message)
            i += 1
            continue

        # 403 forbidden
        elif response.status_code == 403:
            message = f"Forbidden request (403). Skipping: {url} {params}."
            config.logger.error(message)
            return error

        # 404 doesn't exist
        elif response.status_code == 404:
            ## If 404 partial results issue re-arrises:
            # use the commented code below, instead of the following 3 lines
            # if (i <= 2) and (
            #     locgov_json is True
            # ):
            #     message = (
            #         f"Received 404 status_code (locgov_json is True, "
            #         f"trying another time): {url} {params}"
            #     )
            #     config.logger.error(message)
            #     i += 1
            #     continue
            # else:
            #     message = f"Resource does not exist (404). Skipping: {url} {params}."
            #     config.logger.error(message)
            #     return no_record
            message = f"Resource does not exist (404). Skipping: {url} {params}."
            config.logger.error(message)
            return no_record

        # Verify JSON (if applicable)
        if locgov_json is True:
            try:
                output = response.json()
                status = str(output.get("status"))

                # loc.gov JSON 4##
                if status.startswith("4"):
                    if i > 2:  # only makes two attempts
                        config.logger.error(
                            "Resource does not exist (loc.gov %s). Skipping: %s %s.",
                            status,
                            url,
                            params,
                        )
                        return no_record
                    else:
                        config.logger.error(
                            "Received loc.gov JSON 404: %s %s", url, params
                        )
                        i += 1
                        continue

                # loc.gov JSON 5##
                elif status.startswith("5"):
                    config.logger.error(
                        "Server error (%s). Request for %s %s", status, url, params
                    )
                    i += 1
                    continue

                # loc.gov valid JSON
                else:
                    # Check for locgov partial results (e.g. solr timeout)
                    locgov_timeout = _locgov_timedout(output, config=config)
                    if locgov_timeout == True:
                        config.logger.error(
                            "Loc.gov partial record received (likely solr timeout). Request for %s %s",
                            url,
                            params,
                        )
                        i += 1
                        config.logger.info(
                            "Pausing an extra 1 minute due to search time out, to allow cache to catch up . . ."
                        )
                        time.sleep(60)
                        continue

                    # Success, return the loc.gov JSON record
                    config.logger.info("Successful request (loc.gov JSON)")
                    # Proceed to RETURN SUCCESSFUL RESULT

            # loc.gov invalid JSON
            except Exception as e:
                config.logger.error("INVALID JSON (%s): %s %s", e, url, params)
                return invalid
        elif json is True:
            try:
                output = response.json()
                message = "Successful request (JSON)"
                config.logger.info(message)
                # Proceed to RETURN SUCCESSFUL RESULT
            except Exception as e:
                message = f"INVALID JSON ({e}): {url} {params}"
                config.logger.error(message)
                return invalid
        else:
            output = response
            message = "Successful request"
            config.logger.info(message)
            # Proceed to RETURN SUCCESSFUL RESULT

        # RETURN SUCCESSFUL RESULT
        return False, output

    # If hits max attempts, return general error
    return error


def flatten_json(
    record: Union[dict, list],
    donotparse=None,
    donotparse_regex=None,
    parse_lists=True,
    prefix="",
    new_record=None,
    config=None,
) -> dict:
    """
    Flatten/unfold a JSON or JSON-like object.

    Inputs:
     - record (Union[dict, list]): JSON-like dictionary or list.
     - donotparse (list): List of keys to not flatten, at any level of the input record.
     - donotparse_regex (list): List of regular expressions. Any keys matching these expressions
        will not be flattened. Use this field for any nested keys where you need to
        specify parent/child combos.
     - parse_lists (bool): Whether or not to expand lists. E.g., split some_list into
        columns like some_list_1, some_list_2, etc.
     - prefix (str): Used when looping through levels. Do not pass any values
        to this parameter.
     - new_record: Used when looping through levels. Do not pass any values
        to this parameter.
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - Union[dict, list]: Flattened version of the input record.

    """
    try:
        if config is None:
            config = Config()
        if donotparse is None:
            donotparse = []
        if donotparse_regex is None:
            donotparse_regex = []
        # preview_len = min(100, len(str(record)))
        # preview = str(record)[0:preview_len]
        # config.logger.debug("Parsing dictionary: %s . . .", preview)
        if new_record is None:
            new_record = {}
        if any([re.match(pattern, prefix) for pattern in donotparse_regex]):
            config.logger.debug("Stopat pattern encountered, stopping here.")
            new_record[prefix] = record
        elif isinstance(record, dict):
            # config.logger.debug("Parsing input as a dict: %s . . . ", preview)
            for key, value in record.items():
                # config.logger.debug("Parsing key: %s . . . ", key)
                if key in donotparse:
                    # config.logger.debug("Key is on donotparse list")
                    new_key = key if prefix == "" else f"{prefix}.{key}"
                    new_record[new_key] = value
                elif isinstance(record[key], list):
                    if parse_lists is True:
                        # message = f"Skipping. Key is a list: {record[key]}"
                        # config.logger.debug(message)
                        for i, item in enumerate(value):
                            item_prefix = (
                                f"{prefix}.{key}.{i}" if prefix else f"{key}.{i}"
                            )
                            new_record = flatten_json(
                                record=item,
                                prefix=item_prefix,
                                new_record=new_record,
                                donotparse=donotparse,
                                donotparse_regex=donotparse_regex,
                                parse_lists=parse_lists,
                                config=config,
                            )
                    else:
                        # config.logger.debug(
                        #     "Key is a list and we are not parsing lists."
                        # )
                        new_record[key] = value
                elif isinstance(record[key], dict):
                    # config.logger.debug("Key is a dictionary. Parsing . . .")
                    for subkey in value:
                        # config.logger.debug("Parsing subkey: %s", subkey)
                        sub_prefix = (
                            f"{prefix}.{key}.{subkey}"
                            if prefix != ""
                            else f"{key}.{subkey}"
                        )
                        # config.logger.debug(
                        #     "Running subkey %s through flatten_json() . . . ", subkey
                        # )
                        new_record = flatten_json(
                            record=value[subkey],
                            prefix=sub_prefix,
                            new_record=new_record,
                            donotparse=donotparse,
                            donotparse_regex=donotparse_regex,
                            parse_lists=parse_lists,
                            config=config,
                        )
                else:
                    new_key = key if prefix == "" else f"{prefix}.{key}"
                    new_record[new_key] = value
        elif isinstance(record, list):
            # config.logger.debug("Input is list: %s . . . ", preview)
            if parse_lists is True:
                for i, item in enumerate(record):
                    item_prefix = f"{prefix}.{i}" if prefix else str(i)
                    new_record = flatten_json(
                        record=item,
                        prefix=item_prefix,
                        new_record=new_record,
                        donotparse=donotparse,
                        donotparse_regex=donotparse_regex,
                        parse_lists=parse_lists,
                        config=config,
                    )
            else:
                new_record[prefix] = record
        else:
            new_record[prefix] = record

        return new_record
    except Exception as e:
        breakpoint()
        print(e)


def flatten_locgov(records: list, config=None) -> list:
    """
    Standard pattern for flattening loc.gov items for public data packages. Can accept
    full item records or search result items. Relies on `flatten_json()`.

    Does not expand lists. When converted to df, the list will remain in a single column.

    Does not expand dictionaries that are simply facet filter links (e.g., 'contributors')
        or the item.item dictionary.

    Does not expand the "files" section of "resources" key.

    Inputs:
     - records (list): List of loc.gov item records or search results.
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - list: List of flattened loc.gov item records or search results.

    """
    if config is None:
        config = Config()

    config.logger.debug("Flattening record . . .")
    flattened_results = []
    donotparse_regex = [r"resources.\d.files.\d+", r"item\.item\.*"]
    donotparse = [
        "contributors",
        "locations",
        "subjects",
        "partof",
        "more_list_this",
    ]
    for record in records:
        preview = str(record)[0 : min(100, len(str(record)))]
        try:
            flattened_result = flatten_json(
                record=record,
                donotparse=donotparse,
                donotparse_regex=donotparse_regex,
                parse_lists=False,
                config=config,
            )
        except Exception as e:
            config.logger.error(
                'Error countering while attempting to flatten JSON "%s" . . . . Message: %s',
                preview,
                e,
            )
        flattened_results.append(flattened_result)
    config.logger.info("Record successfully flattened.")
    return flattened_results


def df_to_csv(
    df: pd.DataFrame, csv_file: str, append=False, config=None, **kwargs
) -> bool:
    """
    Save dataframe to CSV.
    If the CSV already exists and `append` is True, the data is appended to
        existing CSV. If `append` is False, existing CSV is overwritten.
    If the CSV is open, user is prompted to close the CSV and try again,
        not save but continue with script, or quit the script.

    Inputs:
     - df (pd.DataFrame): Dataframe to save.
     - csv_file (str): Path to CSV output file.
     - append (bool): If the output CSV already exists, it will be updated with
        append new lines to the CSV. If the output CSV does not already exist,
        this parameter is disregarded.
     - config ([None, classes.general.Config]): Config object.
     - **kwargs: Any additional parameters will be input directly into the pandas
        .to_csv() function

    Returns:
     - bool: True if CSV file is saved/updated. False if data failed to save to CSV.

    """
    if config is None:
        config = Config()

    config.logger.debug("Saving to : %s ...", csv_file)

    # Check if the CSV file exists
    while True:
        try:
            if os.path.isfile(csv_file) and append:
                # If CSV exists and operator chooses to append, append new data.
                config.logger.debug("Exists, updating : %s ...", csv_file)
                old_df = pd.read_csv(csv_file)
                df.reset_index(drop=True)
                old_df.reset_index(drop=True)
                new_df = pd.concat([df, old_df], axis=0, ignore_index=True)
                new_df.to_csv(csv_file, mode="a", **kwargs)
                config.logger.info("Updated: %s ", csv_file)
                break
            elif os.path.isfile(csv_file):
                # If not, create/overwrite the CSV with new data.
                config.logger.debug("Overwriting : %s ...", csv_file)
                df.to_csv(csv_file, mode="w", **kwargs)
                break
            else:
                # If not, create/overwrite the CSV with new data.
                config.logger.debug("Creating : %s ...", csv_file)
                df.to_csv(csv_file, mode="w", **kwargs)
                break
        except PermissionError:
            # If permission error occurs, notify the user and prompt for retry
            config.logger.warning("Permission denied: Unable to save to %s.", csv_file)
            print("Please close the file if it is open.")

            # Prompt user to either retry or quit
            user_response = (
                input(
                    "Type 'Y' to try again, 'N' to not save but continue, 'Q' to quit script: "
                )
                .strip()
                .upper()
            )

            if user_response == "Y":
                config.logger.info("Retrying save to %s . . .", csv_file)
                time.sleep(2)  # Pause before retrying
                continue  # Retry saving the file
            elif user_response == "N":
                config.logger.error(
                    "Skipping save to file and continuing script: %s", csv_file
                )
                return False
            elif user_response == "Q":
                config.logger.critical(
                    "User opted to quit the script due to permission error saving to: %s",
                    csv_file,
                )
                exit()
            else:
                print("Invalid input. Please type 'Y', 'N', or 'Q'.")
        except Exception as e:
            config.logger.error("Problem saving df to file. Message: %s", e)
            return False
    config.logger.info("Saved: %s", csv_file)
    return True


def download_file(
    url,
    dest,
    headers=None,
    is_blocked=False,
    overwrite=False,
    timeout=60 * 30,  # 30 minutes by default
    get_filesize=False,
    session=None,
    config=None,
) -> tuple[bool, bool]:
    """
    General function to download a file.
    Optional: First makes a HEAD request to get the filesize.

    Inputs:
     - url (str): URL to download.
     - dest (str): Local filepath to save file.
     - headers (dict): Dictionary of http request headers. Intended especially for
        use with User-Agent, which can be a key in the `headers` dictionary.
     - is_blocked (bool): True means that the server has already returned a 429.
        This is for use in loops where you'd like to hault all requests in the
        event of a 429 status code.
     - overwrite (bool): Overwrite existing files at destination paths.
     - timeout (int): Max number of seconds to wait for each download. Note that the
        source server may set a lower limit.
     - get_filesize (bool): Whether to make a HEAD request to get the filesize before
        downloading.
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
     - config ([None, classes.general.Config]): Config object.


    Returns
     - tuple[bool, bool] - is_blocked, which indicates if the request received a 429
        response, and a second boolean indicating if the download was successful.
    """
    if config is None:
        config = Config()
    if headers is None:
        headers = {}
    if session is None:
        session = requests.Session()
    if not isinstance(timeout, int):
        message = (
            f"The supplied timeout ({timeout}) is not an integer. Changing to: 30 min."
        )
        config.logger.warning(message)
        timeout = 60 * 30

    # Destination exists and shouldn't be overwritten - don't attempt download
    if Path(dest).is_file() and (overwrite is False):
        config.logger.error(
            "Could not download %s. Destination file already exists and `overwrite` is False: %s",
            url,
            dest,
        )
        return is_blocked, False
    message = f"Downloading: {url} (timeout: {timeout/60} min) . . ."
    config.logger.info(message)
    if get_filesize:
        try:
            head_response = requests.head(url, timeout=60)
            if "Content-Length" in head_response.headers:
                file_size = int(head_response.headers["Content-Length"])
                message = f"File size: {file_size} bytes"
                config.logger.info(message)
        except:
            config.logger.info("Could not determine filesize. ")

    is_blocked, response = make_request(
        url,
        headers=headers.update({"User-Agent": config.user_agent}),
        session=session,
        timeout=timeout,
        is_blocked=is_blocked,
        config=config,
    )
    if not str(response).startswith("ERROR - "):
        try:
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                f.write(response.content)
            config.logger.info("File downloaded: %s", dest)
            return is_blocked, True
        except Exception as e:
            config.logger.error("Skipped downloading. Error: %s", e)
            return is_blocked, False
    else:
        config.logger.error(
            "Could not download %s. Encountered an error requesting record: %s",
            url,
            response,
        )
        return is_blocked, False


def download_from_df(
    df: pd.DataFrame,
    src="src",
    dest="dest",
    timeout=60 * 30,
    overwrite=False,
    session=None,
    config=None,
) -> dict:
    """
    Download files from a dataframe with default columns 'src' and dest'

    Inputs:
     - df (pd.DataFrame): Dataframe that has a source column with URLs
        and destination column with paths to save files.
     - src (str): Name of source column with URLs
     - dest (str): Name of destination column with file paths to which files will be saved.
     - timeout (int): Max number of seconds to wait for each download. Note that the
        source server may set a lower limit.
     - overwrite (bool): Overwrite existing files at destination paths.
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - dict: Two keys, `downloaded` and `skipped`. Values are lists of
        download URLs.
    """
    if config is None:
        config = Config()
    if session is None:
        session = requests.Session()
    downloaded_files = []
    skipped_files = []
    is_blocked = False
    if src not in df.columns:
        message = (
            f"Dataframe does not have '{src}' column, which is required to "
            "download files. Skipping file download."
        )
        config.logger.error(message)
        return {}
    if dest not in df.columns:
        message = (
            f"Dataframe does not have a {dest} column, which is required to "
            "download files. Skipping file download."
        )
        config.logger.error(message)
        return {}
    for _, row in df.iterrows():
        src_url = row[src]
        dest_path = row[dest]

        if dest_path is not None:  # Skip rows where 'dest' is None
            is_blocked, downloaded = download_file(
                src_url,
                dest_path,
                is_blocked=is_blocked,
                timeout=timeout,
                overwrite=overwrite,
                session=session,
                config=config,
            )
            if downloaded is True:
                downloaded_files.append(
                    {"src": src_url, "dest": dest_path, "status": "downloaded"}
                )
            else:
                skipped_files.append(
                    {"src": src_url, "dest": dest_path, "status": "skipped"}
                )
        else:  # No destination defined, couldn't parse the src url
            config.logger.error(
                "Skipping %s. Destination path could not be parsed from URL.", src
            )
            skipped_files.append(
                {"src": src_url, "dest": dest_path, "status": "skipped"}
            )
    return {"downloaded": downloaded_files, "skipped": skipped_files}


def csv_to_df(csv_path, *args, config=None, **kwargs) -> Union[pd.DataFrame, None]:
    """
    Loads CSV file as a Pandas DataFrame.

    Inputs:
     - csv_path (str): Relative or absolute path to the input CSV file.
     - *args: Additional positional arguments passed to pd.read_csv().
     - config ([None, classes.general.Config]): Config object.
     - **kwargs: Additional keyword arguments passed to pd.read_csv().

    Returns:
     - pd.DataFrame: Dataframe if successful, None if file doesn't exist or
        otherwise unsuccesful.

    """
    if config is None:
        config = Config()

    if not os.path.exists(csv_path):
        message = f"Error: The file '{csv_path}' does not exist."
        config.logger.error(message)
        return None

    try:
        df = pd.read_csv(csv_path, *args, **kwargs)
        return df
    except Exception as e:
        config.logger.error(
            "An error occurred while reading the CSV file %s: %s", csv_path, e
        )
        return None


def df_to_chunks(df: pd.DataFrame, chunk_size, config=None) -> list:
    """
    Converts a df to a list of smaller dfs.

    Inputs:
     - df (pd.DataFrame): Input dataframe.
     - chunk_size (int): Size (in rows) of each chunk
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - list: List of dataframes.

    """
    if config is None:
        config = Config()

    try:
        chunks = [df.loc[i : i + chunk_size - 1] for i in range(0, len(df), chunk_size)]
        config.logger.debug(
            "DataFrame split into %s %s-size chunks", len(chunks), chunk_size
        )
        return chunks
    except Exception as e:
        config.logger.error(
            "Unknown problem splitting DataFrame into chunks for process."
            "Returning original DataFrame. Message: %s",
            e,
        )
        return df


def filter_dict(requested_keys: dict, input_dict: dict, config=None) -> dict:
    """
    Takes an input dictionary with a second dictionary to map selected keys to new
    keys. For example, if the `requested_keys` is {"s": "states", "c":"cities"} and
    the `input_dict` is {"s":["a", "b"], "c": ["a", "b"], "r": ["a"]}, the output
    would be {"states":["a", "b"], "cities": ["a", "b"]}.

    Inputs:
     - requested_keys (dict): Dictionary of requested keys, and mapped values for output
        dictionary. Keys are the source keys, values are the intended output keys.
     - input_dict (dict): Input dictionary to be filtered and mapped.
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - dict: Filtered dictionary with mapped keys.

    """
    if config is None:
        config = Config()

    if isinstance(input_dict, dict) is False:
        config.logger.error(
            "Cannot filter because not a dictionary: %s . . .", str(input_dict)[0:100]
        )
        return input_dict
    if isinstance(requested_keys, dict) is False:
        config.logger.error(
            "requested_keys must be a dictionary but a %s was received. Skipping filtering.",
            type(requested_keys),
        )
        return input_dict
    config.logger.debug("Filtering dictionary . . .")
    selected_fields_output = {}
    for key in requested_keys:  # e.g., "item.subjects"
        parts = requested_keys[key].split(".")  # e.g., ["item","subjects"]
        add_field = input_dict
        for part in parts:
            add_field = add_field.get(part)
        if add_field is not None:
            selected_fields_output.update({key: add_field})
        else:
            selected_fields_output.update({key: "MISSING FIELD"})
    config.logger.debug(
        "Done filtering dictionary: %s . . .", str(selected_fields_output)[0:100]
    )
    return selected_fields_output


def verify_mimetype(mimetype, config=None) -> bool:
    """
    Validates whether an input string is a mimetype. Uses mimetypes recognized by
     the `mimetypes` Python library to validate. If the input string is not
     recognized, difflib makes suggestions.

    Inputs:
     - mimetype (str): a string that may be a mimetype
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - True if confirmed a mimetype
     - False if not a confirmed mimetype

    """
    if config is None:
        config = Config()

    config.logger.debug("Verifying requested mimetype strings . . .")
    valid_mimetypes = []
    try:
        mimetype_data = []
        for type_list in [
            "https://www.iana.org/assignments/media-types/application.csv",
            "https://www.iana.org/assignments/media-types/audio.csv",
            "https://www.iana.org/assignments/media-types/image.csv",
            "https://www.iana.org/assignments/media-types/multipart.csv",
            "https://www.iana.org/assignments/media-types/text.csv",
            "https://www.iana.org/assignments/media-types/video.csv",
        ]:
            type_data = pd.read_csv(type_list)
            mimetype_data.append(type_data)
        valid_mimetypes = pd.concat(mimetype_data, axis=0)["Template"].to_list()
        config.logger.debug(
            "Verifying requested mimetype strings against IANA mimetype list."
        )
    except Exception as e:
        config.logger.error(
            "IANA mimetypes list could not be accessed. Falling "
            "back to `mimetype` python library for validating mimetype inputs. "
            "Manually adding image/jp2 to list."
            "Message: %s",
            e,
        )
        valid_mimetypes = mimetypes.types_map.values()
        valid_mimetypes.extend(["image/jp2"])

    if mimetype not in valid_mimetypes:
        near_matches = []

        # Find similar based on the entire input string
        near_whole = difflib.get_close_matches(
            mimetype, valid_mimetypes, n=2, cutoff=0.7
        )
        near_matches.extend(near_whole)

        # Find similar based on the second part of the input string, if possible (e.g., "jpg")
        pattern = (
            r"^[^/]+/([^/]+)$"  # Pattern matches a single "/" with chars on either side
        )
        match = re.search(pattern, mimetype)
        if match:
            subtype = f"/{match.group(0)}"
            near_subtype = difflib.get_close_matches(
                subtype, valid_mimetypes, n=1, cutoff=0.4
            )
            near_matches.extend(near_subtype)

        suggestions = ",".join(near_matches)

        config.logger.error(
            "The mimetype %s is not recognized by this script. Could "
            "you have meant: %s?",
            mimetype,
            suggestions,
        )
        return False
    config.logger.debug("Mimetype verified: %s", mimetype)
    return True


def is_url(url) -> bool:
    """
    Confirms if input is an http(s) URL. Returns boolean. False for ftp and s3 urls.
    Does not confirm if the URL is valid, only if it matches the pattern of a URL.

    Inputs:
     - url (str): String to be confirmed as a URL

    Returns:
     - bool: True if a URL, False if not.

    """
    parsed = urllib.parse.urlparse(url)
    return parsed.scheme in ["http", "https"] and bool(parsed.netloc)  # bool


def move_df_column(
    df: pd.DataFrame, column_name: str, target_index: int
) -> pd.DataFrame:
    """
    Moves the specified column to the given index position in the DataFrame.

    Inputs:
     - df (pd.DataFrame): The DataFrame to modify.
     - column_name (str): The name of the column to move.
     - target_index (int): The target index position to move the column to.

    Returns:
     - pd.DataFrame: The modified DataFrame with the column moved.

    """
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame.")
    cols = list(df.columns)
    cols.remove(column_name)
    cols.insert(target_index, column_name)
    df = df[cols]
    return df
