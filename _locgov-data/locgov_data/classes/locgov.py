# pylint: disable=wildcard-import
# pylint: disable=broad-exception-caught

"""
Functions and classes for loc.gov records
"""

# System libraries
import json
import os
from pathlib import Path
import re
import sys
import numpy as np
import pandas as pd
import requests
import xml.etree.ElementTree as ET


# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries
from locgov_data.helpers.general import (
    df_to_csv,
    make_request,
    csv_to_df,
    is_url,
    verify_mimetype,
    download_from_df,
    express_search,
    flatten_locgov,
    move_df_column,
)
from locgov_data.helpers.fulltext import altoxml_to_df
from locgov_data.classes.config import Config
from locgov_data.helpers.marcxml import marcxml_to_df, get_marcxml_record


class LocGovRecords:
    """Object for collecting a set of loc.gov data"""

    def __init__(
        self,
        input_type=None,
        input_path=None,
        output_dir="./output/",
        output_prefix="",
        c=100,
        user_agent=None,
        pause=None,
        env="prod",
        is_election=False,
        config=None,
    ):
        """
        Initialize the Search class with input type and path.
        For CSV input, item_id is a required CSV column and should include
        loc.gov item ids or URLs.

        Inputs:
         - c (int): Pagination parameter, the number of results to return per loc.gov
            search page. Defaults to loc.gov defaults, which vary by collection.
         - user_agent (str): User-Agent http header to be passed with all requests.
            Suggested staff usage is email address. Suggested usage for public
            scrips is the URL to the script.
         - input_type (str): Type of input, either 'csv' or 'search'.
         - input_path (str): Path to the CSV file or search URL. CSV must contain a
            column 'item_id' or 'resource_id'
         - output_dir (str): Directory to save outputs into.
         - output_prefix (str): Prefix for output files.
         - pause (int): Base number of seconds to wait between loc.gov requests.
         - env (str): Environment to operate in: prod (default), test, or dev. Only prod is publicly available.
         - is_election (bool): if this is a search for web archive US Election records.
            Facilitates retrieving and parsing web archive MODS files.
         - config ([None, classes.general.Config]): Config object.

        Returns:
         - locgov_data.LocGovRecords

        """
        if config is None:
            config = Config()

        self.config = config
        if input_type is not None:
            self.input_type = input_type.lower()
        self.output_dir = output_dir
        self.output_prefix = output_prefix
        self.input_type = input_type
        self.input_path = input_path
        self.pause = pause
        if self.pause is None:
            self.pause = config.pause
        self.env = env
        self.base = None
        self.is_election = is_election
        self.top_level_files = [  # TODO confirm with devs if this list is complete, see https://staff.loc.gov/tasks/browse/DIGS-79
            "fulltext_derivative",  # djvu and alto JSON file with page-level text and coordinates
            "text_file",  # alto full plain text unstructured
            "djvu_text_file",  # djvu full plain text unstructured -- only found in item.resources and not in 'resources'
            "djvu_xml_file",  # dju full xml file, when found in 'resources'. Equivalent to 'fulltext_file' found in in 'item.resources'
            "fulltext_file",  # djvu full plain text unstructured when found in 'resources', same as djvu_text_file found in item.resources. When found in item.resources it is the full Alto XML. e.g., https://www.loc.gov/item/07011895/?fo=json&at=item.resources,resources. Can also be a TEI xml file, as in https://www.loc.gov/item/raelbib000150/?fo=json&at=resources.0 and https://www.loc.gov/item/afc2001001.32482/?fo=json&at=resources.0
            "word_coordinates",  # unsure, may always be an error -- see https://www.loc.gov/item/sn86075271/1942-11-12/ed-1/?fo=json&at=resources.0.word_coordinates
            "image",  # thumbnail, usually gif or IIIF jpg. svg also seen e.g., http://www.loc.gov/item/2020706313/
            "pdf",  # PDF. Can be transcriptions, as in https://www.loc.gov/item/raelbib000150/?fo=json&at=resources.0
            "closed_captions",  # VTT as in https://www.loc.gov/item/2021690588/?fo=json&at=item.resources,resources
            "poster",  # only for videos? different format of the poster URL that is also on a segment for the first resource? e.g. https://www.loc.gov/item/00694010/?fo=json&at=item.resources,resources
            "video",  # always a duplicate of the video URL that also is a segment of the first resource? e.g. https://www.loc.gov/item/00694010/?fo=json&at=item.resources,resources
            "video_stream",  # only for non-collection events?
            "background",  # secondary thumbnail for selected videos (VHP)?
            "info",  # IIIF manifest for audio and videos? e.g., http://www.loc.gov/item/afc2001001.44565/?fo=json&at=item.resources,resources and https://www.loc.gov/item/afc2001001.95621/?fo=json&at=item.resources,resources
            "media",  # mp3 for some audio -- on item's item.resources and search's resources, but not an item's resources, e.g., https://www.loc.gov/item/jukebox-119347/?fo=json&at=item.resources,resources
            "audio",  # mp3 for some audio -- sometimes the same url as in the "media"  field. e.g., http://www.loc.gov/item/jukebox-119347/, https://www.loc.gov/item/afc1981004_afs20677/?fo=json&at=item.resources,resources
        ]
        self.errors = {
            "search": [],
            "items": [],
            "resources": [],
        }
        self.item_ids = pd.DataFrame()  # single-column dataframe of item ids
        self.resource_ids = (
            pd.DataFrame()
        )  # used only if input CSV is resource ids/urls OR search returns resource-level results
        self.segment_ids = (
            pd.DataFrame()
        )  # used only if search returns resource-level results with segment params (e.g., a Chronicling America search for full text returning http://www.loc.gov/resource/sn83045462/1929-02-03/ed-1/?sp=82)
        self.search_metadata_json = (
            []
        )  # item-level metadata from search query, unflattened JSON
        self.search_metadata = pd.DataFrame()  # item-level metadata from search query
        self.items = pd.DataFrame()  # item-level metadata from item records
        self.resources = pd.DataFrame()  # resource-level metadata from item records
        self.files_segments = pd.DataFrame()  # segment-level files from item records
        self.files_resources = (
            pd.DataFrame()
        )  # resource-level files, not part of any segment, from item records
        self.c = c  # search parameter c for items per page
        if user_agent is None:
            user_agent = config.user_agent
        if user_agent is None:  # if still none
            self.headers = {}
        else:
            self.headers = {"user_agent": user_agent}

        # populate item_ids automatically if input is csv
        if self.input_type == "csv":
            self._load_csv()

        self.is_blocked = False
        self.session = requests.Session()
        self.fulltext_alto_df = pd.DataFrame()
        self.fulltext_plaintext_df = pd.DataFrame()
        self.fulltext_tei_df = pd.DataFrame()
        self.fulltext_tei_df = pd.DataFrame()
        self.alto_word_df = pd.DataFrame()

        self._set_env()

    def _load_csv(self, dtype=None):
        """
        Load CSV file and initialize item_ids and resource_ids.

        Inputs:
         - dtype - If None, this parameter will be ignored. Otherwise, if a
            dtype is supplied it will be passed to pd.read_csv()

        Updates:
         - self.item_ids
         - self.resource_ids

        Returns:

        """
        input_df = csv_to_df(self.input_path, config=self.config)
        if input_df is None:
            self.config.logger.error("CSV could not be loaded")
            raise Exception(f"The CSV could not be loaded: {self.input_path}")
        elif "item_id" in input_df.columns:
            self.item_ids = input_df[["item_id"]]
        elif "resource_id" in input_df.columns:
            self.resource_ids = input_df[["resource_id"]]
        else:
            self.config.logger.error(
                "CSV file does not have item_id or resource_id column: %s",
                self.input_path,
            )

        self._set_env()

    def _set_env(self):
        """
        This function updates the environment to prod, dev, or test. It
        updates the search URL, if there is one, and updates item, resource, and segment IDs, if
        they have been collected. This can be run more than once during data harvesting.
        """
        self.env = str(self.env)
        # Update self.base and relevant dataframes.
        if self.env not in ["prod", "dev", "test"]:
            self.config.logger.error(
                "The input environment value is not one of prod, dev, or test. Prod will be used instead."
            )
            self.env = "prod"
        if self.env == "prod":
            self.base = "https://www.loc.gov/"
        else:
            self.base = f"https://{self.env}.loc.gov/"

        for df in [self.item_ids, self.resource_ids, self.segment_ids]:
            for column in ["item_id", "resource_id", "segment_id"]:
                if column in df.columns:
                    # Use a regex to replace the column with normalized URLs.
                    # However, if all values are empty, you need to account for the
                    # fact that pandas may treat the column differently. So,
                    # include logic to check if the entire column are NaN values (in
                    # which case the dtype will be float64 instead of object)
                    if df[column].dtype != float:
                        df[column] = df[column].str.replace(
                            r"https?://.*.loc.gov/", self.base, regex=True
                        )
        # Update self.input_path if it's a search URL
        if self.input_type == "search":
            pattern = r"https?://.*.?loc.gov/"
            self.input_path = re.sub(pattern, self.base, self.input_path)

    def _normalize_url_env(self, locgov_id, prefix="item/"):
        """
        Transforms IDs and URLs into standardized loc.gov URLs

        e.g., assuming requested environment is prod:

        _normalize_url_env("98687169, "item/") -> https://www.loc.gov/item/98687169/
        _normalize_url_env("http://loc.gov/item/98687169, "item/") -> https://www.loc.gov/item/98687169/
        _normalize_url_env("https://www.loc.gov/item/98687169, "item/") -> https://www.loc.gov/item/98687169/
        _normalize_url_env("https://dev.loc.gov/item/98687169, "item/") -> https://www.loc.gov/item/98687169/
        _normalize_url_env("g3732a.np000045, "resource/") -> https://www.loc.gov/item/g3732a.np000045/
        """
        if not is_url(locgov_id):
            # If not a URL, turn into a URL, e.g., 20207263765 to https://www.loc.gov/item/20207263765
            url = f"{self.base}{prefix}{locgov_id}"
        else:
            # If it is a URL, update the base to the selected environment prod/dev/text
            pattern = r"https?://.*.?loc.gov/"
            url = re.sub(pattern, self.base, locgov_id)
        if url.endswith("/") == False:
            url = f"{url}/"
        return url

    def get_search(self, n=0) -> bool:
        """
        Perform loc.gov search. Uses express_search() to perform the search, flattens
        the resulting JSON with flatten_locgov(), saves this as JSON to self.search_metadata_json
        and as a dataframe to self.search_metadata. Also updates self.item_ids and self.item_count.

        Optionally can return only the top n results, sorted by relevance.

        Inputs:
         - n(int): If an integer greater than 0 is supplied, only the top n results will be fetched.
            A zero will return all results.

        Updates:
         - self.item_ids
         - self.resource_ids
         - self.segment_ids
         - self.search_metadata
         - self.search_metadata_json
         - self.item_count

        Returns:
         - True - If search runs and finds results
         - False - If error is encountered or search returns no results.
        """
        if self.input_type != "search":
            self.config.logger.error(
                "Attempted to perform a loc.gov search but input_type is not"
                "'search'. Search not executed."
            )
            return False
        else:
            try:
                results_list = express_search(
                    self.input_path,
                    headers=self.headers,
                    c=self.c,
                    n=n,
                    session=self.session,
                    config=self.config,
                )
                self.search_metadata_json = results_list
                if len(results_list) == 0:
                    self.errors["search"].append(
                        "Search returned no results after filters: {self.input_path}. Check logs for details."
                    )
                    return False
                flattened_results_list = flatten_locgov(
                    results_list, config=self.config
                )
                self.search_metadata = pd.DataFrame(flattened_results_list)

                items = self.search_metadata[
                    self.search_metadata["id"].str.contains("loc.gov/item/") == True
                ].copy()
                resources = self.search_metadata[
                    self.search_metadata["id"].str.contains("loc.gov/resource/") == True
                ].copy()
                segments = resources[
                    resources["id"].str.contains(
                        r"^.+\?.*sp=\d+.*$", regex=True
                    )  # ?sp=
                ].copy()

                self.item_ids = items[["id"]].copy()
                self.item_ids.rename(columns={"id": "item_id"}, inplace=True)

                self.resource_ids = resources[["id"]].copy()
                self.resource_ids.rename(columns={"id": "resource_id"}, inplace=True)

                if len(segments) > 0:
                    self.segment_ids = segments[["id"]].copy()
                    self.segment_ids.rename(columns={"id": "segment_id"}, inplace=True)

                # Update IDs to selected environmen
                self._set_env()

                return True
            except Exception as e:
                message = (
                    f"Unknown error encountered by get_search() for {self.input_path}: {e}",
                )
                self.errors["search"].append(message)
                self.config.logger.error(message)
                return False

    def get_items(self) -> bool:
        # TODO split this function into smaller functions
        """
        Fetches item-level records. If the input CSV has a resource_id column and
        not an item_id column, loc.gov resource records will first be retrieved to
        get item ids. No other information from resource records is retained.
        After item records are fetched, information is parsed into dataframes for:
        items, resources, segments, files, and errors.

        Inputs:

        Updates:
         - self.item_ids (adds item IDs for resources from the CSV or for
            resource-level records in search results)
         - self.items
         - self.resources
         - self.segments
         - self.files

        Returns:
         - True - If requests run. Note: True may be returned if errors are encountered.
         - False - If no requests run.

        """
        self.config.logger.info("Downloading item records from loc.gov . . . ")
        no_errors = True
        items = []  # will become self.items
        resources = []  # will become self.resources
        files_segments = []  # will become self.segments
        files_resources = []  # will become self.files

        # If inputs are resources, retrieve item ids from resource records
        if len(self.resource_ids) > 0:
            # updates self.item_ids, self.resource_ids
            self._get_item_id_from_resources()

        if (len(self.item_ids) == 0) & len(self.resource_ids == 0):
            self.config.logger.error(
                "Attempted to request loc.gov item and resource records, but the "
                "lists of item and resource ids are empty. If input_type is "
                "'search', you need to first run  .get_search() on your LocGovRecords "
                "object to generate item ids. If your input_type is 'csv', ensure "
                "that your CSV file has values in the item_id or resource_id columns."
            )
            no_errors = False
            return no_errors

        # For each item_id, request the loc.gov/item/ record and parse
        for _, row in self.item_ids.iterrows():
            # Set up a dictionary and lists for this row/item/resource
            item = row.to_dict()
            item_id = row.get("item_id")
            target_resource = item.get("resource_id")
            # If resource_id has ?... (e.g., ?sp=100), remove everything after the ?
            if target_resource:
                target_resource_id = target_resource.split("?")[0]
            else:
                target_resource_id = None
            # Get the segment parameter, if present
            segment_sp = None
            try:
                segment_sp = re.match(
                    r".*sp=(\d+).*", target_resource.split("?")[1]
                ).group(
                    1
                )  # e.g., "100" from sp=100
            except Exception as _:
                self.config.logger.debug("No segment `sp` param found on resource URL.")

            if segment_sp:
                try:
                    segment_sp = int(segment_sp)
                except Exception as _:
                    self.config.logger.warning(
                        "Attempted to convert 'sp' parameter "
                        "to an integer but failed: %s. Dropping the segment and will collect "
                        "all files for the resource: %s",
                        segment_sp,
                        target_resource,
                    )
            if not item.get("request_error"):
                item["request_error"] = None
            this_item_resources = []
            this_item_files_s = []
            this_item_files_r = []

            while True:
                # SKIP: If the input is a resource and the item id wasn't able to be retrieved
                if item.get("item_id") is None:
                    self.config.logger.error(
                        "Skipping this item because there is no item_id."
                    )
                    break

                # SKIP: If item_id is a non-item URL
                if ("http" in item_id) and ("/item/" not in item_id):
                    self.config.logger.error(
                        "Skipping %s. This does not appear to be a loc.gov item.",
                        item_id,
                    )
                    item["request_error"] = (
                        "ERROR - NOT A LOC.GOV ITEM ID, ITEM API REQUEST SKIPPED"
                    )
                    break

                # PREP: Normalize LCCNs and item URLs
                item_id = self._normalize_url_env(item_id)

                # REQUEST: If not already requested this item (eg, two resources from same item)

                # Try to retrieve the item from items already requested

                dup_item = next(
                    (item for item in items if item["item_id"] == item_id), None
                )
                # If there was a dup, copy it into the current 'item'
                if dup_item:
                    item.update(
                        {
                            **{
                                key: value
                                for key, value in dup_item.items()
                                if key
                                not in ["item_id", "resource_id", "request_error"]
                            }
                        }
                    )
                # Or if there wasn't a dup, make an API request for the item.
                elif not dup_item:
                    # Make API request
                    self.is_blocked, response = make_request(
                        item_id,
                        params={
                            "fo": "json",
                            "at": "item,resources,options.is_partial",
                        },
                        session=self.session,
                        locgov_json=True,
                        is_blocked=self.is_blocked,
                        config=self.config,
                    )

                    # SKIP: If item API request failed
                    if isinstance(response, str):  # all string responses are errors
                        item["request_error"] = response
                        no_errors = False
                        self.config.logger.error(
                            "Resources and files will not be parsed because"
                            "the item record could not be retrieved"
                        )
                        break  # TODO log error in self.errors() via function. see below todo.

                    item.update(response)  # Add response to item dict

                # PARSE: Begin parsing record ==============

                if item.get("resources"):
                    raw_resources = item.get("resources")
                else:
                    raw_resources = None

                # All loc.gov item records with digital resources should have a 'resources' key. If not, skip
                # parsing the resources info
                if not raw_resources:
                    message = f"Item record has no resources key: {item_id}. Skipping parsing of resources and files."
                    self.config.logger.info(message)
                    self.errors["items"].append(
                        {"item_id": item_id, "message": message}
                    )
                    break

                # Create self.resources from parsed item record
                for resource in raw_resources:
                    # Get resource URL and format correctly
                    resource_url = resource.get("url")
                    if resource_url:
                        resource_url = (
                            resource_url
                            if resource_url.endswith("/")
                            else resource_url + "/"
                        )
                        re.sub("(.+)([^/])", "https:", resource_url)
                        re.sub("http:", "https:", resource_url)

                    # SKIP: If there is a target resource from the CSV (item_ids row) and this resource isn't it.
                    if (target_resource_id is not None) & (
                        resource_url != target_resource_id
                    ):
                        continue  # skips this resource & continues to next one from this item

                    this_item_resources.append(
                        {
                            "item_id": item_id,
                            "resource_input_url": target_resource,  # includes segment id, e.g., ?sp=12 as originally provided
                            "segment_count": len(resource.get("files", [])),
                            "resource_id": resource_url,
                            **{
                                key: value
                                for key, value in resource.items()
                                if key != "files"
                            },  # get all keys except the "files" key
                        }
                    )

                    # self.files_resources
                    for possible_field in self.top_level_files:
                        if resource.get(possible_field):
                            this_item_files_r.append(
                                {
                                    "item_id": item_id,
                                    "resource_id": resource_url,
                                    "source_field": possible_field,
                                    "url": resource[possible_field],
                                }
                            )

                    # self.files_segments
                    # TODO this currently also captures captions, which appear as files
                    # If original resources had sp parameter specifying a specific segment
                    # (e.g., ?sp=100), we will only grab the file segments that align
                    # to the original segments
                    if resource.get("files"):
                        for segment_index, file_group in enumerate(resource["files"]):
                            if (segment_sp is not None) & (
                                segment_index + 1 != segment_sp
                            ):
                                continue  # skips this segment & continue to next one from this item/resource
                            for file_index, file in enumerate(file_group):
                                mimetype = file.pop("mimetype", None)
                                file_url = file.pop("url", None)
                                this_item_files_s.append(
                                    {
                                        "item_id": item_id,
                                        "resource_input_url": target_resource,
                                        "resource_id": resource_url,
                                        "segment_num": segment_index,
                                        "file_num": file_index,
                                        "mimetype": mimetype,
                                        "url": file_url,
                                        **file,
                                    }
                                )
                break  # Do not drop this -- we're in a while True block.

            # We remain in a loop representing a single loc.gov item.
            items.append(item)
            resources.extend(this_item_resources)
            files_resources.extend(this_item_files_r)
            files_segments.extend(this_item_files_s)

        # Prep json for items
        items_flattened = flatten_locgov(items, config=self.config)

        # Make the items df
        self.items = pd.DataFrame(items_flattened)

        # Clean the items df -- actions that don't belong in flatten_locgov()
        if "item.resources" in self.items.columns:
            self.items["resource_count"] = self.items["item.resources"].str.len()
            self.items["segment_count"] = self.items["item.resources"].apply(
                lambda resrcs: (
                    sum(resrc.get("files", 0) for resrc in resrcs)
                    if isinstance(resrcs, list)
                    else 0
                )
            )
        else:
            self.items["resource_count"] = 0
            self.items["segment_count"] = 0

        self.items = move_df_column(self.items, "resource_count", 3)
        self.items.rename(columns={"resource_id": "resource_input_url"}, inplace=True)

        # Sort columns so that
        # - columns that don't start with "item." are up front
        # - selected "item." fields come next
        # - remaining "item.*" columns are alphabetical and come last.

        non_item_columns = [
            col for col in self.items.columns if not col.startswith("item.")
        ]
        sort_first_item_columns = [
            "item.resources",
            "item.digitized",
            "item.number_lccn",
            "item.number_fileID",
            "item.number_uuid",
            "item.online_format",
            "item.mime_type",
            "item.partof",
            "item.group",
        ]
        first_item_columnsn = [
            col for col in self.items.columns if col in sort_first_item_columns
        ]
        other_item_columns = [
            col
            for col in self.items.columns
            if ((col.startswith("item.")) & (col not in sort_first_item_columns))
        ]
        other_item_columns.sort()

        self.items = self.items[
            non_item_columns + first_item_columnsn + other_item_columns
        ]

        # Remove the 'resources' column that contains the raw resources json  -- this makes CSVs format badly in Excel if the file list is very long.
        # flatten_locgov doesn't have an option for dropping keys, be careful attempting to add that because it would apply nested at all levels.
        self.items.drop(columns=["resources"], inplace=True)

        self.config.logger.info("Updated: .items")
        self.resources = pd.DataFrame(resources)
        self.config.logger.info("Updated: .resources")
        self.files_resources = pd.DataFrame(files_resources)
        self.config.logger.info("Updated: .files_resources")
        self.files_segments = pd.DataFrame(files_segments)
        self.config.logger.info("Updated: .files_segments")
        errors = self.items[self.items["request_error"].notnull()]
        errors_corrected_fieldnames = errors[["item_id", "request_error"]].rename(
            columns={"request_error": "message"}
        )
        self.errors["items"].append(
            errors_corrected_fieldnames.to_dict(orient="records")
        )

        return no_errors

    def download_files_segments(self, selected_mimetypes: list, dest=None) -> dict:
        """
        Function to download those files in item records that hang from
        resource segments, e.g.,
        https://www.loc.gov/item/08018934/?fo=json&at=resources.0.files.0.5.

        Requires that self.files_segments already be populated.

        This function can only collect files that hang off of segments. To
        download resource-level files (that are not hanging from segments), use
        download_files_resources().

        Note: This function will download *all* sizes of IIIF JPEG files if "image/jpeg" is
        a requested mimetype. Use the abstracted download_jpegs() for better handling.

        Mimetypes recognized by this function are those recognized by the `mimetypes`
        third party Python library.

        Inputs:
         - selected_mimetypes (list): A list of mimetype strings. This can be used to filter
            to a certain subset of files to download, e.g., TIFFs, JP2s, or MP3s.
            Mimetype strings will be matched to the `mimetype` field in loc.gov
            item JSON records, e.g.,
            https://www.loc.gov/item/08018934/?fo=json&at=resources.0.files.0.5
         - dest (str): Local directory to download files into. By default, this will fall back
            to self.outputs unless a value is supplied.

        Updates:

        Returns:
         - dict. Two keys, `downloaded` and `skipped`. Values are lists of
            download URLs
        """
        self.config.logger.info("Preparing to download segment-level files . . .")
        if dest is None:
            dest = self.output_dir
        # Verify inputs
        mimetypes_to_download = []
        if not isinstance(selected_mimetypes, list):
            self.config.logger.error(
                "The `mimetype` input parameter must be a list but was a %s: %s. "
                "Files will not be downloaded.",
                type(selected_mimetypes),
                selected_mimetypes,
            )
            return {}
        for mimetype in selected_mimetypes:
            mimetype_verified = verify_mimetype(mimetype, config=self.config)
            if mimetype_verified is False:
                self.config.logger.error(
                    "Skipping: %s", mimetype
                )  # TODO prompt user to input a replacement mimetype, proceed with other mimetypes, or quit script
            else:
                mimetypes_to_download.append(mimetype)
        if len(mimetypes_to_download) == 0:
            self.config.logger.error("No mimetypes validated. Cannot download files.")
            return {}

        self.config.logger.info(
            "Downloading all files with mimetype(s): %s . . .", mimetypes_to_download
        )

        if len(self.files_segments) == 0:
            self.config.logger.warning(
                "There are no segment files listed in self.files_segments. Did "
                "you forget to run get_items(), or could there be no segment files "
                "associated with your items?"
            )
            self.config.logger.error("Skipping. No segment-level files to download.")
            return {}
        selected_mimetypes = self.files_segments[
            self.files_segments["mimetype"].isin(mimetypes_to_download)
        ].copy()  # filter rows of df down to validated mimetypes ready to download.

        if len(selected_mimetypes) == 0:
            message = "There are no segment files matching your validated mimetypes. Skipping."
            self.config.logger.warning(message)
            return {}

        # TODO https://staff.loc.gov/tasks/browse/DIGS-86 - support IIIF-based download of videos (and audio?) as well

        # Locate download URLs, which may be in various fields in the original JSON.
        # Log items with no found URL. Fields that may contain download URLs. Order
        # from least preferred to most preferred field.
        url_fields = [
            "word_coordinates",
            "fulltext_service",
            "url",
        ]  # TODO these are the fields that could possibly contain the download URL. Confirm if this is the complete list.
        # Create 'src' column in df for the download URL.
        selected_mimetypes["src"] = None
        for src_field in url_fields:
            if src_field in selected_mimetypes.columns:
                selected_mimetypes["src"] = np.where(
                    selected_mimetypes[src_field].notnull(),
                    selected_mimetypes[src_field],
                    selected_mimetypes["src"],
                )
        # Handle rows with no download URL
        no_src_found = selected_mimetypes[selected_mimetypes["src"].isna()]
        skipped_files = []
        if len(no_src_found) > 0:
            self.config.logger.error(
                (
                    "%s files that did not have a download URL in "
                    "any of these fields: %s. Skipping these files."
                ),
                len(no_src_found),
                url_fields,
            )

            for _ in range(len(no_src_found)):
                skipped_files.append(
                    f"ERROR - Unknown file, no download url in {url_fields}"
                )
        # Determine path to save files
        dest = str(Path(dest) / "files_segments/")
        selected_mimetypes["dest"] = selected_mimetypes["src"].apply(
            lambda id: self._generate_dest_path(id, dest)
        )
        self.config.logger.debug("Download paths have been prepared.")

        # Download files
        self.config.logger.debug(
            "Initiating downloads: %s files to download", len(selected_mimetypes)
        )
        download_results = download_from_df(
            selected_mimetypes, session=self.session, config=self.config
        )
        download_results["skipped"].extend(skipped_files)
        return download_results

    def download_files_resources(self, keys: list, dest="./downloads/") -> dict:
        """
        Function to download those files in item records that hang directly from
        resources, e.g.,
        https://www.loc.gov/item/08018934/?fo=json&at=resources.0.fulltext_derivative.

        Requires that self.files_resources already be populated.

        This and download_files_segments() are lower level functions. For general
        use, it is recommended to use more abstracted functions, like
        download_full_text() and download_tiffs(). These functions have built-in logic
        to group files by type (e.g., full text, image, audio).


        Inputs:
         - keys (list): A list of dictionary keys, e.g., fulltext_derivative, image.
            For a full list of possible values, see the Config object definition's
            `self.top_level_files` list.
         - dest (str): Local directory to download files into.

        Updates:

        Returns:
        - dict. Two keys, `downloaded` and `skipped`. Values are lists of
            download URLs
        """

        self.config.logger.info("Preparing to download resource-level files . . .")

        # Verify inputs
        keys_to_download = []
        if not isinstance(keys, list):
            self.config.logger.error(
                "The `keys` input parameter must be a list but was a %s: %s. "
                "Files will not be downloaded.",
                type(keys),
                keys,
            )
            return {}
        for key in keys:
            if key not in (self.top_level_files):
                self.config.logger.error(
                    "You've requested %s but it is not in the list of resource-level "
                    "keys collected by this script. Skipping.",
                    key,
                )  # TODO prompt user to input a replacement key, proceed with other keys, or quit script
            else:
                keys_to_download.append(key)
        if len(keys_to_download) == 0:
            self.config.logger.error("No keys validated. Cannot download files.")
            return {}

        self.config.logger.info("Downloading all files from fields(s): %s . . .", keys)

        if len(self.files_resources) == 0:
            self.config.logger.warning(
                "There are no resource files listed in self.files_resources. Did "
                "you forget to run get_items(), or could there be no resource files "
                "associated with your items?"
            )
            self.config.logger.error("Skipping. No resource-level files to download.")
            return {}
        selected_keys = self.files_resources[
            self.files_resources["file_type"].isin(keys_to_download)
        ].copy()  # filter rows of df down to validated mimetypes ready to download.

        if len(selected_keys) == 0:
            self.config.logger.warning(
                "There are no resource-level files matching your validated fields. Skipping."
            )
            return {}

        # Determine path to save files
        dest = str(Path(dest) / "files_resources/")
        selected_keys["src"] = selected_keys["value"]
        selected_keys["dest"] = selected_keys["src"].apply(
            lambda id: self._generate_dest_path(id, dest)
        )
        self.config.logger.debug("Download paths have been prepared.")

        # Download files
        self.config.logger.debug(
            "Initiating downloads: %s files to download", len(selected_keys)
        )
        download_results = download_from_df(
            selected_keys, session=self.session, config=self.config
        )
        return download_results

    def fulltext_alto(self) -> pd.DataFrame:
        """
        Filters self.files_segments to only Alto XML files. Returns a dataframe of
        Alto XML files with columns that match those of self.files_segments.

        Pattern:
         - Alto XML urls are found in the segment-level 'url' field and end
            in .alto.xml

        Inputs:

        Updates:
         - self.fulltext_alto_df (pd.DataFrame): Same as dataframe returned.

        Returns:
         - pd.DataFrame. Dataframe of Alto XML files. Columns match those of
            self.file_segments.

        """
        if len(self.files_segments) == 0:
            self.config.logger.warning(
                "Cannot create a dataframe of full text Alto XML files because there "
                "are no segment-level files listed. Did you not yet run get_items() "
                "or could your set have no segmented items?"
            )

        output_df = self.files_segments[
            self.files_segments["url"].str.contains(r".+\.alto\.xml$") is True
        ].copy()
        item_ids = set(self.files_segments["item_id"].unique().to_list())
        items_with_alto = set(output_df["item_id"].unique().to_list())
        items_without_alto = list(item_ids - items_with_alto)
        message = (
            f"{len(items_with_alto)} out of your {len(item_ids)} items "
            f"have Alto XML files. {len(items_without_alto)} do not."
        )
        self.config.logger.info(message)
        self.fulltext_alto_df = output_df
        return output_df

    def fulltext_plaintext(self) -> pd.DataFrame:
        """
        Filters self.files_resources to only plain text files from Alto and Djvu full
        text. Returns a dataframe of plain text files with columns that match those
        of self.files_resources.

        NOTE that Chronicling America does not typically include resource-level full
        text files. Use fulltext_alto() instead.

        Pattern:
         - Alto-based plain text files are found in the resource-level 'text_file'
            field and end in '.text.txt'
         - Djvu-based plain text files are found in 'fulltext_file' inside 'resources'
            and end in '_djvu.txt'

        Inputs:

        Updates:
         - self.fulltext_plaintext_df (pd.DataFrame): Same as returned dataframe.

        Returns:
         - pd.DataFrame. Dataframe of plain text files. Columns match those of
            self.files_resources.

        """
        if len(self.files_resources) == 0:
            self.config.logger.warning(
                "Cannot create a dataframe of full text Alto XML files because there "
                "are no resource-level files listed. Did you not yet run get_items() "
                "or could your set have no resources in your items?"
            )
        output_df = self.files_resources[
            (self.files_resources["text_file"].str.contains(r".+\.text\.txt$") is True)
            | (
                self.files_resources["fulltext_file"].str.contains(r".+_djvu\.txt$")
                is True
            )
        ].copy()
        self.fulltext_plaintext_df = output_df
        return output_df

    def fulltext_tei(self) -> pd.DataFrame:
        """
        Filters self.files_resources to XML (TEI) files from the "fulltext_file"
        field. Returns a dataframe with columns that match those of
        self.files_resources.

        NOTE these files are most common in Veterans History Project (VHP)
        collections.

        Pattern:
         - TEI files are found in the resourcelevel 'fulltext_file' field and end in '.xml'

        Inputs:

        Updates:
         - self.alto_tei_df (pd.DataFrame): Same as the returned dataframe.

        Returns:
         - pd.DataFrame. Dataframe of XML (TEI) files from the fulltext_file field on
            resources. Columns match those of self.file_resources.

        """
        if len(self.files_resources) == 0:
            self.config.logger.warning(
                "Cannot create a dataframe of A/V transcription TEI files because there "
                "are no resource-level files listed. Did you not yet run get_items() "
                "or could your set have no resources in your items?"
            )
        output_df = self.files_resources[
            self.files_resources["fulltext_file"].str.contains(r".+\.xml$") is True
        ].copy()
        self.fulltext_tei_df = output_df
        return output_df

    def alto_words(self, alto_urls: list) -> pd.DataFrame:
        """
        Fetches ATLO XML files and parses words into a single dataframe.

        Upon error, returns an empty dataframe and logs error message.

        Inputs:
         - alto_urls (list): List of ALTO XML files with word coordinates

        Updates:
         - self.alto_word_df (pd.DataFrame): Same as the returned dataframe.

        Returns:
         - pd.Dataframe. Dataframe, one row per word (string). Columns:
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
        message = (
            f"Fetching XML from {len(alto_urls)} ALTO XML files, "
            f"and converting into a dataframe . . ."
        )
        self.config.info(message)
        dfs = []
        for alto_url in alto_urls:
            df = altoxml_to_df(alto_url, self.config)
            dfs.append(df)

        try:
            output_df = pd.concat(dfs)
            message = (
                f"Returning dataframe of {len(output_df)} strings from "
                f"{len(alto_urls)} ALTO XML files."
            )
            self.alto_word_df = output_df
            return output_df
        except Exception as e:
            message = (
                f"Could not concatenate Alto XML dataframes together. "
                f"Message: {e}. Returning empty df"
            )
            self.config.logger.error(message)
            return pd.DataFrame()

    def _generate_dest_path(
        self, url: str, base="", iiif_ext=".jpg", iiif_largest=True
    ):  # TODO allow users to specify other IIIF extensions, and handle if they submit a non-valid extension
        """
        For input loc.gov download URL, generates a unique download filename that
        will not clash with other downlaoads.

        Inputs:
         - url (str): URL to download
         - base (str): Base to prefix the front of the filepath, such as a relative or
            absolute directory path.
         - iiif_ext (str): Format for IIIF output. Currently, only the input accepted
            is the default ".jpg"
         - iiif_largest (bool): Download only the largest available IIIF image. Skips clipped
            or scaled images to only download the largest image for a given IIIF identifier.
            If this is set to false, URLs like
            https://tile.loc.gov/image-services/iiif/public:gdcmassbookdig:harriethubbarda00ayer:harriethubbarda00ayer_0009/full/pct:12.5/0/default.jpg
            will result in filenames like
            "public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_0009-full-pct:12.5-0.jpg"
            instead of
            ""public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_0009.jpg"

        Returns:
         - str. The relative or absolute filepath to be saved to.

        Handles various URL patterns like:
         - "https://tile.loc.gov/image-services/iiif/service:gmd:gmd408m:g4084m:g4084cm:g4084cm_g06656195006A:06656_06A_1950-titl/full/pct:12.5/0/default.jpg"
            -> service-gmd-gmd408m-g4084m-g4084cm-g4084cm_g06656195006A-06656_06A_1950-titl.jpg
        - "https://tile.loc.gov/image-services/iiif/public:gdcmassbookdig:harriethubbarda00ayer:harriethubbarda00ayer_0009/full/pct:12.5/0/default.jpg"
            -> public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_0009.jpg
        - "https://tile.loc.gov/storage-services/master/gmd/gmd408m/g4084m/g4084cm/g4084cm_g06656195006A/06656_06A_1950-covr.tif"
            -> master-gmd-gmd408m-g4084m-g4084cm-g4084cm_g06656195006A-06656_06A_1950-covr.tif
        - "https://tile.loc.gov/storage-services/service/gmd/gmd408m/g4084m/g4084cm/g4084cm_g06656195006A/06656_06A_1950-titl.jp2"
            -> service-gmd-gmd408m-g4084m-g4084cm-g4084cm_g06656195006A-06656_06A_1950-titl.jp2
        - "https://tile.loc.gov/storage-services/service/ndnp/vi/batch_vi_eterna_ver01/data/sn84038753/00542866974/1821021001/0151.xml"
            -> service-ndnp-vi-batch_vi_eterna_ver01-data-sn84038753-00542866974-1821021001-0151.xml
        - "https://tile.loc.gov/storage-services/public/gdcmassbookdig/harriethubbarda00ayer/harriethubbarda00ayer_0002.jp2"
            -> public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_0002.jp2
        - "https://tile.loc.gov/storage-services/media/afc/afc1981004/afc1981004_afs20677_01.mp3"
            -> media-afc-afc1981004-afc1981004_afs20677_01.mp3
        - "https://tile.loc.gov/text-services/word-coordinates-service?segment=/public/gdc/00507964357/00507964357_0001.alto.xml&format=alto_xml&full_text=1"
            -> public-gdc-00507964357-00507964357_0001-format-alto_xml-full_text.json
        - "https://tile.loc.gov/text-services/word-coordinates-service?segment=/public/gdc/00507964357/00507964357_0001.alto.xml&format=alto_xml"
            -> public-gdc-00507964357-00507964357_0001-format-alto_xml.json
        - "https://tile.loc.gov/text-services/word-coordinates-service?segment=/public/gdcmassbookdig/harriethubbarda00ayer/harriethubbarda00ayer_djvu.xml&format=djvu_xml&byte_range=6688539-6710358&page_number=543&full_text=1"
            -> public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_djvu-format-djvu_xml-byte_range-6688539-6710358-page_number-543-full_text.json
        - "https://tile.loc.gov/text-services/word-coordinates-service?segment=/public/gdcmassbookdig/harriethubbarda00ayer/harriethubbarda00ayer_djvu.xml&format=djvu_xml&byte_range=6688539-6710358&page_number=543"
            -> public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_djvu-format-djvu_xml-byte_range-6688539-6710358-page_number-543-full_text.json
        """
        static_stor = r"^.*/storage-services/((master|service|public|media)/.+)$"
        iiif_image = r"^.*/image-services/iiif/((?:master|service|public|media):[^/]+)/(.*)/(.*)/(.*)/default.jpg"
        text_services = r"^.*/text-services/word-coordinates-service\?segment=/((master|service|public|media)/.+)(\.alto)?.xml&(.+)"
        # TODO handle text-services
        if not url:
            self.config.logger.debug("Skipping blank URL")
            return None
        elif match := re.match(static_stor, url):
            relative_path = match.group(1).replace("/", "-")
            return str(Path(base) / relative_path)
        elif match := re.match(iiif_image, url):
            if not iiif_ext.startswith("."):
                iiif_ext = f".{iiif_ext}"
            # group1 - identifier eg "public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_0009"
            relative_path = match.group(1).replace(":", "-")
            if iiif_largest is False:
                # group2 - region eg "full"
                # group3 - size eg "pct:..."
                # group4 - rotation eg "0"
                relative_path = f"{relative_path}-{match.group(2)}-{match.group(3)}-{match.group(4)}"
                # public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_0009-full-pct-12.5-0.jpg
                relative_path = f"{relative_path}{iiif_ext}"
            else:
                # public-gdcmassbookdig-harriethubbarda00ayer-harriethubbarda00ayer_0009.jpg
                relative_path = f"{relative_path}{iiif_ext}"
            return str(Path(base) / relative_path)
        elif match := re.match(text_services, url):
            relative_path_base = match.group(1).replace("/", "-")

            # append parameters in a pattern like "format-djvu_xml-byte_range-6688539-6710358-page_number-543-full_text"
            param_list = match.group(4).split("&")
            params = {item.split("=")[0]: item.split("=")[1] for item in param_list}
            relative_path_parts = [relative_path_base]
            for key in ["format", "byte_range", "page_number"]:
                if params.get(key):
                    relative_path_parts.append(key)
                    relative_path_parts.append(params[key])
            if params.get("full_text"):
                relative_path_parts.append("full_text")
            relative_path = "-".join(relative_path_parts)
            relative_path = f"{relative_path}.json"
            return str(Path(base) / relative_path)
        else:
            self.config.logger.error(
                "Encountered an unexpected URL pattern that can't be parsed. Skipping download: %s",
                url,
            )
            return None

    def _get_item_id_from_resources(self) -> pd.DataFrame:
        """
        Input a dataframe with `resource_id` row, output column with `item_id`
            and `request_error` populated.

        Inputs:

        Updates:
         - self.item_ids
         - self.resource_ids
         - self.is_blocked (if a 429 is received)

        Returns:

        """
        self.config.logger.info("Requesting resource records to get item ids . . . ")
        items = []

        for _index, row in self.resource_ids.iterrows():
            resource_id = row["resource_id"]
            if isinstance(resource_id, str):
                resource_id.strip()
            if (pd.isnull(resource_id)) | (resource_id == ""):
                continue  # skip if the resource id is blank, e.g., if the CSV had a row with a blank resource_id value
            item = {}
            item["resource_id"] = resource_id
            item["request_error"] = None

            # Exclude if a non-resource URL
            if ("http" in resource_id) and ("/resource/" not in resource_id):
                self.config.logger.error(
                    "Skipping %s. This does not appear to be a loc.gov resource.",
                    resource_id,
                )
                item["request_error"] = (
                    "ERROR - NOT A LOC.GOV RESOURCE, API REQUEST SKIPPED"
                )
                items.append(item)
                continue

            # Normalize resource id to a url
            resource_id = self._normalize_url_env(resource_id, prefix="resource/")
            item.update({"resource_id": resource_id})

            # Request resource and add item id to self.item_ids.
            self.is_blocked, response = make_request(
                resource_id,
                params={"fo": "json", "at": "item.id,options.is_partial"},
                session=self.session,
                locgov_json=True,
                is_blocked=self.is_blocked,
                config=self.config,
            )
            if isinstance(response, str):
                item["request_error"] = f"{response} - {resource_id}"
            else:
                try:
                    if response.get("item.id"):
                        item["item_id"] = response["item.id"]
                except Exception as e:
                    self.config.logger.error(
                        "Could not retrieve the item id from resource: %s . Message: %s",
                        resource_id,
                        e,
                    )
                    item["request_error"] = (
                        "ERROR - COULD NOT RETRIEVE ITEM ID FROM LOC.GOV RESOURCE "
                        "RECORD. ITEM API REQUEST SKIPPED."
                    )

            if not item.get("item_id"):
                item["item_id"] = None
            items.append(item)

        # Add all item ids to self.item_ids, which becomes a df with columns:
        # item_id, resource_id, request_error
        self.item_ids = pd.DataFrame(items)
        self._set_env()  # Update to selected environment

        self.config.logger.info(
            "Done getting item ids from resource ids. self.item_ids and self.resource_ids updated."
        )

    def get_locgov_records(
        self,
        get_items=True,
        n=0,
        save=False,
    ):
        """
        Used by the locgov_data CLI for non-coders.
        Searches loc.gov item records in bulk. Input type can be either 'search' or
        'csv'. Search is a https://loc.gov/search/ string. CSV is a path to a local
        CSV file with an 'item_ids' column with IDs as LCCNs or loc.gov item URLs.

        User can also supply user_agent header, which will tag their traffic for loc.gov
        staff to use when troubleshooting server issues. Suggested string is an email
        address or URL to app with contact info.

        Inputs:
         - get_items (bool): whether to also get items, or only get research results. Note
            that if the input is a csv, this will be ignored and items will be retrieved.
         - n (int): If an integer greater than 0 is supplied, only the top n results will be
            fetched. A zero will return all results.
         - save (bool): whether to save the results to CSV files.

        Updates:
         - same as locgov_data.LocGovRecords.get_items(). Also saves output files.

        Returns:

        """
        self._set_env()
        if self.input_type == "search":
            search = self.get_search(n=n)
            if search is True:
                self.config.logger.debug("Search outcome: %s", search)
            else:
                self.config.logger.debug("Search outcome: %s. Skipping items.", search)
                get_items = False
        if (get_items is True) | (self.input_type == "csv"):
            self.config.logger.debug("Getting items . . . ")
            item_success = self.get_items()
            self.config.logger.debug("Getting items was successful: %s", item_success)

        if save is True:
            if not os.path.isdir(self.output_dir):
                os.mkdir(self.output_dir)
            # Save search.csv
            if len(self.search_metadata) > 0:  # only save if there's something to save
                search_output = os.path.join(
                    self.output_dir, f"{self.output_prefix}search.csv"
                )
                self.search_metadata.to_csv(search_output, index=False)
                self.config.logger.info("Saved: %s", search_output)
            else:
                self.config.logger.warning("Skipping search.csv, no records to save.")

            if get_items is True:
                # Save items.csv
                if len(self.items) > 0:  # only save if there's something to save
                    items_output = os.path.join(
                        self.output_dir, f"{self.output_prefix}items.csv"
                    )
                    df_to_csv(
                        self.items,
                        items_output,
                        index=False,
                        append=False,
                        config=self.config,
                    )
                else:
                    self.config.logger.warning(
                        "Skipping items.csv, no records to save."
                    )

                # Save resources.csv
                resources_output = os.path.join(
                    self.output_dir, f"{self.output_prefix}resources.csv"
                )
                df_to_csv(
                    self.resources,
                    resources_output,
                    index=False,
                    append=False,
                    config=self.config,
                )

                # Save files_segments.csv
                segments_output = os.path.join(
                    self.output_dir, f"{self.output_prefix}files_segments.csv"
                )
                df_to_csv(
                    self.files_segments,
                    segments_output,
                    index=False,
                    append=False,
                    config=self.config,
                )

                # Save files_resources.csv
                files_output = os.path.join(
                    self.output_dir, f"{self.output_prefix}files_resources.csv"
                )
                df_to_csv(
                    self.files_resources,
                    files_output,
                    index=False,
                    append=False,
                    config=self.config,
                )

                # Save errors.csv
                errors_output = os.path.join(
                    self.output_dir, f"{self.output_prefix}errors.json"
                )
                with open(errors_output, "w", encoding="utf-8") as f:
                    json.dump(self.errors, f, indent=4)
                    self.config.logger.info("Errors saved to %s", errors_output)
                # df_to_csv(
                #     pd.DataFrame(self.errors),
                #     errors_output,
                #     append=False,
                #     config=self.config,
                # )

    def get_marc_df(self) -> pd.DataFrame:
        """
        Retrieves MARC records for all items in self.item_ids. Converts MARC metadata
        into a dataframe.

        Dependencies:
         - self.item_ids - need to be populated with rows with "item_id" values

        Inputs:

        Updates:

        Outputs:
         - pd.DataFrame: Dataframe of MARC fields for each item in self.item_ids.

        """
        self.config.logger.info("Getting MARC metadata . . .")
        marc_df = pd.DataFrame()

        if len(self.item_ids) == 0:
            self.config.logger.warning(
                "There are no items listed in self.items. Skipping MARC records."
            )
            return marc_df
        is_blocked = False
        for _, row in self.item_ids.iterrows():
            item_id = row["item_id"]
            lccn = re.sub(
                r"https?://.*loc.gov/item/([^/]+)/?.*", r"\1", item_id
            )  # item is is likely lccn
            is_blocked, marc_root = get_marcxml_record(
                lccn, config=self.config, is_blocked=is_blocked
            )
            if isinstance(
                marc_root, str
            ):  # If there was an error, this will be a string
                message = f"Could not retrieve record for {item_id}. Skipping MARC for this record. Message: {marc_root}"
                self.config.logger.error(message)
                item_marc_df = pd.DataFrame()
            else:
                marc_data = ET.tostring(marc_root, encoding="unicode")
                item_marc_df = marcxml_to_df(str(marc_data))
            try:
                # add LCCN column with lccn value
                item_marc_df.insert(0, "lccn", [lccn] * len(item_marc_df))
                # add item_id column with item_id value
                item_marc_df.insert(0, "item_id", [item_id] * len(item_marc_df))
                marc_df = pd.concat([marc_df, item_marc_df])
                self.config.logger.info(
                    "MARC record successfully retrieved and added to dataframe: %s",
                    lccn,
                )
            except Exception as e:
                message = f"Error encountered when adding MARC data to df for {item_id}. Skipping MARC for this record. Message: {e}"
                self.config.logger.error(message)

        return marc_df
