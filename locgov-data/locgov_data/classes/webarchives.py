"""
Functions and classes for loc.gov web archive item records
"""

from pathlib import Path
import re
import sys
import traceback
from typing import Union
from bs4 import BeautifulSoup
import pandas as pd

# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries
from locgov_data.classes.config import Config
from locgov_data.helpers.general import *
from locgov_data.classes.locgov import *


def _fetch_mods_url(other_formats: list, config=None) -> Union[str, None]:
    """
    Retrieves the MODS url of a loc.gov item. Input is the item's `item.other_formats`
    field. This function can be run on a dataframe column containing item.other_format
    values as lists.

    Assumes:
     - In item.other_formats, `MODSXML Base Record` is the URL to retrieve. Non-webarchive
        records will have `MODS Record`, which will be excluded.
     - There will only be one `MODSXML Base Record` in item.other_formats.

    Inputs:
     - other_formats (list) : The `item.other_formats` list from a single
        loc.gov item record.
     - config ([None, classes.general.Config]): Config object.

    Returns:
     - Union[str, None]: A string value (MODS url) or None
    """
    if config is None:
        config = Config()

    config.logger.debug(
        "Extracting MODS url from loc.gov item record field `item.other_formats` . . ."
    )
    for other_format in other_formats:
        if isinstance(
            other_format, dict
        ):  # item.other_formats is a list of dictionaries
            if other_format.get("label") and other_format.get("link"):
                if other_format["label"] == "MODSXML Base Record":
                    config.logger.debug(
                        "Successfully extracted MODS url from loc.gov item "
                        "record field item.other_formats: %s",
                        other_format["link"],
                    )
                    url = other_format["link"]
                    if url.startswith("//tile.loc.gov"):
                        url = f"https:{url}"
                    return url
        elif isinstance(
            other_format, str
        ):  # for search records, other_formats will be a list of strings
            pattern = r"^.+/mods/.+xml$"
            if re.match(pattern, other_format):
                return other_format
    config.logger.error(
        "Could not extract MODS url from loc.gov item record field item.other_formats."
    )
    return None


class WebArchives(LocGovRecords):
    """
    Class for batch collection of web archive metadata from loc.gov and
    webarchives.loc.gov. Sub-class of LocGovRecords.
    """

    def __init__(self, parent_class_object=None):
        """
        Inputs:
         - parent_class_object (locgov_data.classes.locgov.LocGovRecords): Parent
            class, a LocGovRecords object.

        Returns:
         - locgov_data.WebArchives

        """
        if parent_class_object is not None:
            self.__dict__.update(vars(parent_class_object))
        else:
            self.session = requests.Session()
        self.chunks = []
        self.metadata_csv = pd.DataFrame()
        self.config.logger.debug("WebArchives object initialized.")
        self.seeds = pd.DataFrame()

    def get_mods_url(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetches and parses the MODS URLs.

        Inputs:
         - df (pd.DataFrame): Generally self.items or self.search_metadata.
            Must contain column "items.other_formats"

        Updates:

        Returns:
         - pd.DataFrame. updated version of df DataFrame. If parsing was
            unsuccessful, un-updated version is returned.

        """
        if "item.other_formats" not in df.columns:
            self.config.logger.error(
                "Could not parse MODS URLs from input df. The df must have "
                "a column `item.other_formats`"
            )
            return df
        try:
            df["mods_url"] = df["item.other_formats"].apply(
                lambda x: _fetch_mods_url(x, config=self.config)
            )
            return df
        except Exception as e:
            self.config.logger.error(
                "Could not parse MODS URLs from df. Could you have skipped "
                "running get_locgov_records()? Message: %s . Traceback: %s",
                e,
                traceback.print_exc(),
            )
            return df

    def get_mods_uselection(self, chunk_size=100):
        """
        Requests MODS records, and parses information relevant to US Election
            candidates. Splits large collections into smaller chunks (default 100
            items) to avoid data loss due to script failing and/or 429s.

        Inputs:
         - chunk_size (int): For large MODS lists, chunks lists into segments of this size
            for processing. Default = 100

        Updates:
         - self.seeds - DataFrame from self.items, with MODS metadata and converted
                from item- to seed-level.
         - self.seeds_by_year - self.seeds exploded by year (equivalent to digiboard `records`)

        Returns:
         - self.seeds DataFrame or None if an unknown error is encountered.

        """
        try:
            self.get_mods_url(self.search_metadata)
            self.chunks = df_to_chunks(
                self.search_metadata, chunk_size=chunk_size, config=self.config
            )  # list of smaller dataframes
            chunk_dfs = []
            chunk_files = []
            for index, chunk in enumerate(self.chunks):
                self.config.logger.debug(
                    "Retrieving and parsing US Elections MODS metadata. . . "
                )
                seed_level_chunk = self._get_mods_info_uselection(chunk.copy())
                seed_level_chunk.to_csv(
                    f"_processed_chunk_{index}.csv", encoding="utf-8-sig"
                )
                self.config.logger.debug(
                    "Saved temporary file: _processed_chunk_%s.csv", index
                )
                chunk_files.append(f"_processed_chunk_{index}.csv")
                chunk_dfs.append(seed_level_chunk)

            self.config.logger.debug(
                "Combining all chunk dfs into one dataframe . . . "
            )
            seed_df = pd.concat(chunk_dfs, ignore_index=True)
            seed_df.rename(
                columns={
                    "id": "item_id",
                    "title": "item_title",
                    "mods_seeds": "record_seeds",
                },
                inplace=True,
            )

            # Normalize blanks in seed_subject_facets
            self.config.logger.info("Cleaning blank 'seed_subject_facets' . . . ")
            seed_df["seed_subject_facets"] = seed_df["seed_subject_facets"].fillna(
                "[{}]"
            )
            seed_df["seed_subject_facets"].str.replace("", "[{}]")

            # self.seeds
            self.seeds = seed_df

            # self.seeds_by_year
            self.config.logger.info("Exploding 'seed_subject_facets' . . . ")
            seeds_by_year = seed_df.explode("seed_subject_facets").copy()
            (
                seeds_by_year["collection"],
                seeds_by_year["website_election"],
                seeds_by_year["website_parties"],
                seeds_by_year["places"],
                seeds_by_year["website_districts"],
            ) = zip(
                *seeds_by_year["seed_subject_facets"].apply(
                    self._parse_subjects_campaigns
                )
            )
            seeds_by_year = seeds_by_year[
                [
                    "item_id",
                    "item_title",
                    "website_url",
                    "website_id",
                    "website_scopes",
                    "collection",
                    "website_election",
                    "website_parties",
                    "places",
                    "website_districts",
                    "website_thumbnail",
                    "website_start_date",
                    "website_end_date",
                    "item_all_years",
                    "website_all_years",
                    "mods_url",
                ]
            ]
            self.seeds_by_year = seeds_by_year
            return seeds_by_year
        except Exception as e:
            self.config.logger.error(
                "Problem retrieving and parsing MODS records. Message: %s . Traceback: %s",
                e,
                traceback.print_exc(),
            )
            return None

    def _parse_subjects_campaigns(self, subjects: dict):
        """
        Parse loc.gov item `subject` field for election campaigns. Designed to be
        run against dataframe of item metadata inherited from parent class.

        Inputs:
         - subjects (dict): Subjects field from loc.gov record, such as:
            {
                'year': 2016,
                'subjects': [
                    'United States Elections, 2016',
                    'United States. Congress. Senate',
                    'Republican Party',
                    'Nevada',
                    'United States Elections, 2016',
                    'United States. Congress. House',
                    'Republican Party',
                    'Nevada',
                    'Nevada (3rd Congressional District)'
                ]
            }

        Returns:
         - collection (list)
         - election (list)
         - party(list)
         - place (list)
         - house_district (list)

        """

        collection = []
        elections = []
        parties = []
        places = []
        house_districts = []

        if subjects.get("subjects") is None:
            self.config.logger.error(
                "No subjects found in MARS, returning blank lists for all "
                "fields derived from MODS subject elements."
            )
            return collection, elections, parties, places, house_districts

        subjects = subjects["subjects"]

        collection = subjects[0]

        def _split_on_elections(input_list):
            split_lists = []
            current_sublist = []

            for item in input_list:
                if item.startswith("United States Elections,"):
                    if current_sublist:
                        split_lists.append(current_sublist)
                        current_sublist = []
                current_sublist.append(item)

            if current_sublist:
                split_lists.append(current_sublist)

            return split_lists

        subject_lists = _split_on_elections(subjects)

        for campaign_subject_list in subject_lists:
            """campaign_subject_list is like:
            [
                'United States Elections, 2016',
                'United States. Congress. House',
                'Republican Party',
                'Nevada',
                'Nevada (3rd Congressional District)' # fifth element only in house districts
            ]
            """
            elections.append(campaign_subject_list[1])
            parties.append(campaign_subject_list[2])
            places.append(campaign_subject_list[3])
            if len(campaign_subject_list) > 4:
                house_districts.append(campaign_subject_list[4])
            else:
                house_districts.append(None)

        return collection, elections, parties, places, house_districts

    def _get_mods_info_uselection(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Input a dataframe of loc.gov metadata for web archives. Fetches and parses
        MODS metadata and returns a dataframe at the seed level with web archive-specific
        metadata from web archive MODS files.

        Inputs:
         - df (pd.DataFrame): Dataframe of web archive loc.gov metadata. Each row
            is a loc.gov item. Expects the 'mods_url' column to have pointers to
            election MODS records.

        Returns:
         - pd.DataFrame. Dataframe at the seed level with web archive-specific
            metadata from web archive MODS files

        """
        df["mods_seeds"] = df["mods_url"].apply(self._parse_mods_uselections)

        # Convert dataframe from item-level to seed-level
        self.config.logger.debug(
            "Converting dataframe from  item-level to seed-level . . ."
        )
        exploded = df.explode("mods_seeds").copy()
        exploded["website_url"] = exploded["mods_seeds"].str["website_url"]
        exploded["website_id"] = exploded["mods_seeds"].str["website_id"]
        exploded["website_thumbnail"] = exploded["mods_seeds"].str["website_thumbnail"]
        exploded["website_scopes"] = exploded["mods_seeds"].str["website_scopes"]
        exploded["website_start_date"] = exploded["mods_seeds"].str[
            "website_start_date"
        ]
        exploded["website_end_date"] = exploded["mods_seeds"].str["website_end_date"]
        exploded["item_all_years"] = exploded["mods_seeds"].str["item_all_years"]
        exploded["website_all_years"] = exploded["mods_seeds"].str["website_all_years"]
        exploded["seed_subject_facets"] = exploded["mods_seeds"].str[
            "seed_subject_facets"
        ]
        self.config.logger.debug(
            "Finished fetching MODS metadata, seed-level dataframe created."
        )
        return exploded

    def _parse_mods_uselections(self, mods_url: str) -> list:
        """
        Retrieves and parses web archive MODS file for US election candidates.

        Inputs:
         - mods_url (str): MODS download URL

        Returns:
         - Empty list [] for any error or no candidates in MODS
         - Otherwise, returns list of dictionaries, one dictionary per
                resource (digiboard seed) on the loc.gov item (digiboard entity).
        """
        self.config.logger.debug("Requesting MODS record: %s . . . ", mods_url)
        is_blocked, response = make_request(
            mods_url,
            params=[],
            session=self.session,
            config=self.config,
        )
        records = []
        if not str(response).startswith("ERROR -"):
            self.config.logger.debug("Parsing MODS file as xml: %s . . . ", mods_url)
            try:
                soup = BeautifulSoup(response.text, "xml")
                # Get entity-level collections
                subjects = soup.find_all("subject")
                collections = []
                for subject in subjects:
                    topic = subject.find("topic")
                    if topic and ("United States Elections" in topic.get_text()):
                        collections.append(topic.get_text(strip=True))
                entity_campaign_years = [
                    collection_title[-4:] for collection_title in collections
                ]

                # Get seed-level metadata
                related_items = soup.find_all("relatedItem", type="constituent")
                for item in related_items:
                    self.config.logger.debug(
                        "Parsing resource/seed in MODS record . . . "
                    )
                    record_dict = {}

                    # Extract seed URL
                    seed_url_identifier = item.find(
                        "identifier", displayLabel="Access URL"
                    )
                    seed_url = (
                        seed_url_identifier.get_text(strip=True)
                        if seed_url_identifier
                        else None
                    )

                    # Extract seed id
                    database_id_identifier = item.find("identifier", type="database id")
                    seed_id = (
                        database_id_identifier.get_text(strip=True)
                        if database_id_identifier
                        else None
                    )

                    # Extract thumbnails
                    location_url = item.find("location").find(
                        "url", displayLabel="thumbnail image"
                    )
                    seed_thumbnail = (
                        location_url.get_text(strip=True) if location_url else None
                    )

                    # Extract scopes
                    parts = item.find_all("part", type="scope")
                    seed_scopes = []
                    for part in parts:
                        texts = part.find_all("text")
                        for text in texts:
                            seed_scopes.append(text.get_text(strip=True))

                    # Extract start and end date
                    start = item.find("dateCaptured", point="start")
                    start_date = start.get_text(strip=True) if start else None
                    end = item.find("dateCaptured", point="end")
                    end_date = end.get_text(strip=True) if end else None

                    # Print or store the extracted data
                    record_dict["website_url"] = seed_url
                    record_dict["website_id"] = seed_id
                    record_dict["website_thumbnail"] = seed_thumbnail
                    record_dict["website_scopes"] = seed_scopes
                    record_dict["website_start_date"] = start_date
                    record_dict["website_end_date"] = end_date
                    record_dict["item_all_years"] = entity_campaign_years
                    record_dict["website_all_years"] = self._parse_dates_campaigns(
                        record_dict
                    )
                    record_dict["seed_subject_facets"] = [
                        {
                            "year": year,
                            "subjects": self.get_subject_facets_campains(soup, year),
                        }
                        for year in record_dict["website_all_years"]
                    ]
                    self.config.logger.debug(
                        "Parsed resource/seed in MODS record: %s ", record_dict
                    )
                    records.append(record_dict)
                return records

            except Exception as e:
                self.config.logger.error(
                    "Error parsing MODS: %s, %s. Traceback: %s",
                    mods_url,
                    e,
                    traceback.print_exc(),
                )
                return records
        self.config.logger.error("Error retrieving MODS: %s, %s", mods_url, response)
        return records

    def _parse_dates_campaigns(self, record_dict: dict) -> list:
        """
        Compare the MODS record date range for a seed's meaningful captures against
        a list of possible campaign collection years for that seed (usually the
        entity-level list of campaign years).

        Inputs:
         - record_dict (dict): Dictionary describing website, from
            _parse_mods_uselections().

        Returns:
         - list. List of campaign years for given website.
        """
        self.config.logger.debug(
            "Determining collection year(s) for %s based on capture year range "
            "and item collection years . . .",
            record_dict["website_url"],
        )
        start_date = record_dict["website_start_date"]
        end_date = record_dict["website_end_date"]
        entity_campaign_years = record_dict["item_all_years"]
        website_campaign_years = []
        try:
            for year in range(int(start_date[:4]), int(end_date[:4]) + 1):
                if str(year) in entity_campaign_years:
                    website_campaign_years.append(year)
            self.config.logger.debug(
                "Determined collection year(s) for %s: %s",
                record_dict["website_url"],
                website_campaign_years,
            )
            return website_campaign_years
        except Exception as e:
            self.config.logger.error(
                "Encountered error determining collection year(s) for %s: %s . Traceback: %s",
                record_dict["website_url"],
                e,
                traceback.print_exc(),
            )
            return website_campaign_years

    def get_subject_facets_campains(self, soup, campaign_year) -> list:
        """
        Collects election-year-specific subject facets from MODS record.
        Does not retain the authority attribute (e.g., 'lcsh' or 'local') or
        the element name (e.g., 'geographic', 'namePart').
        Return most but not all subject facets on the loc.gov item. Only returns
        <subject>s with displayLabel="United States Elections, 2008", e.g.:

        Does not retain:
            <subject authority="lcsh">
                <topic>Politics and government</topic>
                <geographic>United States</geographic>
            </subject>
        Does retain:
            <subject authority="local" displayLabel="United States Elections, 2008">
                <topic>United States Elections, 2008</topic>
            </subject>

        Inputs:
         - soup (bs4.BeautifulSoup.soup): Soup object from MODS XML record
         - campaign year (int): Campaign year, for which facet information should
            be parsed

        Updates:

        Returns:
         - list. List of subject facets.
        """
        self.config.logger.debug(
            "Fetching MODS subject elements . . . ",
        )
        campaign_subjects = soup.find_all(
            "subject", displayLabel=f"United States Elections, {campaign_year}"
        )
        subject_facets = []
        for subject in campaign_subjects:
            joined_subjects = subject.get_text(". ", strip=True)
            subject_facets.append(joined_subjects)
        self.config.logger.debug(
            "Fetched MODS subject elements: %s . . . ", subject_facets
        )
        return subject_facets

    def make_metadata_csv(self):
        """
        Saves a publicly publishable version of metadata.csv for a data package.

        Inputs:

        Updates:
         - self.metadata_csv - pd.DataFrame version of metadat.csv

        Returns:
         - bool. True if a metadata.csv was created and self.metadata_csv
            updated. False if failed to create.
        """
        if not hasattr(self, "seeds_by_year"):
            self.config.logger.error(
                "Metadata.csv skipped. Could not create because self.seeds_by_year "
                "does not exist. Could you have forgotten to run get_mods_uselection()?"
            )
            return False

        seeds_by_year = self.seeds_by_year.copy()

        # Combine record (seed_by_year) metadata with loc.gov access_condition field
        access_conditions = self.search_metadata[["id", "item.access_condition"]].copy()
        access_conditions.rename(
            columns={"id": "item_id", "item.access_condition": "access_condition"},
            inplace=True,
        )
        access_conditions = access_conditions[
            access_conditions["item_id"].duplicated() is False
        ]
        try:
            seeds_by_year = seeds_by_year.merge(
                access_conditions,
                how="left",
                on="item_id",
                validate="many_to_one",
                indicator="_merge",
            )
        except Exception as e:
            self.config.logger.error(
                "Metadata.csv skipped. Failed to add `access_condition` column. "
                "Message: %s. Traceback: %s",
                e,
                traceback.print_exc(),
            )
            return False

        # QC - check if all lines merged
        if seeds_by_year["_merge"].value_counts()["both"] < len(seeds_by_year):
            self.config.logger.error(
                "Metadata.csv skipped. Failed to add `access_condition` column. "
                "Problem matching item ids.",
            )
            return False

        # QC - Add additional quality checks here

        seeds_by_year.pop("_merge")
        self.metadata_csv = seeds_by_year.copy()
        self.config.logger.info(
            "Publishable metadata.csv saved and added as self.metadata_csv"
        )
        return True
