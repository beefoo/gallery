import os
import pandas as pd
import shutil
import tempfile
import unittest
from unittest.mock import patch, Mock
import locgov_data as ld
import locgov_data.cli.__main__ as ldcli

# results = ldcli.main()


class TestCli(unittest.TestCase):
    """
    Confirms that the locgov_data CLI is operating as intended.
    Tests the following scenarios:
        - A general search URL that also pulls item metadata
        - A collection search (Sanborn -- includes online and not online)
        - A format search (audio with content from multiple collections)
        - A ChronAm full text search
        - Resource list CSV input

    For each scenario tests:
        - The intended files are saved (to temp location, deleted after tests).
            For a list of expected files, see self.expected_files in setUp()
        - Files contain a curated list of required columns (e.g., "item_id")
            For a list of expected columns, see self.expected_columns in setUp()
        - For some file types, checks that certain columns do *not* exist (e.g.,
            "resources" shouldn't exist in items.csv). For a list of expected
            columns, see self.expected_columns in setUp()
    """

    def setUp(self):
        """
        This will automatically run before each unit test function
        (which start with "test") below.
        """
        # Create a temporary directory for output files
        self.output_dir = tempfile.mkdtemp()

        # Set up inputs for the CLI
        self.n = 6  # fetch only the top 45 results
        self.output_prefix = "test_"
        self.pause = 1
        self.user_agent = "DCMS unittest"
        self.log = "./log/"
        self.log_debug = True
        self.verbose = False
        self.is_election = False

        self.expected_files = {
            "is_search": ["search.csv"],
            "get_item_true": [
                "resources.csv",
                "items.csv",
                "files_resources.csv",
                "files_segments.csv",
            ],
            "get_item_false": ["search.csv"],
            "no_results_expected": [],
        }

        self.expected_columns = {
            "items.csv": {
                "has_column": [
                    "item_id",
                    "resource_count",
                    "segment_count",
                    "item.id",
                    "item.group",
                    "item.aka",
                    "item.item",
                    "item.digitized",
                ],
                "hasnot_column": ["resources", "full_text"],
            },
            "resources.csv": {
                "has_column": [
                    "item_id",
                    "resource_id",
                    "resource_input_url",
                    "segment_count",
                    "url",
                ],
                "hasnot_column": [],
            },
            "files_resources": {
                "has_column": [
                    "item_id",
                    "resource_input_url",
                    "resource_id",
                    "source_field",
                    "url",
                ],
                "hasnot_column": [],
            },
            "files_segments": {
                "has_column": [
                    "item_id",
                    "resource_input_url",
                    "resource_id",
                    "segment_num",
                    "file_num",
                    "mimetype",
                    "url",
                ],
                "hasnot_column": [],
            },
            "search.csv": {
                "has_column": ["id", "group", "aka", "digitized"],
                "hasnot_column": ["item_id"],
            },
        }
        self.expected_value_types = {
            "item_id": str,
            "id": str,
            "group": list,
            "aka": list,
            "item": dict,
            "digitized": bool,
        }
        self.expected_string_values = {
            "item_id": r"https?://.?\.?loc.gov/.+",  # eg., http://lccn.loc.gov/98643777, http://www.loc.gov/item/sn93059260/
            "id": r"https?://.?\.?loc.gov/.+",  # same as item_id
        }

    def tearDown(self):
        """
        This will automatically run after each unit test function
        (which start with "test") below.
        """
        # Clean up temporary directory after test
        shutil.rmtree(self.output_dir)

    def run_search_test(
        self, input_path, get_items, input_type, env="prod", no_results_expected=False
    ):
        # Set up the expected files
        expected_files = []
        self.env = env
        if get_items == True:
            expected_files = expected_files + self.expected_files["get_item_true"]
        if input_type.lower() == "search":
            expected_files = expected_files + self.expected_files["is_search"]
        if no_results_expected == True:
            expected_files = self.expected_files["no_results_expected"]

        print("Submitting API request . . . ")
        ldcli.main(
            input_type=input_type,
            input_path=input_path,
            n=self.n,
            output_dir=self.output_dir,
            output_prefix=self.output_prefix,
            pause=self.pause,
            env=self.env,
            user_agent=self.user_agent,
            log=self.log,
            log_debug=self.log_debug,
            verbose=self.verbose,
            is_election=self.is_election,
            get_items=get_items,
        )
        print("Done with API request")
        for filename in expected_files:
            csv_file = os.path.join(self.output_dir, self.output_prefix + filename)
            # Check that the CSV exists
            self.assertTrue(
                os.path.exists(csv_file), f"Output file does not exist: {csv_file}"
            )
            df = None
            try:
                # Check that CSV isn't empty and can be read by pandas
                df = pd.read_csv(csv_file)
            except pd.errors.EmptyDataError:
                print(f"File is blank: {filename}")
                print("Moving on to next file and not checking content . . .")
                continue
            except:
                self.fail(f"An output CSV file can't be read by pandas: {csv_file}")
            if isinstance(df, pd.DataFrame):
                if self.expected_columns.get(filename):
                    for column in self.expected_columns[filename]["has_column"]:
                        # Check the minimum columns expected are present
                        self.assertTrue(
                            column in df.columns,
                            f"{filename} is missing an expected column: {column}",
                        )
                        # Check that values in these columns are the expected types.

                        # For string columns, check that the values match expected regex.

                    for column in self.expected_columns[filename]["hasnot_column"]:
                        # Check that excluded or not-expected columns are not present
                        self.assertTrue(
                            column not in df.columns,
                            f"{filename} has a column that shouldn't be present: {column}",
                        )

    # General search - https://www.loc.gov/search/?all=true&dates=1920/1920&fa=location:massachusetts&q=jamaica+plains
    def test_general_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/search/?all=true&dates=1920/1920&fa=location:massachusetts&q=jamaica+plains",
            get_items=True,
            input_type="search",
        )

    # Collection search (Sanborn) - https://www.loc.gov/collections/sanborn-maps/?dates=1928/1931&fa=location_state:illinois
    def test_collection_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/collections/sanborn-maps/?dates=1928/1931&fa=location_state:illinois",
            get_items=True,
            input_type="search",
        )

    # Format search (audio, mixed collections) - https://www.loc.gov/audio/?dates=1900/1925&fa=location:hawaii
    def test_format_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/audio/?dates=1900/1925&fa=location:hawaii",
            get_items=True,
            input_type="search",
        )

    # ChronAm full text search - https://www.loc.gov/collections/chronicling-america/?dl=page&end_date=1930-12-31&ops=AND&qs=influenza&searchType=advanced&start_date=1920-12-01
    def test_chronam_fulltext_search_live(self):
        print("running test_chronam_fulltext_search_live")
        self.run_search_test(
            input_path="https://www.loc.gov/collections/chronicling-america/?dl=page&end_date=1930-12-31&ops=AND&qs=influenza&searchType=advanced&start_date=1920-12-01",
            get_items=True,
            input_type="search",
        )

    # ChronAm title-level search - https://www.loc.gov/collections/chronicling-america/?dates=1780/1789&dl=title
    def test_chronam_title_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/collections/chronicling-america/?dates=1780/1789&dl=title",
            get_items=True,
            input_type="search",
        )

    # ChronAm mixed-level search - https://www.loc.gov/collections/chronicling-america/?dates=1784&dl=all
    def test_chronam_mixedlevel_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/collections/chronicling-america/?dates=1784&dl=all",
            get_items=True,
            input_type="search",
        )

    # Search, Directory of US Newspapers in American Libraries - https://www.loc.gov/collections/directory-of-us-newspapers-in-american-libraries/?dates=1800/1910&fa=location_state:california%7Clanguage:chinese&searchType=advanced
    def test_newspaper_directory_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/collections/directory-of-us-newspapers-in-american-libraries/?dates=1800/1910&fa=location_state:california%7Clanguage:chinese&searchType=advanced",
            get_items=True,
            input_type="search",
        )

    # Multi-resource display items and their child items, search - https://www.loc.gov/search/?in=&q=%22Collections+of+the+Maine+historical+society%22
    def test_multiresource_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/search/?in=&q=%22Collections+of+the+Maine+historical+society%22",
            get_items=True,
            input_type="search",
        )

    # BtP transcribed search - https://www.loc.gov/search/?fa=partof:early+copyright+title+pages,+1790+to+1870%7Conline-format:online+text
    def test_btp_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/search/?fa=partof:early+copyright+title+pages,+1790+to+1870%7Conline-format:online+text",
            get_items=True,
            input_type="search",
        )

    # Zero results search - https://www.loc.gov/search/?all=true&fa=location:lollygaggalog
    def test_zero_results_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/search/?all=true&fa=location:lollygaggalog",
            get_items=True,
            input_type="search",
            no_results_expected=True,
        )

    # Search for item with 900+ resources - https://www.loc.gov/search/?in=&q=El+Mosquito+2020741465
    def test_big_items_search_live(self):
        self.run_search_test(
            input_path="https://www.loc.gov/search/?in=&q=El+Mosquito+2020741465",
            get_items=True,
            input_type="search",
        )

    # Resource list CSV input - ./tests/resources/p1_cli_resource_list.csv
    def test_resource_csv_live(self):
        self.run_search_test(
            input_path="./tests/resources/p1_cli_resource_list.csv",
            get_items=True,
            input_type="csv",
        )

    # Item list CSV input - ./tests/resources/p1_cli_item_list.csv
    def test_item_csv_live(self):
        self.run_search_test(
            input_path="./tests/resources/p1_cli_item_list.csv",
            get_items=True,
            input_type="csv",
        )

    # Dev environment - Resource list CSV input - ./tests/resources/p1_cli_resource_list.csv
    def test_resource_csv_dev(self):
        self.run_search_test(
            input_path="./tests/resources/p1_cli_resource_list.csv",
            get_items=True,
            input_type="csv",
            env="dev",
        )

    # Test environment - General search - https://www.loc.gov/search/?all=true&dates=1920/1920&fa=location:massachusetts&q=jamaica+plains
    def test_general_search_test(self):
        self.run_search_test(
            input_path="https://www.loc.gov/search/?all=true&dates=1920/1920&fa=location:massachusetts&q=jamaica+plains",
            get_items=True,
            input_type="search",
            env="test",
        )


def main():
    unittest.main()


if __name__ == "__main__":
    main()
