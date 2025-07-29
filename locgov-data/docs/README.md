# `locgov_data` Library Reference

- <a href="#">locgov_data</a>
  - <a href="#classes">Classes</a>
    - <a href="#config">Config</a>
      - <a href="#__init__">\_\_init\_\_</a>
    - <a href="#locgovrecords">LocGovRecords</a>
      - <a href="#__init__-1">\_\_init\_\_</a>
      - <a href="#get_search">get_search</a>
      - <a href="#get_items">get_items</a>
      - <a href="#download_files_segments">download_files_segments</a>
      - <a href="#download_files_resources">download_files_resources</a>
      - <a href="#fulltext_alto">fulltext_alto</a>
      - <a href="#fulltext_plaintext">fulltext_plaintext</a>
      - <a href="#fulltext_tei">fulltext_tei</a>
      - <a href="#alto_word_df">alto_word_df</a>
      - <a href="#get_locgov_records">get_locgov_records</a>
    - <a href="#webarchives">WebArchives</a>
      - <a href="#__init__-2">\_\_init\_\_</a>
      - <a href="#get_mods_url">get_mods_url</a>
      - <a href="#get_mods_uselection">get_mods_uselection</a>
      - <a href="#get_subject_facets_campains">get_subject_facets_campains</a>
      - <a href="#make_metadata_csv">make_metadata_csv</a>
  - <a href="#functions">Functions</a>
    - <a href="#express_search">express_search</a>
    - <a href="#make_request">make_request</a>
    - <a href="#flatten_json">flatten_json</a>
    - <a href="#flatten_locgov">flatten_locgov</a>
    - <a href="#df_to_csv">df_to_csv</a>
    - <a href="#download_file">download_file</a>
    - <a href="#download_from_df">download_from_df</a>
    - <a href="#csv_to_df">csv_to_df</a>
    - <a href="#df_to_chunks">df_to_chunks</a>
    - <a href="#filter_dict">filter_dict</a>
    - <a href="#verify_mimetype">verify_mimetype</a>
    - <a href="#is_url">is_url</a>
    - <a href="#jupyter">jupyter</a>
      - <a href="#review_images_jupyter">review_images_jupyter</a>
    - <a href="#fulltext">fulltext</a>
      - <a href="#altoxml_to_df">altoxml_to_df</a>
      - <a href="#altoxmls_to_df">altoxmls_to_df</a>
    - <a href="#marcxml">marcxml</a>
      - <a href="#get_marcxml_record">get_marcxml_record</a>
      - <a href="#marcxml_to_df">marcxml_to_df</a>
      - <a href="#marcxml_to_sdf">marcxml_to_sdf</a>
      - <a href="#get_marc_field">get_marc_field</a>

# locgov_data

Python library for easily retrieving metadata and media files from loc.gov

## Classes

### Config

```python
class Config
```

Configuration object, to share parameters across functions.
Primarily for logging and standardizing parameters when requesting
resources across domains.

```python
def __init__(
    self,
    debug=False,
    log="./log",
    verbose=False,
    pause=5,
    user_agent=None
)
```

**Example usage**:

```python
import locgovdata as ld
config = ld.Config(
    debug=True,
    log="./project-logs",
    verbose=True,
    pause=4,
    user_agent="my-email@loc.gov my-project-1"
)
```

**Inputs**:

- debug (bool): Whether to set log.log at DEBUG level (default is INFO)
- log (str): Relative or absolute path to directory for saving log files.
- verbose (bool): Whether to output to terminal (at INFO level)
- pause (int): Base number of seconds between requests to non-loc.gov domains.
- user_agent(str): User-Agent header for non-loc.gov requests.

**Returns**:

- locgov_data.Config

### LocGovRecords

```python
class LocGovRecords
```

Object for collecting a set of loc.gov data

```python
def __init__(
    self,
    input_type=None,
    input_path=None,
    ouput_dir="./output/",
    output_prefix="",
    c=100,
    user_agent=None,
    pause=5,
    env="prod",
    is_election=False,
    config=None
)
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
    output_dir="lc_output",
    c=200,
    user_agent="my-email@loc.gov",
    pause=4
)
```

Initialize the Search class with input type and path.
For CSV input, item_id is a required CSV column and should include loc.gov item ids or URLs.

**Inputs**:

- input_type (str): Type of input, either 'csv' or 'search'.
- input_path (str): Path to the CSV file or search URL. CSV must contain a
  column 'item_id' or 'resource_id'
- output_dir (str): Directory to save outputs into.
- output_prefix (str): Prefix for output files.
- c (int): Pagination parameter, the number of results to return per loc.gov
  search page
- user_agent (str): User-Agent http header to be passed with all requests.
  Suggested staff usage is email address. Suggested usage for public
  scrips is the URL to the script.
- pause (int): Base number of seconds to wait between loc.gov requests.
- env (str): Environment to operate in: prod (default), test, or dev. Only prod is publicly available.
- is_election (bool): if this is a search for web archive US Election records.
  Facilitates retrieving and parsing web archive MODS files.
- config ([None, classes.general.Config]): Config object.

**Returns**:

- locgov_data.LocGovRecords

#### get_search

```python
def get_search(n=0) -> bool
```

**Example usage**:

```python
# Fetch search metadata for items in the collection
# "10th-16th Century Liturgical Chants"

import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
```

```python
# Fetch search metadata for the top 100 Chronicling # Ameria page-level results mentioning "influenza" in the 1920s.

import locgovdata as ld
chronam_influenza = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/chronicling-america/?dl=page&start_date=1920-01-01&end_date=1929-12-31&ops=AND&qs=influenza&searchType=advanced",
)
fetch_search_success = chronam_influenza.get_search(100)
```

Perform loc.gov search, if item_type is 'search'.

**Inputs**:

- n(int): If an integer greater than 0 is supplied, only the top n results will be fetched.
  A zero will return all results.

**Updates**:

- self.item_ids
- self.resource_ids
- self.segment_ids
- self.search_metadata
- self.search_metadata_json
- self.item_count

**Returns**:

- bool: True if search runs and finds results. False if error is encountered or search returns no results.

#### get_items

```python
def get_items() -> bool
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
fetch_items_success = liturgical_chants.get_items()
```

Fetches item-level records.

If the input CSV has a resource_id column and not an item_id column, loc.gov resource records will first be retrieved to get item ids. No other information from resource records is retained.

After item records are fetched, information is parsed into dataframes for: items, resources, segments, files, and errors.

**Inputs**:

**Updates**:

- self.item_ids (adds item IDs for resources from the CSV or for resource-level records in search results)
- self.items
- self.resources
- self.segments
- self.files

**Returns**:

- bool: True if requests run. Note: True may be returned if errors are encountered. False if no requests run.

#### download_files_segments

```python
def download_files_segments(
    selected_mimetypes: list,
    dest="./downloads/"
    ) -> dict
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
fetch_search_success = liturgical_chants.get_items()
download_results_dict  = liturgical_chants.download_files_segments()
```

Function to download those files in item records that hang from resource segments, e.g., https://www.loc.gov/item/08018934/?fo=json&at=resources.0.files.0.5.

Requires that self.files_segments already be populated.

This function can only collect files that hang off of segments. To download resource-level files (that are not hanging from segments), use download_files_resources().

Note: This function will download _all_ sizes of IIIF JPEG files if "image/jpeg" is
a requested mimetype. Use the abstracted download_jpegs() for better handling.

Mimetypes recognized by this function are those recognized by the `mimetypes`
third party Python library.

**Inputs**:

- selected_mimetypes (list): A list of mimetype strings. This can be used to filter
  to a certain subset of files to download, e.g., TIFFs, JP2s, or MP3s.
  Mimetype strings will be matched to the `mimetype` field in loc.gov
  item JSON records, e.g.,
  https://www.loc.gov/item/08018934/?fo=json&at=resources.0.files.0.5
- dest (str): Local directory to download files into.

**Updates**:

**Returns**:

- dict. Two keys, `downloaded` and `skipped`. Values are lists of
  download URLs

#### download_files_resources

```python
def download_files_resources(
    keys: list,
    dest="./downloads/"
    ) -> dict
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
fetch_search_success = liturgical_chants.get_items()
download_results_dict  = liturgical_chants.download_files_resources()
```

Function to download those files in item records that hang directly from
resources, e.g.,
https://www.loc.gov/item/08018934/?fo=json&at=resources.0.fulltext_derivative.

Requires that self.files_resources already be populated.

This and download_files_segments() are lower level functions. For general
use, it is recommended to use more abstracted functions, like
download_full_text() and download_tiffs(). These functions have built-in logic
to group files by type (e.g., full text, image, audio).

**Inputs**:

- keys (list): A list of dictionary keys, e.g., fulltext_derivative, image.
  For a full list of possible values, see the Config object definition's
  `self.top_level_files` list.
- dest (str): Local directory to download files into.

**Updates**:

**Returns**:

- dict. Two keys, `downloaded` and `skipped`. Values are lists of
  download URLs

#### fulltext_alto

```python
def fulltext_alto() -> pd.DataFrame
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
fetch_search_success = liturgical_chants.get_items()
alto_df  = liturgical_chants.fulltext_alto()
```

Filters self.files_segments to only Alto XML files. Returns a dataframe of
Alto XML files with columns that match those of self.files_segments.

**Pattern**:

- Alto XML urls are found in the segment-level 'url' field and end
  in .alto.xml

**Inputs**:

**Updates**:

- self.fulltext_alto_df (pd.DataFrame): Same as dataframe returned.

**Returns**:

- pd.DataFrame. Dataframe of Alto XML files. Columns match those of
  self.file_segments.

#### fulltext_plaintext

```python
def fulltext_plaintext() -> pd.DataFrame
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
fetch_search_success = liturgical_chants.get_items()
plaintext_df  = liturgical_chants.fulltext_plaintext()
```

Filters self.files_resources to only plain text files from Alto and Djvu full
text. Returns a dataframe of plain text files with columns that match those
of self.files_resources.

NOTE that Chronicling America does not typically include resource-level full
text files. Use fulltext_alto() instead.

**Pattern**:

- Alto-based plain text files are found in the resource-level 'text_file'
  field and end in '.text.txt'
- Djvu-based plain text files are found in 'fulltext_file' inside 'resources'
  and end in '\_djvu.txt'

**Inputs**:

**Updates**:

- self.fulltext_plaintext_df (pd.DataFrame): Same as returned dataframe.

**Returns**:

- pd.DataFrame. Dataframe of plain text files. Columns match those of
  self.files_resources.

#### fulltext_tei

```python
def fulltext_tei() -> pd.DataFrame
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
fetch_search_success = liturgical_chants.get_items()
tei_df  = liturgical_chants.fulltext_tei()
```

Filters self.files_resources to XML (TEI) files from audio/visual
transcriptions. Returns a dataframe with columns that match those
of self.files_resources.

NOTE these files are most common in Veterans History Project (VHP)
collections.

**Pattern**:

- TEI files are found in the resourcelevel 'fulltext_file' field and end in '.xml'

**Inputs**:

**Updates**:

- self.alto_tei_df (pd.DataFrame): Same as the returned dataframe.

**Returns**:

- pd.DataFrame. Dataframe of XML (TEI) files from a/v transcriptions. Columns
  match those of self.file_resources.

#### alto_word_df

```python
def alto_word_df(alto_urls: list) -> pd.DataFrame
```

**Example usage**:

```python
import locgovdata as ld
liturgical_chants = ld.LocGovRecords(
    input_type="search",
    input_path="https://www.loc.gov/collections/tenth-to-sixteenth-century-liturgical-chants/about-this-collection/",
)
fetch_search_success = liturgical_chants.get_search()
fetch_search_success = liturgical_chants.get_items()
alto_word_df  = liturgical_chants.alto_word_df()
```

Fetches ATLO XML files and parses words into a single dataframe.

Upon error, returns an empty dataframe and logs error message.

**Inputs**:

- alto_urls (list): List of ALTO XML files with word coordinates

**Updates**:

- self.alto_word_df (pd.DataFrame): Same as the returned dataframe.

**Returns**:

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

#### get_locgov_records

```python
def get_locgov_records(get_items=True, n=0, save=False)
```

Used by the locgov_data CLI for non-coders.
Searches loc.gov item records in bulk. Input type can be either 'search' or
'csv'. Search is a https://loc.gov/search/ string. CSV is a path to a local
CSV file with an 'item_ids' column with IDs as LCCNs or loc.gov item URLs.

User can also supply user_agent header, which will tag their traffic for loc.gov
staff to use when troubleshooting server issues. Suggested string is an email
address or URL to app with contact info.

**Inputs**:

- get_items (bool): whether to also get items, or only get research results. Note
  that if the input is a csv, this will be ignored and items will be retrieved.
- n (int): If an integer greater than 0 is  
  supplied, only the top n results will be
  fetched. A zero will return all results.
- save (bool): whether to save the results to CSV files.

**Updates**:

- same as locgov_data.LocGovRecords.get_items(). Also saves output files.

**Returns**:

#### get_marc_df

```python
def get_marc_df() -> pd.DataFrame
```

Retrieves MARC records for all items in self.item_ids. Converts MARC metadata into a dataframe.

**Dependencies**:

- self.item_ids - need to be populated with rows with "item_id" values

**Inputs**:

**Updates**:

**Outputs**:

- pd.DataFrame: Dataframe of MARC fields for each item in self.item_ids.

### WebArchives

```python
class WebArchives(LocGovRecords)
```

Class for batch collection of web archive metadata from loc.gov and
webarchives.loc.gov. Sub-class of LocGovRecords.

```python
def __init__(self, parent_class_object=None)
```

**Inputs**:

- parent_class_object (locgov_data.LocGovRecords): Parent
  class, a LocGovRecords object.

**Returns**:

- locgov_data.WebArchives

#### WebArchives.get_mods_url

```python
def get_mods_url(df: pd.DataFrame) -> pd.DataFrame
```

Fetches and parses the MODS URLs.

**Inputs**:

- df (pd.DataFrame): Generally self.items or self.search_metadata.
  Must contain column "items.other_formats"

**Updates**:

**Returns**:

- pd.DataFrame. updated version of df DataFrame. If parsing was
  unsuccessful, un-updated version is returned.

#### WebArchives.get_mods_uselection

```python
def get_mods_uselection(chunk_size=100)
```

Requests MODS records, and parses information relevant to US Election
candidates. Splits large collections into smaller chunks (default 100
items) to avoid data loss due to script failing and/or 429s.

**Inputs**:

- chunk_size (int): For large MODS lists, chunks lists into segments of this size
  for processing. Default = 100

**Updates**:

- self.seeds - DataFrame from self.items, with MODS metadata and converted
  from item- to seed-level.
- self.seeds_by_year - self.seeds exploded by year (equivalent to digiboard `records`)

**Returns**:

- self.seeds DataFrame or None if an unknown error is encountered.

#### WebArchives.get_subject_facets_campains

```python
def get_subject_facets_campains(soup, campaign_year) -> list
```

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

**Inputs**:

- soup (bs4.BeautifulSoup.soup): Soup object from MODS XML record
- campaign year (int): Campaign year, for which facet information should
  be parsed

**Updates**:

**Returns**:

- list. List of subject facets.

#### WebArchives.make_metadata_csv

```python
def make_metadata_csv()
```

Saves a publicly publishable version of metadata.csv for a data package.

**Inputs**:

**Updates**:

- self.metadata_csv - pd.DataFrame version of metadata.csv

**Returns**:

- bool. True if a metadata.csv was created and self.metadata_csv
  updated. False if failed to create.

## Functions

### General functions

#### express_search

```python
def express_search(
    search_url: str,
    c="",
    n=0,
    at="results,pagination",
    headers=None,
    session=None,
    only_items=True,
    config=None,
) -> list
```

**Example usage**:

```python
import locgovdata as ld
search_results = ld.express_search(
    "https://www.loc.gov/search/?q=cook-book",
    c=200,
    headers={"User_Agent":"myemail@loc.gov"},
)
```

Get all results for a loc.gov search URL, iterating through pages of results.

Optionally can filter out non-items from results (e.g., events, collection
pages, research centers).

Optionally can return only the top n results, sorted by relevance.

Each page is checked for a timeout error. If a timeout is encountered, the
script waits 5 seconds and re-requests the page.

If search result pagination is cut off and fewer results are returned than
expected, an error is logged and printed to the terminal. Script continues.

Note: If the `c` parameter is too large, JSON response may be cut off and
non-valid.

This function is based on work from the Library of Congress's Data
Transformation Services experiment code, developed by AVP (weareavp.com).

**Inputs**:

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

**Returns**:

- List of search results, e.g., 'results'
  list from https://www.loc.gov/search/?q=cook-book&fo=json&at=results
- Returns empty list if errors or no results. Error message to log and terminal.

#### make_request

```python
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
    config=None
) -> tuple[bool, Union[str, bytes, list, dict]]
```

**Example usage**:

```python
import locgovdata as ld
my_marc_xml = ld.make_request(
    "https://lccn.loc.gov/2020449244/marcxml",
    headers={"User_Agent":"myemail@loc.gov"},
    max_attempts=5,
    timeout=30,
)
```

```python
import locgovdata as ld
my_config = ld.Config(
    debug=True,
    log="./project-logs",
    verbose=True,
    pause=4,
    user_agent="my-email@loc.gov my-project-1"
)
my_json_item = ld.make_request(
    "https://www.loc.gov/item/08018934/",
    params={"at":"resources,options.is_partial"},
    locgov_json=True,
    max_attempts=5,
    timeout=30,
    config=config
)
```

Makes a request for a URL.

**Inputs**:

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
  on retries.
- locgov_json (bool): True means that the response is a loc.gov JSON record
- other_json (bool): True means that the response is a JSON record (this is
  ignored if locgov_json = True)
- is_blocked (bool): True means that the server has already returned a 429.
  This is for use in loops where you'd like to hault all requests in the
  event of a 429 status code.
- config ([None, classes.general.Config]): Config object.

**Returns**:

A tuple of two values:

- is_blocked (bool): True means that the server has already returned a 429.
  This is for use in loops where you'd like to hault all requests in the
  event of a 429 status code.
- result
  - JSON-like object if locgov_json or json are True (i.g., dict
    or list);
  - binary if successful and not anticipating JSON; or
  - a string error message beginning "ERROR -".

**Error handling**:

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
  - on final attempt, timeouts
  - status code 403
- Returns `True, 'ERROR - BLOCKED'`:
  - status_code 429 (too many requests, blocked by server)

#### flatten_json

```python
def flatten_json(
    record: Union[dict, list],
    donotparse=None,
    donotparse_regex=None,
    parse_lists=True,
    prefix="",
    new_record=None,
    config=None
) -> dict
```

**Example usage**:

```python
import locgovdata as ld
my_json_item = ld.make_request(
    "https://www.loc.gov/item/08018934/",
    params={"at":"resources,options.is_partial"},
    headers = {"User-Agent": "Test Project, myemail@gmail.com"},
    locgov_json=True,
)
search_results = ld.flatten_json(
    my_json_item,
    donotparse=["files"],
    donotparse_regex=[r"fulltext_*"]
    parse_lists=False
)
```

Flatten/unfold a JSON or JSON-like object.

**Inputs**:

- record (Union[dict, list]): JSON-like dictionary or list.
- donotparse (list): List of keys to not flatten, at any level of the input record.
- donotparse_regex (list): List of regular expressions. Any keys matching these expressions
  will not be flattened.
- parse_lists (bool): Whether or not to expand lists. E.g., split some_list into
  columns like some_list_1, some_list_2, etc.
- prefix (str): Used when looping through levels. Do not pass any values
  to this parameter.
- new_record (): Used when looping through levels. Do not pass any values
  to this parameter.
- config ([None, classes.general.Config]): Config object.

**Returns**:

- Union[dict, list]. Flattened version of the input record.

#### flatten_locgov

```python
def flatten_locgov(records: list, config=None) -> list
```

Standard pattern for flattening loc.gov items for public data packages. Can accept
full item records or search result items. Relies on `flatten_json()`.

Does not expand lists. When converted to df, the list will remain in a single column.

Does not expand dictionaries that are simply facet filter links (e.g., 'contributors')
or the item.item dictionary.

Does not expand the "files" section of "resources" key.

**Inputs**:

- records (list): List of loc.gov item records (dicts) or search result records (dicts).
- config ([None, classes.general.Config]): Config object.

**Returns**:

- list: List of flattened loc.gov item records or search results.

#### df_to_csv

```python
def df_to_csv(
    df: pd.DataFrame,
    csv_file: str,
    append=False,
    config=None,
    **kwargs
) -> bool
```

Save dataframe to CSV.
If the CSV already exists and `append` is True, the data is appended to
existing CSV. If `append` is False, existing CSV is overwritten.
If the CSV is open, user is prompted to close the CSV and try again,
not save but continue with script, or quit the script.

**Inputs**:

- df (pd.DataFrame): Dataframe to save.
- csv_file (str): Path to CSV output file.
- append (bool): If the output CSV already exists, it will be updated with
  append new lines to the CSV. If the output CSV does not already exist,
  this parameter is disregarded.
- config ([None, classes.general.Config]): Config object.
- \*\*kwargs: Any additional parameters will be input directly into the pandas
  .to_csv() function

**Returns**:

- bool. True if CSV file is saved/updated. False if data failed to save to CSV.

#### download_file

```python
def download_file(
    url,
    dest,
    headers=None,
    is_blocked=False,
    overwrite=False,
    timeout=60*30,
    get_filesize=False,
    session=None,
    config=None
) -> tuple[bool, bool]
```

General function to download a file.

**Inputs**:

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

**Returns**:

- tuple[bool, bool] - is_blocked, which indicates if the request received a 429
  response, and a second boolean indicating if the download was successful.

#### download_from_df

```python
def download_from_df(
    df: pd.DataFrame,
    src="src",
    dest="dest",
    timeout=60*30,
    session=None,
    config=None
) -> dict
```

Download files from a dataframe with columns 'src' and dest'

**Inputs**:

- df (pd.DataFrame): Dataframe that has a source column with URLs
  and destination column with paths to save files.
- src (str): Name of source column with URLs
- dest (str): Name of destination column with file paths to which files will be saved.
- timeout (int): Max number of seconds to wait for each download. Note that the
  source server may set a lower limit.
- session (requests.Session): Python request Session. If you are making
  multiple requests, it is significantly more efficient to set up a requests.
  If none is supplied, a session will be created within the function.
  Session to share across requests. See
  https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
- config ([None, classes.general.Config]): Config object.

**Returns**:

- dict. Two keys, `downloaded` and `skipped`. Values are lists of
  download URLs.

#### csv_to_df

```python
def csv_to_df(
    csv_path,
    *args,
    config=None,
    **kwargs
) -> Union[pd.DataFrame, None]
```

Loads CSV file as a Pandas DataFrame.

**Inputs**:

- csv_path (str): Relative or absolute path to the input CSV file.
- \*args: Additional positional arguments passed to pd.read_csv().
- config ([None, classes.general.Config]): Config object.
- \*\*kwargs: Additional keyword arguments passed to pd.read_csv().

**Returns**:

- pd.DataFrame. Dataframe if successful, None if file doesn't exist or
  otherwise unsuccesful.

#### df_to_chunks

```python
def df_to_chunks(df: pd.DataFrame, chunk_size, config=None) -> list
```

Converts a df to a list of smaller dfs.

**Inputs**:

- df (pd.DataFrame): Input dataframe.
- chunk_size (int): Size (in rows) of each chunk
- config ([None, classes.general.Config]): Config object.

**Returns**:

- list. List of dataframes.

#### filter_dict

```python
def filter_dict(requested_keys: dict, input_dict: dict, config=None) -> dict
```

Takes an input dictionary with a second dictionary to map selected keys to new
keys. For example, if the `requested_keys` is {"s": "states", "c":"cities"} and
the `input_dict` is {"s":["a", "b"], "c": ["a", "b"], "r": ["a"]}, the output
would be {"states":["a", "b"], "cities": ["a", "b"]}.

**Inputs**:

- requested_keys (dict): Dictionary of requested keys, and mapped values for output
  dictionary. Keys are the source keys, values are the intended output keys.
- input_dict (dict): Input dictionary to be filtered and mapped.
- config ([None, classes.general.Config]): Config object.

**Returns**:

- dict. Filtered dictionary with mapped keys.

#### verify_mimetype

```python
def verify_mimetype(mimetype, config=None) -> bool
```

Validates whether an input string is a mimetype. Uses mimetypes recognized by
the `mimetypes` Python library to validate. If the input string is not
recognized, difflib makes suggestions.

**Inputs**:

- mimetype (str): a string that may be a mimetype
- config ([None, classes.general.Config]): Config object.

**Returns**:

- bool. True if confirmed a mimetype. False if not a confirmed mimetype

#### is_url

```python
def is_url(url) -> bool
```

Confirms if input is an http(s) URL. Returns boolean. False for ftp and s3 urls.
Does not confirm if the URL is valid, only if it matches the pattern of a URL.

**Inputs**:

- url (str): String to be confirmed as a URL

**Returns**:

- bool. True if a URL, False if not.

#### move_df_column

```python
def move_df_column(
    df: pd.DataFrame, column_name: str, target_index: int
) -> pd.DataFrame
```

Moves the specified column to the given index position in the DataFrame.

**Inputs**

- df (pd.DataFrame): The DataFrame to modify.
- column_name (str): The name of the column to move.
- target_index (int): The target index position to move the column to.

**Returns**

- pd.DataFrame: The modified DataFrame with the column moved.

### jupyter

Jupyter notebook functions

#### review_images_jupyter

```python
def review_images_jupyter(
    image_urls: list,
    is_blocked=False,
    session=None,
    config=None
)
```

**Example Usage:**

_For Jupyter notebooks only_

```python
import locgov_data as ld
images = [
    'https://tile.loc.gov/image-services/iiif/public:gdcmassbookdig:reninnobenshoho008800:reninnobenshoho008800_0101/full/pct:50.0/0/default.jpg',
    'https://tile.loc.gov/image-services/iiif/public:gdcmassbookdig:roshiyakakumeiun008800:roshiyakakumeiun008800_0001/full/pct:50.0/0/default.jpg'
    ]
ld.review_images_jupyter(images)
```

Takes a list of image URLs of any format, and displays the images
for review in a Jupyter Notebook. This function can only be run within
a notebook, otherwise will have no effect.

**Inputs**:

- image_urls (list): list of image URLs. Can be IIIF.
- is_blocked (bool): True means that the server has already returned a 429. This is for use in loops where you'd like to hault all requests in the event of a 429 status code.
- session (requests.Session): Python request Session. If you are making
  multiple requests, it is significantly more efficient to set up a requests.
  If none is supplied, a session will be created within the function.
  Session to share across requests. See
  https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
- config - config ([None, classes.general.Config]): Config object.

**Returns**:

- Nothing. Displays images in a Jupyter notebook.

### fulltext

Full text functions

#### altoxml_to_df

```python
def altoxml_to_df(alto_url, session=None, config=None) -> pd.DataFrame
```

Takes a single ALTO XML file and outputs a pandas dataframe of strings.
Ignores white spaces. Can return an empty dataframe.

**Inputs**:

- alto_url (str): ALTO XML file URL
- session (requests.Session): Python request Session. If you are making
  multiple requests, it is significantly more efficient to set up a requests.
  If none is supplied, a session will be created within the function.
  Session to share across requests. See
  https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
- config ([None, classes.general.Config]): Config object.

**Returns**:

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

#### altoxmls_to_df

```python
def altoxmls_to_df(alto_urls, session=None, config=None) -> pd.DataFrame
```

Takes a list of ALTO XML urls and converts into a single dataframe for all.

**Inputs**:

- alto_urls (list): List of ALTO XML URLs
- session (requests.Session): Python request Session. If you are making
  multiple requests, it is significantly more efficient to set up a requests.
  If none is supplied, a session will be created within the function.
  Session to share across requests. See
  https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
- config ([None, classes.general.Config]): Config object.

**Returns**:

- pd.DataFrame: Aggregated dataframe combining words from all ALTO XML files.

### marcxml

MARC XML functions

#### get_marcxml_record

```python
def get_marcxml_record(
    lccn,
    beautifulsoup=False,
    is_blocked=False,
    session=None,
    config=None
) -> Tuple(bool, Union[ET.Element, BeautifulSoup])
```

Pull a record via permalink for a given LCCN and return the MARCXML

**Inputs**:

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

**Returns**:

- Union[ET.Element, BeautifulSoup]: A single MARCXML record or an error string from MARCXML_ERRORS

#### marcxml_to_df

```python
def marcxml_to_df(xml_data: str, config=None) -> pd.DataFrame
```

Takes MARC XML data as a string and outputs a single-row dataframe (assuming
the XML is a single record).

**Inputs**:

- xml_data (str): XML data, as a string
- config (classes.config.Config): Config object.

**Returns**:

- pd.DataFrame: Dataframe representing the MARC XML data. One row per record
  (usually one row)

#### marcxml_to_sdf

```python
def marcxml_to_df(xml_data: str, config=None) -> pd.DataFrame
```

Takes MARC XML data as a string and outputs a simplified version
of the dataframe than marcxml*to_df() would output. Blank indicators are shown as "*"
and subfield labels (e.g., "$a") are included as a single string with values. This
way, the CSV appears similar to how a MARC record might render in the Catalog "MARC
tags" interface.

**Inputs**:

- xml_data (str): XML data, as a string
- config (classes.config.Config): Config object.

**Returns**:

- pd.DataFrame: Dataframe representing the MARC XML data. One row per record
  (usually one row)

#### get_marc_field

```python
def get_marc_field(df: pd.DataFrame, field: str, config=None) -> pd.DataFrame
```

Takes the output of `get_marc_df()` and a field, and returns a new dataframew
with one row per value from that field. Output columns include: field_name,
field_value, lccn, item_id, 001

**Inputs**:

- df (pd.DataFrame): Pandas dataframe, output of get_marc_df()
- field (str): field (e.g., '985') to isolate and parse
- config (classes.config.Config): Config object.

**Returns**:

- pd.DataFrame: Dataframe with one row per instance of requested field.
  Columns will be split into subfields and indicators. For repeat instances of
  the same subfield, these will appear numbered from 1 as in "subfield.a_1",
  "subfield.a_2"
