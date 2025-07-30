# locgov_data

Python staff library for retrieving public metadata and media files from loc.gov.

It offers simple commands for quick tasks and advanced tools for writing your own scripts.

This library is not formally supported by OCIO or LC developers.

For more info, see https://staff.loc.gov/wikis/display/DCM/How+to+-+locgov_data+Python+library

- [Installation](#installation)
  - [Install](#install)
  - [Update](#update)
- [For Coders: Customizable Programming Tools](#for-coders-customizable-programming-tools) -[Notebook](#notebook) -[Comprehensive documentation](#comprehensive-documentation)
- [For Non-Coders: Easy-to-use commands](#for-non-coders-easy-to-use-commands)
  - [Example usage (easy-to-use commands)](#example-usage-easy-to-use-commands)
  - [Parameters](#parameters)
  - [Input](#input)
  - [Outputs](#outputs)
- [Past releases](#past-releases)

## Installation

If this is your first time using Python, Jupyter Notebooks, or Git on your LC computer, see beginner setup instructions on Confluence at https://staff.loc.gov/wikis/display/DCM/How+to+-+locgov_data+Python+library#Howtolocgov_dataPythonlibrary-Setup. Those instructions will take you through the process of installing all requirements.

If you are installing on a Virtual Machine (VM), please consult your local team's documentation for your VM. Instructions for setting up a virtual environment in your VM can be found at https://staff.loc.gov/wikis/x/et3ZDQ.

### Install

```
pip install git+https://git.loc.gov/DCMS/digital-scholarship/locgov-data/
```

or

```
pip install git+ssh://git@git.loc.gov:7999/DCMS/digital-scholarship/locgov-data.git
```

or if you'd like to have a local copy of the code (or don't want to install git):

1. It's a good idea to first create and activate a virtual environment. See https://staff.loc.gov/wikis/x/et3ZDQ
1. Download this repo (or clone with git). If downloaded as a compressed file (e.g., a ZIP), uncompress.
1. cd to the top-level directory of the downloaded repo.
1. run `pip install .` (including the period)

### Update

```
pip install git+https://git.loc.gov/DCMS/digital-scholarship/locgov-data/ --upgrade
```

or

```
pip install git+ssh://git@git.loc.gov:7999/DCMS/digital-scholarship/locgov-data.git --upgrade
```

or if you are installing from a copy of this repo:

1. Assuming you're using a virtual environment, activate the virtual environment that has locgov_data installed. See https://staff.loc.gov/wikis/x/et3ZDQ
1. Download a fresh copy this repo (or use git to pull the main branch). If downloaded as a compressed file (e.g., a ZIP), uncompress.
1. run `pip uninstall locgov_data`
1. cd to the top-level directory of the downloaded repo.
1. run `pip install .` (including the period)

## For Coders: Customizable Programming Tools

For advanced users who want to built their own scripts, the full library of `locgov_data` functions helps you automate your loc.gov data tasks with custom code.

### Notebook

You can get started by running the **notebook** at https://git.loc.gov/DCMS/digital-scholarship/locgov-data/-/tree/main/notebook. Be sure to also download the other file(s) in the "notebook" folder.

### Comprehensive documentation

Refer to the **comprehensive documentation** for full details on all functions, classes, and features at https://git.loc.gov/DCMS/digital-scholarship/locgov-data/-/blob/main/docs/README.md. It's your complete guide to using the library in your own code.

## For Non-Coders: Easy-to-use commands

`locgov_data` comes with a set of simple commands to get basic metadata tasks done quickly and easily, no programming required. (Under the hood, this is called a **CLI**.)

The commands fetch data from loc.gov and save outputs to CSV files. They do not download media files.

### Example usage (easy-to-use commands)

**Example 1**

Retrieve loc.gov search metadata for AFC's Occupational Folklife Project collection. Flag your server traffic with `myemail@loc.gov` so OCIO knows its staff traffic. This will save 1 CSV file, to `outputs/search.csv`.

```
locgov_data -i "https://www.loc.gov/audio/?fa=partof:occupational+folklife+project" -u myemail@loc.gov --verbose
```

**Example 2**

Retrieve loc.gov search metadata **plus item-level metadata** for AFC's Occupational Folklife Project collection. Flag your server traffic with `myemail@loc.gov` so OCIO knows its staff traffic. This will save 5 CSV files to `outputs/` directory: `search.csv`, `items.csv`, `resources.csv`, `files_segments.csv` and `files_resources.csv`. Verbose output.

```
locgov_data -i "https://www.loc.gov/audio/?fa=partof:occupational+folklife+project" --get-items -u myemail@loc.gov --verbose
```

**Example 3**

**Input a CSV of LCCNs** (or loc.gov item URLs, resource IDs, or resource URLs) and retrieve search plus item-level metadata. Flag your server traffic with `myemail@loc.gov` so OCIO knows its staff traffic. This will save 5 CSV files to `outputs/` directory: `search.csv`, `items.csv`, `resources.csv`, `files_segments.csv` and `files_resources.csv`. Verbose output.

```
locgov_data -t 'csv' -i "/path/to/my/csv/list.csv" --get-items -u myemail@loc.gov --verbose
```

### Parameters

| short flat | long flat     | value type | description                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ---------- | ------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| -h         | --help        | n/a        | show this help message and exit                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -t         | --input-type  | str        | 'csv', 'CSV', or 'search' depending on whether your input is a CSV file of loc.gov IDs or a loc.gov search url. Default: `search`                                                                                                                                                                                                                                                                                                                                        |
| -i         | --input-path  | str        | loc.gov search URL or path to a CSV file. Default: `https://www.loc.gov/newspapers/?dates=1820/1821&fa=location_state:district+of+columbia`                                                                                                                                                                                                                                                                                                                              |
| -n         |               | int        | Collect only the top n results from a search input. This is ignored if the input is a CSV.                                                                                                                                                                                                                                                                                                                                                                               |
| -g         | --get-items   | None       | Whether to also get item-level records. If the input is a CSV list, this will be treated as True regardless of user input.                                                                                                                                                                                                                                                                                                                                               |
| -d         | --output-dir  | str        | Local directory to save outputs. Default: `./output/`                                                                                                                                                                                                                                                                                                                                                                                                                    |
| -p         | --pause       | int        | Base number of seconds to pause between requests. Default: `15`                                                                                                                                                                                                                                                                                                                                                                                                          |
|            | --env         | str        | Environment to operate in: prod (default), test, or dev. Only prod is publicly available. Default: prod                                                                                                                                                                                                                                                                                                                                                                  |
| -u         | --user-agent  | str        | Email address, app URL, or other identifier used to tag traffic. Default is blank. Recommendation for staff is to use your email address.                                                                                                                                                                                                                                                                                                                                |
| -l         | --log         | str        | Directory to save log files. Default: `./log/`                                                                                                                                                                                                                                                                                                                                                                                                                           |
| -o         | --log-debug   | None       | Sets logging to [DEBUG](https://docs.python.org/3/library/logging.html#logging.DEBUG) level, so that the log file and terminal output (if `-v` or `--verbose` are also used) includes all log messages DEBUG and higher. By default, only [WARNING](https://docs.python.org/3/library/logging.html#logging.WARNING) and higher are printed to the terminal, and only [INFO](https://docs.python.org/3/library/logging.html#logging.INFO) and higher go to the log files. |
| -v         | --verbose     | None       | Whether to print logs to the terminal. Use in combination with `-log-debug` to get more detailed logs.                                                                                                                                                                                                                                                                                                                                                                   |
| -e         | --is-election | None       | Whether this is targetting US Election web archives. If so, MODS files will be retrieved and parsed into an additional CSV.                                                                                                                                                                                                                                                                                                                                              |

### Input

Users can input either:

1. a loc.gov search URL
2. a CSV of loc.gov resource IDs/URLs - with column `resource_id`. This file can also have an `item_id` column, which will be ignored.
3. a CSV of loc.gov item IDs/URLs - with column `item_id`

If inputting a CSV (#2 or #3), no search will be conducted but item records will always be fetched. If inputting a CSV of resource IDs (#3), resource and item records will be fetched, but metadata is retuned only from only the item records. If inputting a search URL (#1), a search will be conducted and returned, and item records can optionally be fetched with the `--get-items` or `-g` flag.

For CSVs, the script will **automatically detect whether inputs are resource ids vs item ids**: if you have a `resource_id` column, it will assume all values are resources and will ignore the `item_id` column. If you only have an `item_id` column, it will assume item IDs.

If you are inputting a search URL, use `-t search` or `--input-type search`. The two mean the same thing (`-t` is short for `--input-type`).

If you are inputting a CSV, use `-t csv` or `--input-type csv`. If you capitalize "csv" that's also OK.

### Outputs

You can find examples of outputs in the `outputs` directory of this Git repo.

- **errors.csv** - List of request errors
- **search.csv** - Metadata from loc.gov search, partially flattened. Only returned if input is a search URL.
- **items.csv** - Metadata from loc.gov items, partially flattened. Always created for CSV inputs. Only created for search URL inputs if `--get-items` or `-g` flag.
- **resources.csv** - Metadata about resources. Extracted from items.csv Created whenever items.csv is created.
- **files_resources.csv** - Metadata about top-level files that hang directly from resources. Extracted from item.csv. Created whenever items.csv is created.
- **files_segments.csv** - Metadata about files tat hang from segments (pages) on a resource. Extracted from item.csv. Created whenever items.csv is created.
- **metadata.csv** - Publishable metadata.csv file for US Election web archives (only created if `is_election` is True). Metadata is mostly sourced from election MODS files linked from the item records, with the exception of one field.

The metadata from loc.gov is partially flattened according to a loc.gov scheme defined in [flatten_locgov()](https://git.loc.gov/DCMS/digital-scholarship/locgov-data/-/blob/main/docs/README.md?ref_type=heads#flatten_locgov).

## Past releases

See [releases](https://git.loc.gov/DCMS/digital-scholarship/locgov-data/-/releases) for the current and past releases as downloadable packages.

The current version is also available on the main branch, and can be installed using the installation instructions above.

Upcoming versions in development can be found on branches. Branches are named after versions, e.g., `v0.2.1`.
