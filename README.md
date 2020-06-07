# Python tools for interacting with NOAA Institutional Repository REST API

The [NOAA Institutional Repository](https://repository.library.noaa.gov/) has a REST API that which outputs JSON.

The following Python scripts allow you to inteact with the API.

## CLI Menu

 This tool provides a menu to view or individual collections as well as download all items from IR. All packages used within the files are builtins, with the exception of requests.

### Usage

Download api_query.py and menu.py and place in the same. Run the following in shell:

```python menu.py ```

If there is no issue, you should be greeted with the following:

```
 Query NOAA Resposistory JSON REST API

    1. Get CSV of collection
    2. Get CSV of all items
    3. Quit

```

Or, you can simply use ```api_query.py``` on its own. 

## Add API data to Sqlite DB

This program allows you to leverage the IR API via ```api_query```, downloading data and adding to to a sqlite database. Once a db has been created you can also launch a datasette instance either locally or through heroku. 