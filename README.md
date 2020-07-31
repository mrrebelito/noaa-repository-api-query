## Python tools for interacting with NOAA Institutional Repository REST API

The [NOAA Institutional Repository](https://repository.library.noaa.gov/) has a REST API that which outputs JSON.

The following Python scripts allow you to inteact with the API.

### CLI Menu


This tool provides a menu to view or individual collections as well as download all items from IR. All packages used within the files are builtins, with the exception of requests.

#### Usage

Download api_query.py and menu.py and place in the same. Run ```python menu.py``` in the shell or terminal. Doing so start the following menu:

```
Query NOAA Resposistory JSON REST API

1. Get CSV of collection
2. Get CSV of all items
3. Quit
```

You can also use ```api_query.py``` which ```menu.py``` uses as to retrieve data from the JSON API.

### Generate Docker datasette image, Publish on Heroku

Use ```generate_db.py``` to generate a sqlite database of all NOAA IR holdings. 

Columns for the DB can be customized```generate_db``` by using the code within ```api_query.py``.

#### Publish SQLite db as Docker Datasette image or to Heroku

Use CLI script ```publish.db``` to either:
- publish Datasette Docker image;
- publisher to Heroku

