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

### Publish on Heroku

Use ```publish.py``` to generate a sqlite database and publish of all NOAA IR holdings on Heroku.

