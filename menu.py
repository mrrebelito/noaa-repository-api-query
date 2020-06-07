import os
import sys
import csv
import json
from datetime import datetime
from api_query import RepositoryQuery, DataExporterm, Menu

""" 
Module used to provide an interactive command-line menu api_query.py 
module.py

Contains Menu class which will return IR collection data in the form 
of a CSV or JSON file in the following ways: 
1) individual collections via JSON
2) individual collections (title and item link) via CSV
3) all collections (title and item link) via CSV
"""


class Menu:

    data_info_json = datetime.now().strftime("%Y_%m_%d") + ".json"
    date_info_csv = datetime.now().strftime("%Y_%m_%d") + ".csv"

    """
    Display a menu and respond to choices when run method is executed.
    """
    def __init__(self):
        self.query = RepositoryQuery()
        self.export = DataExporter()
        self.choices = {
                "1": self.get_csv_of_collection_titles,
                "2": self.get_csv_of_all_items,
                "3": self.exit_menu
                }

    def display_menu(self):
        print("""
Query NOAA Resposistory JSON REST API

    1. Get CSV of collection
    2. Get CSV of all items
    3. Quit
    """)

    def run(self):
        """
        Call method on Menu class to display the menu and respond to choices.
        """
        while True:
            self.display_menu()
            choice = input("Enter an option: ")
            action = self.choices.get(choice)
            if action:
                action()
            else:
                print("{0} is not a valid choice".format(choice))

    @staticmethod
    def collections():
        print("")
        print("Enter the number associated with collection to retrieve data:")
        print("")
        print("NEPA : 1")
        print("Coral Reef Conservation Program: 3")
        print("Ocean Exploration Program: 4")
        print("National Marine Fisheries Service : 5")
        print("National Weather Service: 6")
        print("Office of Oceanic and Atmospheric Research: 7")
        print("National Ocean Service: 8")
        print("National Environmental Satellite and Data Information Service: 9")
        print("Sea Grant Publications: 11")
        print("Education and Outreach: 12")
        print("NOAA General Documents: 10031")
        print("NOAA International Agreements: 11879")
        print("Office of Marine and Aviation Operations (OMAO): 16402")
        print("Integrated Ecosystem Assessment (IEA): 22022")
        print("NOAA Cooperative Institutes: 23649")
        print("")  
        
    def get_csv_of_collection_titles(self):
        """
        Method returns individually selected collection in form of CSV
        which includes fields for title and item link.
        """
        self.collections()
        collection_pid = input("Select a collection: ")
        
        # export collection
        self.export.export_collection_as_csv(self.query, collection_pid)


    def get_csv_of_all_items(self):
        """
        Method creates a deduplicated title and link list of all items in the IR.
        """
        # calls api.get method which call JSON API to retrieve all collections
        
        # export all collections
        self.export.export_all_collections_as_csv(self.query,
            self.query.get_all_ir_data())

    def exit_menu(self):
        print("")
        print("Bye Bye.")
        print("")
        os._exit(0)
        

if __name__ == "__main__":
   m = Menu()
   m.run()