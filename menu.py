import os
import sys
import csv
import json
from datetime import datetime
from api_query import Query

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
    """
    Display a menu and respond to choices when run method is executed.
    """
    def __init__(self):
        self.api = Query()
        self.choices = {
                "1": self.view_collection_data,
                "2": self.get_JSON_of_collection_data,
                "3": self.get_csv_of_collection_titles,
                "4": self.get_csv_of_all_items,
                "5": self.exit_menu
                }

    def display_menu(self):
        print("""
Query NOAA Resposistory JSON REST API

    1. View JSON collection data
    2. Get JSON of collection data 
    3. Get CSV of collection (by title and link)
    4. Get CSV of all items (by title and link)
    5. Quit
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
        print("Deep Water Horizon: 2")
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
        print("")  
        
    def view_collection_data(self):
        """
        Method used to select collection once data pull decision
        has been made. 
        """
        self.collections()
        collection = input("Select a collection: ")
        data = self.api.query_collection(collection)
        print(json.dumps(data,indent=4))
        print("")

    def get_JSON_of_collection_data(self):
        """
        Method returns individually selected collection in form of JSON. 
        """
        self.collections()
        collection = input("Select a collection: ")
        data = self.api.query_collection(collection)

        jsonfile = "noaa_json_" +datetime.now().strftime("%Y_%m_%d")\
            + ".json"
        with open(jsonfile, 'w') as f:
            for record in data['response']['docs']:
                json.dump(record,f, indent=4, sort_keys=True)

        print("")
        print("JSON file created: " + jsonfile)

    def get_csv_of_collection_titles(self):
        """
        Method returns individually selected collection in form of CSV
        which includes fields for title and item link.
        """
        self.collections()
        collection = input("Select a collection: ")
        data = self.api.query_collection(collection)
        title_link = self.api.query_collection_by_title_and_link(data)
        
        csvfile = "noaa_titles_" +datetime.now().strftime("%Y_%m_%d")\
            + ".csv"
        with open(csvfile, 'w', newline='') as f:
            fh = csv.writer(f, delimiter='\t')
            fh.writerow(["Title", "Link"])
            for t,l in title_link:
                fh.writerow([t,l])

        print("")
        print("CSV file created: " + csvfile)

    def get_csv_of_all_items(self):
        """
        Method creates a deduplicated title and link list of all items in the IR.
        """
        # calls api.get method which call JSON API to retrieve all collections
        data = self.api.get_collections()    
        csvfile = "noaa_collections_" +datetime.now().strftime("%Y_%m_%d")\
            + ".csv"
        deduped_csvfile = "noaa_collections_final_" +datetime.now().\
            strftime("%Y_%m_%d")+ ".csv"     
        with open(csvfile, 'w', newline='') as fw:
            fh = csv.writer(fw, delimiter='\t')
            fh.writerow(["Title", "Link"])
            for collection in data:
                title_link = self.api.query_collection_by_title_and_link(collection)
                for t,l in title_link:
                    fh.writerow([t,l])

        # deduplicate files
        f = set(open(csvfile).readlines())
        open(deduped_csvfile,'w').writelines(f)
        os.remove(csvfile)

        print("")
        print("CSV file created: " + deduped_csvfile)

    def exit_menu(self):
        print("")
        print("Bye Bye.")
        print("")
        os._exit(0)
        

if __name__ == "__main__":
   m = Menu()
   m.run()