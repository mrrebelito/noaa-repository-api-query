from api_query import Query
import sys
import csv
import json
from datetime import datetime

class Menu:
    '''Display a menu and respond to choices when run.'''
    def __init__(self):
        self.api = Query()
        self.choices = {
                "1": self.view_collection_data,
                "2": self.get_JSON_of_collection_data,
                "3": self.get_csv_of_collection_titles,
                "4": self.quit
                }

    def display_menu(self):
        print("""
Query NOAA Resposistory JSON REST API

1. View JSON collection data
2. Get JSON of collection data 
3. Get CSV of collection (by title and link)
4. Quit
    """)

    def run(self):
        '''Display the menu and respond to choices.'''
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
        self.collections()
        collection = input("Select a collection: ")
        data = self.api.query_collection(collection)
        print(json.dumps(data,indent=4))
        print("")

    def get_JSON_of_collection_data(self):
        self.collections()
        collection = input("Select a collection: ")
        data = self.api.query_collection(collection)

        jsonfile = "noaa_json_" +datetime.now().strftime("%Y_%m_%d")+  ".json"
        with open(jsonfile, 'w') as f:
            for record in data['response']['docs']:
                json.dump(record,f, indent=4, sort_keys=True)

    def get_csv_of_collection_titles(self):
        self.collections()
        collection = input("Select a collection: ")
        data = self.api.query_collection(collection)
        title_link = self.api.query_collection_by_title_and_link(data)
        
        csvfile = "noaa_titles_" +datetime.now().strftime("%Y_%m_%d")+  ".csv"
        with open(csvfile, 'w', newline='') as f:
            fh = csv.writer(f, delimiter='\t')
            fh.writerow(["Title", "Link"])
            for t,l in title_link:
                fh.writerow([t,l])

        print("")
        print("CSV file created: " + csvfile)

    def quit(self):
        print("")
        print("Goodbye.")
        print("")
        sys.exit(0)

if __name__ == "__main__":
    Menu().run()
    