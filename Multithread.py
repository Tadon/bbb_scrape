import psycopg2
from concurrent.futures import ThreadPoolExecutor
from scrape_functions import Scrape_Functions
from search_information import SearchInformation
import psycopg2.extras
from threading import Lock
#creating session with retry functionality incase of http error, and db connection params
session = Scrape_Functions.requests_retry_session()
db_params = SearchInformation.db_params
lock = Lock()
#effecient iterating library
iterating_library = {}
for i in range(200):
    for state, cities in SearchInformation.city_and_states_full.items():
        if i < len(cities):
            key = f'City # {i+1}'
            city = cities[i][:-2]
            if key not in iterating_library:
                iterating_library[key] = []
            iterating_library[key].append(city)

#library of already existing numbers to ensure no duplicate entries
existing_numbers = {}
conn = psycopg2.connect(**db_params)
cur = conn.cursor()
cur.execute('SELECT "Unique ID" FROM bbb_data')
rows = cur.fetchall()
for row in rows:
    unique_id = row[0]
    existing_numbers[unique_id] = ''

#library of phone carriers so we can add phone carrier column to bbb db
phone_carrier_dictionary = {}
cur.execute('SELECT acex, "Company" FROM carrier_db')
rows = cur.fetchall()
for row in rows:
    acex = row[0]
    phone_carrier_dictionary[acex] = row[1]
#main scraping logic
total_businesses = 0

with ThreadPoolExecutor(max_workers = 5) as executor:
    for city_num, cities in iterating_library.items():
        for city in cities:
            for category in SearchInformation.popular_categories:
                executor.submit(Scrape_Functions.process_city_category,city,category,db_params,phone_carrier_dictionary,existing_numbers,lock)

            



