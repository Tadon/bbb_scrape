import psycopg2
import requests
from bs4 import BeautifulSoup
import time
from scrape_functions import Scrape_Functions
from search_information import SearchInformation
import psycopg2.extras

#creating session with retry functionality incase of http error, and db connection params
session = Scrape_Functions.requests_retry_session()
db_params = SearchInformation.db_params

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
for city_num, cities in iterating_library.items():
    for city in cities:
        
        for category in SearchInformation.popular_categories:
            counter = 1
            while True:
                url = f'https://www.bbb.org/search?find_country=USA&find_loc={city}&find_text={category}&page={counter}'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0'
                }
                print(url)
                success = False
                #connecting to website
                for attempt in range(1000):
                    try:
                        r = session.get(url, headers=headers, timeout = 10)
                        content = r.content.decode()

                        if 'You are being rate limited' in content or 'temporarily banned' in content:
                            print('Rate limited, pausing...')
                            time.sleep(0.5)
                            continue
                        success = True
                        print('connected')
                        break
                    except requests.exceptions.RequestException as e:
                        if attempt >= 999:
                            print('Max attempts reached, moving to next.')
                            print('Error:', str(e))
                            break
                        time.sleep(0.1)
                    
                if not success:
                    continue
                
                #initial soup used to grab each business profile url  
                main_soup = BeautifulSoup(r.content, 'html.parser')
                #creating library of all business areas needed to scrape business profile url
                data_blocks = main_soup.find_all('div', class_ ='result-item-ab exws2cl0 css-z34rva e1ri33r70')
                #batch data list to upload to db
                batch_data = []
                #iterating through the areas (blocks) scraped from main soup
                batch_businesses = 0
                for block in data_blocks:
                    #grabbing name and phone to create unique id
                    business_phone = Scrape_Functions.get_business_phone(block)
                    business_name = Scrape_Functions.find_name(block)
                    unique_id = Scrape_Functions.get_unique_id(business_name, business_phone)
                    
                    if unique_id not in existing_numbers and unique_id != '_No Unique Id_':
                    #creating object with all information contained within certain class, in the case 'a'    
                        anchor = block.find('a', class_ = 'text-blue-medium css-1jw2l11 eou9tt70')
                        if anchor:
                            #searching through each anchor ('a' class) for a href variable, then assigning it to a usable variable
                            html = anchor['href'] if anchor else '_No Hyperlink Found_'
                            #now diving into business profile to extract required data, if url is present
                            
                            if html != '_No Hyperlink Found_':
                                #new request to business profile page                
                                new_request = requests.get(html, headers=headers)
                                #new soup of business profile page
                                business_profile_soup = BeautifulSoup(new_request.content, 'html.parser')
                                #business rating function
                                business_rating = Scrape_Functions.get_bbb_rating(business_profile_soup)
                                #business accreditation
                                is_accredited = Scrape_Functions.is_accredited(business_profile_soup)
                                #business license number if exists
                                business_license_number = Scrape_Functions.get_business_license_number(business_profile_soup)
                                #years in business
                                years_in_business = Scrape_Functions.get_years_in_business(business_profile_soup)
                                #when business started
                                start_date = Scrape_Functions.get_business_start_date(business_profile_soup)
                                #when business incorporated
                                business_incorporation_date = Scrape_Functions.get_incorporation_date(business_profile_soup)
                                #business address
                                business_address = Scrape_Functions.get_business_address(business_profile_soup)
                                #business website
                                business_website = Scrape_Functions.get_business_website(business_profile_soup)
                                #entity type
                                entity_type = Scrape_Functions.get_entity_type(business_profile_soup)
                                #what the business does
                                business_category = Scrape_Functions.get_category(business_profile_soup)
                                #Different business contacts
                                business_contacts = Scrape_Functions.get_contact_info(business_profile_soup)
                                #if any additional info exists
                                additional_info = Scrape_Functions.get_additional_contact_information(business_profile_soup)
                                area_exchange_code = business_phone[:6]
                                phone_carrier = phone_carrier_dictionary.get(area_exchange_code, '_No Carrier Found_')
                                existing_numbers[unique_id] = ''

                                business_data = (unique_id,business_name,business_phone,business_rating,is_accredited,business_license_number,years_in_business,start_date,business_incorporation_date,business_address,business_website,entity_type,business_category,business_contacts,additional_info,phone_carrier)
                                batch_data.append(business_data)
                                
                                total_businesses += 1
                                batch_businesses += 1
                                print(f'Business #{batch_businesses} added to batch')
                                
                            
                query = '''INSERT INTO bbb_data ("Unique ID", "Business Name", "Business Phone", "Business Rating", "Accredited?", "License Number", "Years in Business", "Start Date", "Incorporation Date", "Business Address", "Business Website", "Entity Type", "Business Category", "Business Contacts", "Additional Information", "Phone Carrier")
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''   
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, query, batch_data)
                    conn.commit()
                print(f'{batch_businesses} added this batch!Total businesses added is {total_businesses}!')

                next_page_exists = main_soup.find('a', rel = 'next')
                if next_page_exists:
                    counter+= 1
                else:
                    break
                
        print('Finished!')

            



