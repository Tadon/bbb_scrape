import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import re
import hashlib
import requests
from bs4 import BeautifulSoup
import psycopg2.extras
import time
class Scrape_Functions:
    @staticmethod
    def find_name(block):
        try:
            business_name_block = block.find('a', class_='text-blue-medium css-1jw2l11 eou9tt70')    
            business_name = business_name_block.find('span').text        
            return business_name
        except:
            
            return '_Business Not Found_'
    @staticmethod
    def get_bbb_rating(business_profile_soup):
        try:
            spans = business_profile_soup.find_all('span')
            for span in spans:
                if 'BBB rating' in span.get_text():
                    rating_span = span.find_previous_sibling('span')
                    if rating_span:
                        return rating_span.get_text(strip=True)
        except:
            return '_Rating Not Found_'
    @staticmethod
    def get_business_phone(block):
        try:
            business_phone_block = block.find('p', class_= 'bds-body css-1u1ibea e230xlr0')
            business_phone = business_phone_block.find('a').text
            if business_phone:
                business_phone = business_phone.translate({ord(c): None for c in '()- '})
                business_phone = ''.join(filter(str.isdigit, business_phone))
                return business_phone
            return '_No Phone Provided_'
        except:
            return f'_No Phone Provided_'
    @staticmethod
    def is_accredited(business_profile_soup):
        try:
            accredited_element = business_profile_soup.find('img', alt='BBB accredited business')
            if accredited_element:
                return 'BBB Accredited'
            else:
                return 'Not BBB Accredited'
        except Exception as e:
            return f'_Error: {str(e)}_'
    @staticmethod
    def get_business_license_number(business_profile_soup):
        try:
            cluster_elements = business_profile_soup.find_all('div', class_='cluster')
            for cluster in cluster_elements:
                bds_body_elements = cluster.find_all('div', class_= 'bds-body')
                for element in bds_body_elements:
                    if 'license number' in element.text:
                        license_text = element.text
                        license_number = license_text.split('license number of ')[1].split(' for')[0]
                        return license_number
            return '_License Not Found_'
        except Exception as e:
            return f'_Error: {str(e)}_'
    @staticmethod
    def get_years_in_business(business_profile_soup):
        try:
            cluster_elements = business_profile_soup.find_all('div', class_= 'cluster')
            for cluster in cluster_elements:
                if 'Years in Business:' in cluster.text:
                    years = cluster.find('dd').text.strip()
                    return years
            return '_Years in Business Not Found_'
        except Exception as e:
            return f'_Error: {str(e)}_'
    @staticmethod
    def get_business_start_date(business_profile_soup):
        try:
            cluster_elements = business_profile_soup.find_all('div', class_= 'cluster')
            for cluster in cluster_elements:
                if 'Business Started:' in cluster.text:
                    start_date = cluster.find('dd').text.strip()
                    return start_date
            return '_Start Date Not Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_incorporation_date(business_profile_soup):
        try:
            cluster_elements = business_profile_soup.find_all('div', class_= 'cluster')
            for cluster in cluster_elements:
                if 'Business Incorporated:' in cluster.text:
                    business_incorporated = cluster.find('dd').text.strip()
                    return business_incorporated
            return '_Incorporation Date Not Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_business_address(business_profile_soup):
        try:
            address_elements = business_profile_soup.find('div', class_= 'dtm-address stack')
            if address_elements:
                business_address = address_elements.find('dd').text.strip()
                return business_address
            return '_Address Not Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_business_website(business_profile_soup):
        try:
            website_element = business_profile_soup.find('a', class_= 'dtm-url')
            if website_element:
                website = website_element['href']
                return website
            return '_Website Not Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_entity_type(business_profile_soup):
        try:    
            cluster_elements = business_profile_soup.find_all('div', class_= 'cluster')
            for cluster in cluster_elements:
                if 'Type of Entity:' in cluster.text:
                    entity_type = cluster.find('dd').text.strip()
                    return entity_type
            return '_Entity Type Not Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_category(business_profile_soup):
        try:
            category_element = business_profile_soup.find('div', class_='text-size-4 text-gray-70')
            if category_element:
                category = category_element.text
                return category
            return '_No Category Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_contact_info(business_profile_soup):
        try:
            contact_info_title = business_profile_soup.find('dt', class_= 'bds-h5', text = 'Contact Information')
            
            if not contact_info_title:
                return '_Contact Information Not Found'
            business_contact_container = contact_info_title.find_next_sibling('dd')

            if not business_contact_container:
                return '_Contact Information Not Found_'
            
            business_contact_list = []

            contacts = business_contact_container.find_all('p', class_= 'bds-body')
            for contact in contacts:
                title = contact.get_text(strip=True)

                ul = contact.find_next('ul')
                if ul:
                    li = ul.find('li')
                    if li:
                        name_and_title = li.get_text(strip=True)
                        business_contact_list.append(f'{title}: {name_and_title}')
            contact_info = ' | '.join(business_contact_list)
            if contact_info:
                return contact_info
            return '_Contact Info Not Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_additional_contact_information(business_profile_soup):
        try:
            additional_contact_title = business_profile_soup.find('dt', class_= 'bds-h5', text='Additional Contact Information')
            if not additional_contact_title:
                return '_Additional Contact Not Found_'
            additional_contact_container = additional_contact_title.find_next_sibling('dd')
            if not additional_contact_container:
                return '_Additional Contact Not Found_'
            
            additional_contact_list = []
            contacts = additional_contact_container.find_all('p', 'bds-body')
            for contact in contacts:
                title = contact.get_text(strip=True)
                ul = contact.find_next('ul')
                if ul:
                    for li in ul.find_all('li'):
                        a = li.find('a')
                        if a:
                            contact_info = a.get_text(strip=True)
                            if re.match(r'\(\d{3}\) \d{3}-\d{4}', contact_info):
                                contact_info = re.sub(r'\D', '', contact_info)
                            additional_contact_list.append(f'{title}: {contact_info}')
                
            return ' | '.join(additional_contact_list) if additional_contact_list else '_Additional Contacts Not Found_'
        except Exception as e:
            return f'_Exception {str(e)}_'
    @staticmethod
    def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500,502,504), session=None):
        session = session or requests.Session()
        retry = Retry(
            total = retries,
            read = retries,
            connect = retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            )  
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session  
    @staticmethod
    def get_unique_id(business_name, business_phone):
        try:
            if business_phone != '_No Phone Provided_':
                data_string = f'{business_name}{business_phone}'
                hash_object = hashlib.md5(data_string.encode())
                return hash_object.hexdigest()
            return '_No Unique Id_'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def process_city_category(city, category, db_params, phone_carrier_dictionary,existing_numbers, lock):
        print('Thread started')
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        counter = 1
        total_counter = 0
        success = False
        batch_data = []
        session = Scrape_Functions.requests_retry_session()
        while True:
            url = f'https://www.bbb.org/search?find_country=USA&find_loc={city}&find_text={category}&page={counter}'
            headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0'
            }
            for attempt in range(1000):
                try:
                    r=session.get(url, headers=headers, timeout=10)
                    content = r.content.decode()

                    if 'You are being rate limited' in content or 'temporarily banned' in content:
                        print('Rate limited, pausing...')
                        time.sleep(0.5)
                        continue
                    success = True
                    print('Connected')
                    break
                except requests.exceptions.RequestException as e:
                    if attempt >= 999:
                        print(f'Max attenots reacged, Error: {e}')
                        break
                    time.sleep(5)
            if not success:
                continue

            search_results_page = BeautifulSoup(r.content, 'html.parser')
            data_blocks = search_results_page.find_all('div', class_= 'result-item-ab exws2cl0 css-z34rva e1ri33r70')
            business_counter = 0
            for block in data_blocks:
                business_phone = Scrape_Functions.get_business_phone(block)
                business_name = Scrape_Functions.find_name(block)
                unique_id = Scrape_Functions.get_unique_id(business_name, business_phone)

                if unique_id not in existing_numbers and unique_id != '_No Unique Id_':
                    anchor = block.find('a', class_='text-blue-medium css-1jw2l11 eou9tt70')

                    if anchor:
                        html = anchor['href'] if anchor else '_No Hyperlink found_'

                        if html != '_No Hyperlink found_':
                            new_request = requests.get(html, headers=headers)
                            business_profile_soup = BeautifulSoup(new_request.content, 'html.parser')
                            business_rating = Scrape_Functions.get_bbb_rating(business_profile_soup)
                            is_accredited = Scrape_Functions.is_accredited(business_profile_soup)                            
                            business_license_number = Scrape_Functions.get_business_license_number(business_profile_soup)                          
                            years_in_business = Scrape_Functions.get_years_in_business(business_profile_soup)                       
                            start_date = Scrape_Functions.get_business_start_date(business_profile_soup)                          
                            business_incorporation_date = Scrape_Functions.get_incorporation_date(business_profile_soup)                           
                            business_address = Scrape_Functions.get_business_address(business_profile_soup)                         
                            business_website = Scrape_Functions.get_business_website(business_profile_soup)                          
                            entity_type = Scrape_Functions.get_entity_type(business_profile_soup)                          
                            business_category = Scrape_Functions.get_category(business_profile_soup)                         
                            business_contacts = Scrape_Functions.get_contact_info(business_profile_soup)                          
                            additional_info = Scrape_Functions.get_additional_contact_information(business_profile_soup)
                            area_exchange_code = business_phone[:6]
                            phone_carrier = phone_carrier_dictionary.get(area_exchange_code, '_No Carrier Found_')
                            business_counter += 1
                            business_data = (unique_id,business_name,business_phone,business_rating,is_accredited,business_license_number,years_in_business,start_date,business_incorporation_date,business_address,business_website,entity_type,business_category,business_contacts,additional_info,phone_carrier)
                            
                            with lock:
                                if unique_id not in existing_numbers:
                                    existing_numbers[unique_id] = ''
                                    batch_data.append(business_data)
                            print(f'Business #{business_counter} added from page {counter} of {category} in {city}')
            total_counter += business_counter
            print(f'{business_counter} entries being batched to database. Total count for this thread is {total_counter}.')
            query = '''INSERT INTO bbb_data ("Unique ID", "Business Name", "Business Phone", "Business Rating", "Accredited?", "License Number", "Years in Business", "Start Date", "Incorporation Date", "Business Address", "Business Website", "Entity Type", "Business Category", "Business Contacts", "Additional Information", "Phone Carrier")
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''' 
            if batch_data:
                psycopg2.extras.execute_batch(cur, query, batch_data)
                conn.commit()
                batch_data.clear()
            next_page_exists = search_results_page.find('a', rel = 'next')
            if next_page_exists:
                counter+= 1
            else:
                break    
        cur.close()
        conn.close()
    
