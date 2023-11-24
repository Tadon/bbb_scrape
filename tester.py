from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time


driver = webdriver.Chrome()
driver.get('https://www.bbb.org/')
time.sleep(5)

topic_search = driver.find_element(By.ID, ":Rjalal4pa:")
location_search = driver.find_element(By.ID, ":Rlalal4pa:")

topic_search.send_keys('Auto Repair')
location_search.send_keys('')