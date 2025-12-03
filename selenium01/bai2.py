from selenium import webdriver
from selenium.webdriver.common.by import By
import time

options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
driver = webdriver.Chrome(options=options)

url = "https://en.wikipedia.org/wiki/List_of_painters_by_name"
driver.get(url)

time.sleep(2)

tags = driver.find_elements(By.XPATH, "//a[contains(@title,'List of painters')]")

links = [tag.get_attribute("href") for tag in tags]

for link in links:
    print(link)
