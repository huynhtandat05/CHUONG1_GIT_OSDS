from selenium import webdriver
from selenium.webdriver.common.by import By
import time

options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
driver = webdriver.Chrome(options=options)

url = "https://en.wikipedia.org/wiki/List_of_painters_by_name_beginning_with_%22P%22"
driver.get(url)

time.sleep(2)

# Lấy UL chính xác chứa danh sách painters
ul_painters = driver.find_element(By.XPATH, "(//div[@class='mw-parser-output']/ul)[1]")

# Lấy tất cả li trong UL đó
li_tags = ul_painters.find_elements(By.TAG_NAME, "li")

# Lấy URL
links = []
titles = []

for li in li_tags:
    a = li.find_element(By.TAG_NAME, "a")   # mỗi painter có 1 thẻ a
    links.append(a.get_attribute("href"))
    titles.append(a.get_attribute("title"))

# In URL
print("------ URL ------")
for link in links:
    print(link)

# In Title
print("------ TITLE ------")
for title in titles:
    print(title)
