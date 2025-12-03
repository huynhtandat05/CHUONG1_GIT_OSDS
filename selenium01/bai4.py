from selenium import webdriver
from selenium.webdriver.common.by import By
import time

options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
driver = webdriver.Chrome(options=options)

for i in range(ord(""), ord("Z")+1):      # A → Z
    try:
        letter = chr(i)
        url = f"https://en.wikipedia.org/wiki/List_of_painters_by_name_beginning_with_%22{letter}%22"

        print("\n===== ĐANG LẤY CHỮ:", letter, "=====")

        driver.get(url)
        time.sleep(2)

        # Lấy đúng UL đầu tiên chứa danh sách painters
        ul = driver.find_element(By.XPATH, "(//div[@class='mw-parser-output']/ul)[1]")

        # Lấy tất cả li
        li_tags = ul.find_elements(By.TAG_NAME, "li")

        # Lấy title của từng thẻ a
        titles = []
        for li in li_tags:
            a = li.find_element(By.TAG_NAME, "a")
            titles.append(a.get_attribute("title"))

        # In kết quả
        for t in titles:
            print(t)

    except Exception as e:
        print("Error:", e)

driver.quit()

