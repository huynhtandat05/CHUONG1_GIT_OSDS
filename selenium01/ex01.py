from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
import time

# Đường dẫn đến file thực thi geckodriver
gecko_path = r"c:\\Users\\DEll\\Downloads\\geckodriver.exe\\geckodriver.exe"

# Khởi tạo đối tượng dịch vụ với đường geckodriver
ser = Service(gecko_path)

# Tạo tùy chọn Firefox đúng cách
options = Options()

# Đường dẫn đúng đến firefox.exe (KHÔNG dùng Firefox Installer)
options.binary_location = r"c:\\Program Files\\Mozilla Firefox\\firefox.exe"

# Hiển thị giao diện
options.headless = False

# Khởi tạo driver
driver = webdriver.Firefox(options=options, service=ser)

# Tạo url
url = 'http://pythonscraping.com/pages/javascript/ajaxDemo.html'

# Truy cập
driver.get(url)

# In ra nội dung của trang web
print("Before: ============================\n")
print(driver.page_source)

# Tạm dừng khoảng 3 giây
time.sleep(3)

# In lại
print("\n\n\nAfter: ============================\n")
print(driver.page_source)

# Đóng browser
driver.quit()
