import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import re
import os # Thêm thư viện để kiểm tra/xóa file DB (tùy chọn)

######################################################
## I. Cấu hình và Chuẩn bị
######################################################

# Thiết lập tên file DB và Bảng
DB_FILE = 'Painters_Data.db'
TABLE_NAME = 'painters_info'
all_links = []

# Tùy chọn cho Chrome (có thể chạy ẩn nếu cần, nhưng để dễ debug thì không dùng)
# chrome_options = Options()
# chrome_options.add_argument("--headless") 

# Nếu muốn bắt đầu với DB trống, có thể xóa file cũ (Tùy chọn)
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print(f"Đã xóa file DB cũ: {DB_FILE}")

# Mở kết nối SQLite và tạo bảng nếu chưa tồn tại
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Tạo bảng
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    name TEXT PRIMARY KEY, -- Sử dụng tên làm khóa chính để tránh trùng lặp
    birth TEXT,
    death TEXT,
    nationality TEXT
);
"""
cursor.execute(create_table_sql)
conn.commit()
print(f"Đã kết nối và chuẩn bị bảng '{TABLE_NAME}' trong '{DB_FILE}'.")

# Hàm đóng driver an toàn
def safe_quit_driver(driver):
    try:
        if driver:
            driver.quit()
    except:
        pass

######################################################
## II. Lấy Đường dẫn (URLs)
######################################################

print("\n--- Bắt đầu Lấy Đường dẫn ---")

# Lặp qua ký tự 'F' (chr(70))
for i in range(70, 71): 
    driver = None
    try:
        driver = webdriver.Chrome() # Khởi tạo driver cho phần này
        url = "https://en.wikipedia.org/wiki/List_of_painters_by_name_beginning_with_%22"+chr(i)+"%22"
        driver.get(url)
        time.sleep(3)

        # Lấy tất cả thẻ ul
        ul_tags = driver.find_elements(By.TAG_NAME, "ul")
        
        # Thử chọn chỉ mục (index) 20. Cần kiểm tra lại nếu index này thay đổi.
        if len(ul_tags) > 20:
            ul_painters = ul_tags[19] 
            li_tags = ul_painters.find_elements(By.TAG_NAME, "li")

            # Lọc các đường dẫn hợp lệ (có thuộc tính href)
            links = [tag.find_element(By.TAG_NAME, "a").get_attribute("href") 
                     for tag in li_tags if tag.find_elements(By.TAG_NAME, "a")]
            all_links.extend(links)
        else:
            print(f"Lỗi: Không tìm thấy thẻ ul ở chỉ mục 20 cho ký tự {chr(i)}.")

    except Exception as e:
        print(f"Lỗi khi lấy links cho ký tự {chr(i)}: {e}")
    finally:
        safe_quit_driver(driver) # Đóng driver sau khi xong phần này

print(f"Hoàn tất lấy đường dẫn. Tổng cộng {len(all_links)} links đã tìm thấy.")

######################################################
## III. Lấy thông tin & LƯU TRỮ TỨC THỜI
######################################################

print("\n--- Bắt đầu Cào và Lưu Trữ Tức thời ---")
count = 0
for link in all_links:
    # Giới hạn số lượng truy cập để thử nghiệm nhanh
    if (count >= 20): # Đã tăng lên 5 họa sĩ để có thêm dữ liệu mẫu
        break
    count = count + 1

    driver = None
    try:
        driver = webdriver.Chrome() 
        driver.get(link)
        time.sleep(2)

        # 1. Lấy tên họa sĩ
        try:
            name = driver.find_element(By.TAG_NAME, "h1").text
        except:
            name = ""
        
        # 2. Lấy ngày sinh (Born)
        try:
            birth_element = driver.find_element(By.XPATH, "//th[text()='Born']/following-sibling::td")
            birth = birth_element.text
            # Trích xuất định dạng ngày (ví dụ: 12 June 1900)
            birth_match = re.findall(r'[0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}', birth)
            birth = birth_match[0] if birth_match else ""
        except:
            birth = ""
            
        # 3. Lấy ngày mất (Died)
        try:
            death_element = driver.find_element(By.XPATH, "//th[text()='Died']/following-sibling::td")
            death = death_element.text
            death_match = re.findall(r'[0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}', death)
            death = death_match[0] if death_match else ""
        except:
            death = ""
            
        # 4. Lấy quốc tịch (Nationality)
        try:
            nationality_element = driver.find_element(By.CSS_SELECTOR, "table.infobox .birthplace")
            # Cần lấy text và chỉ lấy phần tử đầu tiên nếu có nhiều quốc tịch
            nationality = nationality_element.text.split('\n')[0]
        except:
            nationality = ""

        safe_quit_driver(driver)
        
        # 5. LƯU TỨC THỜI VÀO SQLITE
        insert_sql = f"""
        INSERT OR IGNORE INTO {TABLE_NAME} (name, birth, death, nationality) 
        VALUES (?, ?, ?, ?);
        """
        # Sử dụng 'INSERT OR IGNORE' để bỏ qua nếu Tên (PRIMARY KEY) đã tồn tại
        cursor.execute(insert_sql, (name, birth, death, nationality))
        conn.commit()
        print(f"  --> Đã lưu thành công: {name}")

    except Exception as e:
        print(f"Lỗi khi xử lý hoặc lưu họa sĩ {link}: {e}")
        safe_quit_driver(driver)


        
print("\nHoàn tất quá trình cào và lưu dữ liệu tức thời.")

######################################################
## IV. Truy vấn SQL Mẫu và Đóng kết nối
######################################################

"""
A. Yêu Cầu Thống Kê và Toàn Cục
1. Đếm tổng số họa sĩ đã được lưu trữ trong bảng.
2. Hiển thị 5 dòng dữ liệu đầu tiên để kiểm tra cấu trúc và nội dung bảng.
3. Liệt kê danh sách các quốc tịch duy nhất có trong tập dữ liệu.

B. Yêu Cầu Lọc và Tìm Kiếm
4. Tìm và hiển thị tên của các họa sĩ có tên bắt đầu bằng ký tự 'F'.
5. Tìm và hiển thị tên và quốc tịch của những họa sĩ có quốc tịch chứa từ khóa 'French' (ví dụ: French, French-American).
6. Hiển thị tên của các họa sĩ không có thông tin quốc tịch (hoặc để trống, hoặc NULL).
7. Tìm và hiển thị tên của những họa sĩ có cả thông tin ngày sinh và ngày mất (không rỗng).
8. Hiển thị tất cả thông tin của họa sĩ có tên chứa từ khóa '%Fales%' (ví dụ: George Fales Baker).

C. Yêu Cầu Nhóm và Sắp Xếp
9. Sắp xếp và hiển thị tên của tất cả họa sĩ theo thứ tự bảng chữ cái (A-Z).
10. Nhóm và đếm số lượng họa sĩ theo từng quốc tịch.
"""
print("\n==============================")
print("A. THỐNG KÊ VÀ KIỂM TRA")
print("==============================")

# 1. Tổng số họa sĩ
print("\n1. Tổng số họa sĩ:")
cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
print(cursor.fetchone()[0])

# 2. 5 dòng đầu tiên
print("\n2. 5 dòng đầu tiên:")
df_first5 = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} LIMIT 5;", conn)
print(df_first5)

# 3. Quốc tịch duy nhất
print("\n3. Danh sách quốc tịch duy nhất:")
df_nat = pd.read_sql_query(
    f"""
    SELECT DISTINCT nationality 
    FROM {TABLE_NAME}
    WHERE nationality IS NOT NULL AND nationality <> '';
    """,
    conn
)
print(df_nat)


print("\n==============================")
print("B. LỌC VÀ TÌM KIẾM")
print("==============================")

# 4. Tên bắt đầu bằng F
print("\n4. Họa sĩ tên bắt đầu bằng 'F':")
df_f = pd.read_sql_query(
    f"SELECT name FROM {TABLE_NAME} WHERE name LIKE 'F%';",
    conn
)
print(df_f)

# 5. Quốc tịch chứa French
print("\n5. Họa sĩ có quốc tịch chứa 'French':")
df_french = pd.read_sql_query(
    f"SELECT name, nationality FROM {TABLE_NAME} WHERE nationality LIKE '%French%';",
    conn
)
print(df_french)

# 6. Không có quốc tịch
print("\n6. Họa sĩ không có quốc tịch:")
df_no_nat = pd.read_sql_query(
    f"SELECT name FROM {TABLE_NAME} WHERE nationality IS NULL OR nationality = '';",
    conn
)
print(df_no_nat)

# 7. Có cả ngày sinh và ngày mất
print("\n7. Họa sĩ có đầy đủ ngày sinh & ngày mất:")
df_life = pd.read_sql_query(
    f"SELECT name FROM {TABLE_NAME} WHERE birth <> '' AND death <> '';",
    conn
)
print(df_life)

# 8. Tên chứa 'Fales'
print("\n8. Họa sĩ có tên chứa 'Fales':")
df_fales = pd.read_sql_query(
    f"SELECT * FROM {TABLE_NAME} WHERE name LIKE '%Fales%';",
    conn
)
print(df_fales)


print("\n==============================")
print("C. NHÓM VÀ SẮP XẾP")
print("==============================")

# 9. Sắp xếp A–Z
print("\n9. Danh sách tên A-Z:")
df_sorted = pd.read_sql_query(
    f"SELECT name FROM {TABLE_NAME} ORDER BY name ASC;",
    conn
)
print(df_sorted)

# 10. Đếm số họa sĩ theo quốc tịch
print("\n10. Thống kê số lượng họa sĩ theo quốc tịch:")
df_group_nat = pd.read_sql_query(
    f"""
    SELECT nationality, COUNT(*) AS count_painters
    FROM {TABLE_NAME}
    WHERE nationality IS NOT NULL AND nationality <> ''
    GROUP BY nationality
    ORDER BY count_painters DESC;
    """,
    conn
)
print(df_group_nat)

# Đóng kết nối
conn.close()
print("\n>>> ĐÃ HOÀN THÀNH PHẦN IV – ĐÃ ĐÓNG KẾT NỐI DB.")