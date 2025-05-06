import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

def main():
    try:
        base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
       
        print("Đang tải danh sách files...")
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
       
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
       
        if not table:
            raise Exception("Không tìm thấy bảng dữ liệu")
       
        target_timestamp = "2024-01-19 10:27"
        target_file = None
       
        print(f"Đang tìm file được chỉnh sửa lúc {target_timestamp}...")
        for row in table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) >= 2 and cols[1].text.strip() == target_timestamp:
                target_file = cols[0].text.strip()
                break
               
        if not target_file:
            raise Exception(f"Không tìm thấy file với timestamp {target_timestamp}")
       
        file_url = f"{base_url}{target_file}"
        print(f"Đang tải file: {target_file}")
       
        file_response = requests.get(file_url, timeout=30)
        file_response.raise_for_status()

        with open(target_file, 'wb') as f:
            f.write(file_response.content)
           
        print("Đang phân tích dữ liệu thời tiết...")
        df = pd.read_csv(target_file)
       
        df['HourlyDryBulbTemperature'] = pd.to_numeric(
            df['HourlyDryBulbTemperature'],
            errors='coerce'
        )
       
        max_temp = df['HourlyDryBulbTemperature'].max()
        hot_days = df[df['HourlyDryBulbTemperature'] == max_temp]
       
        print(f"\nNhiệt độ cao nhất: {max_temp}°F")
        print("\nCác bản ghi có nhiệt độ cao nhất:")
       
        cols_to_show = [
            'STATION', 'DATE', 'HourlyDryBulbTemperature',
            'HourlyRelativeHumidity', 'HourlyWindSpeed',
            'HourlyPrecipitation'
        ]
       
        print(hot_days[cols_to_show].to_string(index=False))
       
    except requests.exceptions.RequestException as e:
        print(f"Lỗi kết nối: {e}")
    except pd.errors.EmptyDataError:
        print("Lỗi: File tải về không có dữ liệu")
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        if 'target_file' in locals() and os.path.exists(target_file):
            os.remove(target_file)
            print("\nĐã xóa file tạm")

if __name__ == "__main__":
    main()

