import pandas as pd

FILE_ID = "1G8DHyYC5oBUepETIWR-xxtZbXcIRsNk5"
file_url = f"https://drive.google.com/uc?id={FILE_ID}"
raw_data = pd.read_csv(file_url)  # читаем файл
print(raw_data.head(10))  # выводим на экран первые 10 строк для проверки
