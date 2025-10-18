from pathlib import Path
import re
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text

PATH = Path(r"C:\Users\stnva\proj1\ironalloys.parquet")

# Сначала удалим из датасета все специальные символы и пробелы
bad_chars = re.compile(r"[^A-Za-z0-9_]+")


def clean_string(text: str | None) -> str | None:
    # Пробелы заменим на «_».
    # Всё, что не латиница/цифры/«_» удалим
    if text is None:
        return text
    text = text.replace(" ", "_")
    return bad_chars.sub("", text)


df = pd.read_parquet(PATH)

# Чистим имена колонок
df.columns = [clean_string(col) for col in df.columns]

# Чистим содержимое всех текстовых столбцов
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].astype(str).map(clean_string)

print("Колонки после очистки:")
print(list(df.columns))

first_col = df.columns[0]
print(f"\nЗначения первого столбца «{first_col}»:")
print(df[first_col].tolist())

# Сохраним чистый датасет
df.to_parquet(PATH.with_stem(PATH.stem + "_cleaned"))

db_creds_path = "C:/Users/stnva/proj1/creds.db"

# Посмотрим содержимое creds.db
with sqlite3.connect(db_creds_path) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM access LIMIT 1;")
    row_values = cursor.fetchone()
    col_names = [desc[0] for desc in cursor.description]
    creds = dict(zip(col_names, row_values))

# Запишем учетные данные
url = creds.get("url")
port = creds.get("port")
user = creds.get("user")
password = creds.get("pass")

# Подключаемся к БД homeworks
conn_homeworks = f"postgresql+psycopg2://{user}:{password}@{url}:{port}/homeworks"
engine = create_engine(conn_homeworks, echo=False, future=True)
print("Подключились к homeworks")

# Загрузим датасет
PATH_clean = Path(r"C:\Users\stnva\proj1\ironalloys_cleaned.parquet")
df = pd.read_parquet(PATH_clean).head(100)

# Запишем датасет в БД
df.to_sql(
    name="ustinova",
    con=engine,
    schema="public",
    if_exists="replace",
    index=False,
)
print("Таблица public.ustinova создана и заполнена")

# Проверим, что натворили
print("Проверка:")
with engine.connect() as conn:
    # Посмотрим таблицы
    tables = conn.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public';"
        )
    ).fetchall()
    print("Таблицы", [t[0] for t in tables])

    # Посмотрим первые 5 строк
    result = conn.execute(text("SELECT * FROM public.ustinova LIMIT 5;"))
    rows = result.fetchall()
    # Достанем имена колонок
    col_names = result.keys()
    print("Содержимое ustinova:")
    for row in rows:
        print(dict(zip(col_names, row)))
