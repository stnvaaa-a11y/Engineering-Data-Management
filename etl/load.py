from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from dotenv import load_dotenv


def get_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Не задана переменная окружения: {name}")
    return val


def build_pg_url() -> str:
    host = get_env("DB_HOST")
    port = get_env("DB_PORT")
    user = get_env("DB_USER")
    password = get_env("DB_PASSWORD")
    dbname = get_env("DB_NAME")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    load_dotenv(dotenv_path=script_dir / ".env")

    path_clean = script_dir.parent / "data" / "ironalloys_cleaned.parquet"

    if not path_clean.exists():
        raise FileNotFoundError(f"Файл не найден: {path_clean}")

    df = pd.read_parquet(path_clean).head(100)

    # Читаем имя схемы и таблицы из переменных окружения
    schema = os.getenv("DB_SCHEMA", "public")
    table = os.getenv("DB_TABLE", "ustinova")

    # Подключаемся к БД
    url = build_pg_url()
    engine = create_engine(url, echo=False, future=True)

    # Пишем в БД
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"Таблица {schema}.{table} создана и заполнена")

    # Проверяем
    with engine.connect() as conn:
        tables = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema"
            ),
            {"schema": schema},
        ).fetchall()
        print("Таблицы в схеме", schema, ":", [t[0] for t in tables])

        result = conn.execute(text(f"SELECT * FROM {schema}.{table} LIMIT 1;"))
        rows = result.fetchall()
        col_names = result.keys()
        print("Первые строки:")
        for row in rows:
            print(dict(zip(col_names, row)))
