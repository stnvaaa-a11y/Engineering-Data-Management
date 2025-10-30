from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import click
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from etl.extract import build_gdrive_url, load_dataset
from etl.transform import transform_file


ENV_PATH = Path(r"C:\Users\stnva\proj1\etl\.env")


@dataclass
class Config:
    PROJECT_DIR: Path
    CSV_PATH: Path
    PARQUET_PATH: Path

    FILE_ID: str

    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    DB_SCHEMA: str
    DB_TABLE: str


def load_environment_variables(file_id: str) -> Config:
    if not ENV_PATH.exists():
        raise FileNotFoundError(f".env не найден по пути: {ENV_PATH}")
    load_dotenv(dotenv_path=ENV_PATH)

    project_dir = Path(__file__).resolve().parent
    csv_path = project_dir / "data" / "data.csv"
    parquet_path = project_dir / "data" / "ironalloys_cleaned.parquet"

    return Config(
        PROJECT_DIR=project_dir,
        CSV_PATH=csv_path,
        PARQUET_PATH=parquet_path,
        FILE_ID=file_id,
        DB_HOST=os.getenv("DB_HOST", "localhost"),
        DB_PORT=int(os.getenv("DB_PORT", "5432")),
        DB_USER=os.getenv("DB_USER", "user"),
        DB_PASSWORD=os.getenv("DB_PASSWORD", "password"),
        DB_NAME=os.getenv("DB_NAME", "postgres"),
        DB_SCHEMA=os.getenv("DB_SCHEMA", "public"),
        DB_TABLE=os.getenv("DB_TABLE", "ustinova"),
    )


def etl_process(cfg: Config) -> None:
    print("Начинаем ETL-процесс")

    print("Скачиваем CSV с Google Drive...")
    url = build_gdrive_url(cfg.FILE_ID)
    _, out_csv = load_dataset(url)
    print(f"Готово, сохранено в {out_csv.resolve()}")

    print("Обрабатываем и сохраняем в Parquet...")
    df_out, saved_parquet = transform_file(verbose=True)
    print(f"Готово, Parquet: {saved_parquet.resolve()} (строк: {len(df_out)})")

    print("Загружаем в PostgreSQL...")
    url_obj = URL.create(
        "postgresql+psycopg2",
        username=cfg.DB_USER,
        password=cfg.DB_PASSWORD,
        host=cfg.DB_HOST,
        port=cfg.DB_PORT,
        database=cfg.DB_NAME,
    )
    engine = create_engine(url_obj, echo=False, future=True)

    df_out.to_sql(
        name=cfg.DB_TABLE,
        con=engine,
        schema=cfg.DB_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(
        f"Таблица {cfg.DB_SCHEMA}.{cfg.DB_TABLE} создана и заполнена ({len(df_out)} строк)"
    )

    with engine.connect() as conn:
        tables = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema"
            ),
            {"schema": cfg.DB_SCHEMA},
        ).fetchall()
        print("Таблицы в базе", cfg.DB_SCHEMA, ":", [t[0] for t in tables])

        result = conn.execute(
            text(f'SELECT * FROM "{cfg.DB_SCHEMA}"."{cfg.DB_TABLE}" LIMIT 5;')
        )
        rows = result.fetchall()
        col_names = result.keys()
        print("Первые строки:")
        for row in rows:
            print(dict(zip(col_names, row)))

    print("Ура! Мы закончили!!!")


@click.command()
@click.option(
    "--file-id",
    prompt="Введите Google Drive FILE_ID",
    help="Google Drive FILE_ID (например, 1G8DHyYC5oBUepETIWR-xxtZbXcIRsNk5).",
)
def cli(file_id: str):
    cfg = load_environment_variables(file_id=file_id)
    etl_process(cfg)


if __name__ == "__main__":
    cli()
