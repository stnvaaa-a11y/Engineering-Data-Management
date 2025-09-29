from pathlib import Path
import re
from typing import Union
import pandas as pd
import numpy as np

FILE_ID = "1G8DHyYC5oBUepETIWR-xxtZbXcIRsNk5"
URL = f"https://drive.google.com/uc?id={FILE_ID}"

print("Скачиваем и читаем CSV...")

df = pd.read_csv(URL)  # читаем файл
print("До обработки")
print(df.info(memory_usage="deep"))
print(df.head(10))  # выводим на экран первые 10 строк для проверки


# Есть колонки, в которых в каждой ячейке написаны единицы измерения или данные приведены сразу в нескольких единицах.
print("Исправляем единицы измерения...")


def make_regex(units: list[str]) -> re.Pattern:
    joined = "|".join(map(re.escape, units))
    return re.compile(rf"([0-9]*\.?[0-9]+)\s*({joined})", re.I)


# Density оставим только g/cm3
density_re = make_regex(["g/cm3"])

just_number_re = re.compile(r"^[\s]*([0-9]*\.?[0-9]+)[\s]*$")

energy_suffix_re = make_regex(["MJ/m3", "kJ/m3"])


def parse_density(cell: str) -> Union[float, pd.NA]:
    # Возвратим только число, без единицы измерения
    if pd.isna(cell):
        return pd.NA
    m = density_re.search(str(cell))
    if m:
        return float(m.group(1))

    num = pd.to_numeric(cell, errors="coerce")
    return float(num) if not pd.isna(num) else pd.NA


def parse_energy(cell: str) -> Union[float, pd.NA]:
    # Вернем число, отбросив единицу измерения MJ/kJ, либо само число, если единицы измерения нет.
    if pd.isna(cell):
        return pd.NA

    num_only = just_number_re.match(str(cell))
    if num_only:
        return float(num_only.group(1))

    with_unit = energy_suffix_re.search(str(cell))
    if with_unit:
        return float(with_unit.group(1))

    return pd.NA


# Переименуем колонки, добавив единицы измерения
density_cols_old = ["Density min", "Density max"]
density_cols_new = [c + ", g/cm³" for c in density_cols_old]

ultimate_cols_old = [
    "Resilience: Ultimate (Unit Rupture Work) min",
    "Resilience: Ultimate (Unit Rupture Work) max",
]
ultimate_cols_new = [c + ", MJ/m³" for c in ultimate_cols_old]

modulus_cols_old = [
    "Resilience: Unit (Modulus of Resilience) min",
    "Resilience: Unit (Modulus of Resilience) max",
]
modulus_cols_new = [c + ", kJ/m³" for c in modulus_cols_old]


for old, new in zip(density_cols_old, density_cols_new):
    if old in df.columns:
        df[new] = df[old].map(parse_density).astype("Float32")

for old, new in zip(ultimate_cols_old, ultimate_cols_new):
    if old in df.columns:
        df[new] = df[old].map(parse_energy).astype("Float32")

for old, new in zip(modulus_cols_old, modulus_cols_new):
    if old in df.columns:
        df[new] = df[old].map(parse_energy).astype("Float32")

# Проверяем
keep_cols = density_cols_new + ultimate_cols_new + modulus_cols_new
print("\nИсправленные столбцы:")
print(df[keep_cols].head())
print(df[keep_cols].info())


# Теперь разберемся с оставшимися столбцами
# Учитываем, что пустая ячейка в колонках с химическим составом означает нулевое содержание,
# а пустая ячейка в колонках с физическим свойством - отсутствие данных
print("Приводим типы...")

text_cols = ["iron_alloy_name"]

first_comp, last_comp = "Iron (Fe)Fe min", "Lead (Pb)Pb max"
comp_cols = df.loc[:, first_comp:last_comp].columns.tolist()


def cast_numeric(s: pd.Series, empty_to_zero: bool = False) -> pd.Series:
    s = s.replace(r"^\s*$", np.nan, regex=True)
    s = pd.to_numeric(s, errors="coerce")

    if empty_to_zero:
        s = s.fillna(0)

    if (s.dropna() % 1 == 0).all():
        return s.astype("Int32")
    return s.astype("Float32")


for col in df.columns:
    if col in text_cols:
        continue

    if col in comp_cols:
        df[col] = cast_numeric(df[col], empty_to_zero=True)
    else:
        df[col] = cast_numeric(df[col], empty_to_zero=False)

# Проверяем, как изменился датасет после всех преобразований
print("После обработки")
print(df.info(memory_usage="deep"))
print(df.head(10))

print("Сохраняю файл...")
out_path = Path(r"C:\Users\stnva\proj1\dataset_clean.parquet")

out_path.parent.mkdir(parents=True, exist_ok=True)

df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)

print(f"Файл сохранён: {out_path}")
