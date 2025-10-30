from __future__ import annotations

from pathlib import Path
from typing import Optional, Union, List, Tuple
import re

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
DEFAULT_INPUT = DATA_DIR / "data.csv"
DEFAULT_OUTPUT_PARQUET = DATA_DIR / "ironalloys_cleaned.parquet"


# Ищем ячейки, где помимо числа есть еще единицы измерения
def make_regex(units: List[str]) -> re.Pattern:
    joined = "|".join(map(re.escape, units))
    return re.compile(rf"([0-9]*\.?[0-9]+)\s*({joined})", re.I)


_density_re = make_regex(["g/cm3"])
_just_number_re = re.compile(r"^[\s]*([0-9]*\.?[0-9]+)[\s]*$")
_energy_suffix_re = make_regex(["MJ/m3", "kJ/m3"])

_bad_chars = re.compile(r"[^A-Za-z0-9_]+")


def parse_density(cell: Union[str, float, int, None]) -> Union[float, pd.NA]:
    # Возвращает плотность без единиц измерения
    if pd.isna(cell):
        return pd.NA

    s = str(cell)
    m = _density_re.search(s)
    if m:
        return float(m.group(1))

    num = pd.to_numeric(s, errors="coerce")
    return float(num) if not pd.isna(num) else pd.NA


def parse_energy(cell: Union[str, float, int, None]) -> Union[float, pd.NA]:
    # Возвращает энергию без единиц измерения
    if pd.isna(cell):
        return pd.NA

    s = str(cell)
    num_only = _just_number_re.match(s)
    if num_only:
        return float(num_only.group(1))

    with_unit = _energy_suffix_re.search(s)
    if with_unit:
        return float(with_unit.group(1))

    return pd.NA


def cast_numeric(s: pd.Series, empty_to_zero: bool = False) -> pd.Series:
    # Приводим типы
    s = s.replace(r"^\s*$", np.nan, regex=True)
    s = pd.to_numeric(s, errors="coerce")

    if empty_to_zero:
        s = s.fillna(0)

    non_na = s.dropna()
    if len(non_na) == 0:
        return s.astype("Float32")

    if (non_na % 1 == 0).all():
        return s.astype("Int32")

    return s.astype("Float32")


def clean_string(text: Optional[str]) -> Optional[str]:
    # Удаляем специальные символы
    if text is None:
        return text
    t = str(text).replace(" ", "_")
    return _bad_chars.sub("", t)


def fix_units(df: pd.DataFrame) -> pd.DataFrame:
    # Меняем название колонок, добавляя единицы измерения
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

    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    # Для химических колонок пустые ячейки заполним "0" (нулевое содержание в сплаве), а в физических свойствах - NaN
    text_cols = ["iron_alloy_name"]

    first_comp, last_comp = "Iron (Fe)Fe min", "Lead (Pb)Pb max"
    if first_comp in df.columns and last_comp in df.columns:
        comp_cols = df.loc[:, first_comp:last_comp].columns.tolist()
    else:
        comp_cols = []

    for col in df.columns:
        if col in text_cols:
            continue

        if col in comp_cols:
            df[col] = cast_numeric(df[col], empty_to_zero=True)
        else:
            df[col] = cast_numeric(df[col], empty_to_zero=False)

    return df


def clean_columns_and_text(df: pd.DataFrame) -> pd.DataFrame:
    # Чистим имена колонок и текстовые столбцы
    df.columns = [clean_string(col) for col in df.columns]

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).map(clean_string)

    return df


def transform_dataframe(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    if verbose:
        print("До обработки:")
        print(df.info(memory_usage="deep"))
        print(df.head(10))

    if verbose:
        print("\nИсправляем единицы измерения...")
    df = fix_units(df)

    if verbose:
        keep_cols = [
            c
            for c in df.columns
            if c.endswith(", g/cm³") or c.endswith(", MJ/m³") or c.endswith(", kJ/m³")
        ]
        if keep_cols:
            print("\nИсправленные столбцы (первые строки):")
            print(df[keep_cols].head())
            print(df[keep_cols].info())

    if verbose:
        print("\nПриводим типы...")
    df = cast_types(df)

    if verbose:
        print("\nПосле обработки:")
        print(df.info(memory_usage="deep"))
        print(df.head(10))

    if verbose:
        print("\nУдаляем специальные символы...")
    df = clean_columns_and_text(df)

    if verbose:
        print("\nКолонки после очистки:")
        print(list(df.columns))
        first_col = df.columns[0]
        print(f"\nЗначения первого столбца «{first_col}»:")
        print(df[first_col].head(10).tolist())

    return df


def load_input(input_path: Optional[Path | str] = None) -> pd.DataFrame:
    path = DEFAULT_INPUT
    if not path.exists():
        raise FileNotFoundError(f"Входной файл не найден: {path}")

    df = pd.read_csv(path)
    return df


def _with_cleaned_suffix(path: Path, new_suffix: str) -> Path:
    return path.with_name(path.stem + "_cleaned").with_suffix(new_suffix)


def save_output(
    df: pd.DataFrame,
    output_path: Optional[Path | str],
    base_input_path: Optional[Path | str] = None,
) -> Path:
    out_path = DEFAULT_OUTPUT_PARQUET

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return out_path


def transform_file(
    input_path: Optional[Path | str] = None,
    output_path: Optional[Path | str] = None,
    verbose: bool = True,
) -> Tuple[pd.DataFrame, Path]:
    df_in = load_input(input_path)
    df_out = transform_dataframe(df_in, verbose=verbose)

    saved_path = save_output(
        df_out,
        output_path=output_path,
        base_input_path=DEFAULT_OUTPUT_PARQUET,
    )

    return df_out, saved_path
