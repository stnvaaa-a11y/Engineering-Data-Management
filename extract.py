from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
OUTPUT_FILENAME = "data.csv"


def build_gdrive_url(file_id: str) -> str:
    return f"https://drive.google.com/uc?id={file_id}"


def load_dataset(
    url: str,
    data_dir: Path | None = None,
    output_filename: str = OUTPUT_FILENAME,
):
    # Загружает csv по URL и сохраняет его в data/data.csv.
    if data_dir is None:
        data_dir = DATA_DIR

    df = pd.read_csv(url)

    # Создаём папку для данных
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / output_filename

    # Сохраняем результат
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Готово. Размерность: {df.shape}. Сохранено в: {out_path.resolve()}")

    return df, out_path
