import requests
from tqdm import tqdm
import pandas as pd
import os

FACTS_API_URL = "https://catfact.ninja/fact"
OUTPUT_FILENAME = "data/cat_facts.csv"


def load_cat_facts_data(facts_api_url: str, num_of_items: int = 10) -> list[dict]:
    # Загружаем факты о котиках"

    items = []

    # Создаем директорию data, если её нет
    os.makedirs("data", exist_ok=True)

    print(f"Загружаем {num_of_items} фактов о котиках из Cat Facts API...")

    for item_number in tqdm(range(num_of_items)):
        try:
            response = requests.get(
                facts_api_url,
                timeout=15,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Cat Facts Reader v1.0",
                },
            )

            if response.status_code == 200:
                fact_data = response.json()

                fact_data["request_number"] = item_number + 1
                fact_data["api_source"] = "catfact.ninja"

                items.append(fact_data)

            else:
                print(
                    f"\nВнимание! Код факта #{item_number + 1} - {response.status_code}"
                )

        except Exception as e:
            print(f"\nОшибка! Проблемы с загрузкой факта #{item_number + 1}: {e}")

    return items


def convert_to_df_and_save(
    data: list[dict],
    fname: str,
) -> pd.DataFrame | None:
    # Конвертируем данные в DataFrame и сохраним в CSV

    if data:
        df = pd.DataFrame(data)

        # Создаем директорию, если её нет
        os.makedirs(os.path.dirname(fname), exist_ok=True)

        # Сохраняем
        df.to_csv(fname, index=False, encoding="utf-8")
        print(f"Данные сохранены в файл: {fname}")
        return df

    print("Нет данных для сохранения")
    return None


def main():
    # Основная функция для загрузки и обработки фактов о котиках

    print("🐱 Хотите узнать больше о котиках? 🐱")
    print("Источник данных: https://catfact.ninja/")

    # Загружаем факты о котиках
    cat_facts = load_cat_facts_data(FACTS_API_URL, 20)

    if not cat_facts:
        print("Не удалось загрузить факты")
        return

    # Конвертируем в DataFrame и сохраняем
    result = convert_to_df_and_save(cat_facts, OUTPUT_FILENAME)

    if result is not None:
        print(f"\nУспешно загружено {len(result)} фактов о котиках!")

        print("\nИнформация о данных")
        print(result.info())

        print("Первые 5 фактов о котиках")
        if "fact" in result.columns:
            for i, fact in enumerate(result["fact"].head(5), 1):
                print(f"{i}. {fact}")

        print(f"\nСтатистика")
        print(f"Всего фактов: {len(result)}")
        print(f"Колонки: {list(result.columns)}")

    print("\nГотово! Все факты о котиках загружены и сохранены! 🐱")


if __name__ == "__main__":
    main()
