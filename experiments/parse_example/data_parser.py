import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm.auto import tqdm

CATEGORY_URL = "https://jusso.ru/product-category/shkolnye-ryukzaki/"
OUT_CSV = "jusso_backpacks.csv"


def get_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def collect_product_links(url: str) -> list[str]:
    # Возвращает список URL товаров
    soup = get_soup(url)
    return [
        a["href"]
        for a in soup.select("ul.products li.product a.woocommerce-LoopProduct-link")
    ]


def parse_product(url: str) -> dict:
    # Получим название товара, его цену и описание
    soup = get_soup(url)

    title_tag = soup.select_one("h1")
    title = title_tag.get_text(strip=True) if title_tag else "-"

    price_tag = soup.select_one("span.woocommerce-Price-amount")
    price = price_tag.get_text(" ", strip=True) if price_tag else "-"

    desc_tag = soup.select_one("#tab-description")
    description = desc_tag.get_text("\n", strip=True) if desc_tag else "-"

    return {"title": title, "description": description, "price": price, "url": url}


def main():
    links = collect_product_links(CATEGORY_URL)

    rows = [parse_product(link) for link in tqdm(links, desc="Ищем рюкзаки...")]

    # Фиксируем порядок колонок
    df = pd.DataFrame(rows, columns=["title", "description", "price", "url"])
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\nСохранено {len(df)} товаров в {OUT_CSV}")
    print("\nНайденные рюкзаки:")
    print(df)


if __name__ == "__main__":
    main()
