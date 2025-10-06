### Cat Facts API

API URL: https://catfact.ninja/fact
API, который предоставляет случайные интересные факты о котиках.

### О скрипте

api_reader.py - Python-скрипт для загрузки интересных фактов о кошках через Cat Facts API, автоматического создания Pandas DataFrame и сохранения фактов в CSV файл.

### Требования

- Python 3.7+
- pip (менеджер пакетов Python)

### Установка зависимостей

```bash
pip install requests tqdm pandas

### Использование скрипта

1. Скачайте скрипт api_reader.py
2. Запустите скрипт:
  python api_reader.py
3. Результат: Скрипт создаст папку data/ и сохранит файл cat_facts.csv с загруженными фактами о кошках.

Вы можете изменить количество загружаемых фактов, отредактировав строку в функции main():
cat_facts = load_cat_facts_data(FACTS_API_URL, 20)  # Измените 20 на нужное число

### Пример вывода
<img width="1603" height="754" alt="image" src="https://github.com/user-attachments/assets/ef89903b-cc65-4a2b-9f6f-9d39fc1fc584" />
