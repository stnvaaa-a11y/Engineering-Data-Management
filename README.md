# О проекте
Этот проект посвящен анализу, обработке и аналитике данных, содержащих информацию о составе и физических свойствах различных железных сплавов.
### Задачи проекта
1. Поиск и формирование исходного датасета
    - Загрузка исходного набора данных (CSV) с Google Drive по FILE_ID
3. Подготовка данных
    - приведение ячеек, содержащих единицы измерения к численным
    - приведение типов
    - удаление спецсимволов
    - сохранение в parquet
4. Исследование и визуализация данных (EDA)
    - Анализ полноты данных и основных корелляций
    - Использование статической (seaborn) и динамической (plotly express) визуализации
5. Загрузка результата в PostgreSQL
6. Сопутствующие исследовнаия
    - Создание простого парсера
    - Пример использования Api
### Датасет
Датасет посвящен физическим свойствам сплавов на основе железа и расположен по ссылке:<br> <a href="https://drive.google.com/drive/folders/1RMOLvTF27d-mAMkZQmKJ_ajW_TtetIsZ?usp=sharing">Doodle Drive</a> <br>Источник данных:<br> <a href="https://www.kaggle.com/datasets/nikitamanaenkov/iron-alloys-dataset">Kaggle</a>
### Структура проекта
  <pre>
Engineering-Data-Management/
├── etl/                                # ETL-пайплайн (extract → transform → load)
│   ├── __init__.py
│   ├── main.py                         # Объединяет все этапы (extract -> transform -> load)
│   ├── extract.py                      # Загрузка CSV (Google Drive FILE_ID)
│   ├── transform.py                    # Очистка данных, приведение типов, сохранение в parquet
│   └── load.py                         # Загрузка в PostgreSQL
│
├── docs/                               # GitHub Pages
│   └── interactive_chromium.html       # Интерактивный график (Plotly)
├── notebooks/                          # Jupyter-ноутбуки (исследования)
│   └── EDA.ipynb                       # Первичный анализ данных
├── .gitignore
├── requirements.txt                    # Зависимости проекта
├── README.md
└── experiments/                        # Сопутствующие исследования
    ├── api_example/                    # Пример использования API (рандомные факты о котиках)
    │   ├── api_reader.py               # Получение и сохранение данных из API
    │   └── README.md
    └── parse_example/                  # Парсинг данных с сайта продажи школьных рюкзаков
        ├── data_parser.py              # Парсинг и сохранение в CSV
        └── README.md
  </pre>
</details>

### Установка и использование
1. Клонируйте репозиторий:<br> ```git clone https://github.com/stnvaaa-a11y/Engineering-Data-Management.git```<br>
```cd Engineering-Data-Management```
2. Создайте и активируйте переменное окружение:<br> ```conda create -n myenv python=3.13 pip```<br>
```conda activate myenv```
3. Установите зависимости:<br>```pip install -r requirements.txt```
4. Настройка окружения (.env)<br>Создайте файл .env со следующими переменными:<br>DB_HOST=<br>DB_PORT=<br>DB_USER=<br>DB_PASSWORD=<br>DB_NAME=<br>DB_SCHEMA=<br>DB_TABLE=
5. Запуск ETL<br>```python -m etl.main```<br>После запуска будет запрошен Google Drive ID
