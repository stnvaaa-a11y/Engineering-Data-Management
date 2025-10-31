# О проекте
Этот проект посвящен анализу, обработке и аналитике данных, содержащих информацию о составе и физических свойствах различных желехны сплавов.
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
### Датасет
Датасет посвящен физическим свойствам сплавов на основе железа и расположен по ссылке:
https://drive.google.com/drive/folders/1RMOLvTF27d-mAMkZQmKJ_ajW_TtetIsZ?usp=sharing
Источник данных:
https://www.kaggle.com/datasets/nikitamanaenkov/iron-alloys-dataset
### Структура проекта
Engineering-Data-Management/ ├── etl/ # ETL-пайплайн (extract → transform → load) │ ├── __init__.py │ ├── extract.py # Загрузка CSV (Google Drive FILE_ID) │ ├── transform.py # Очистка данных и приведение типов │ ├── load.py # Загрузка в PostgreSQL │   ├── docs/ # GitHub Pages │ └── interactive_chromium.html # Интерактивный график (Plotly) ├── notebooks/ # Jupyter-ноутбуки (исследования) │ └── EDA.ipynb # Первичный анализ данных ├── .gitignore ├── requirements.txt # Зависимости проекта (pip) ├── README.md 
