## data_test.py
# файл: data_test.py

import psycopg2
import random
import string
import datetime
from typing import List, Dict, Any
from models import Relation, Attribute

# ======================= Настройки подключения =======================
# Задайте ваши параметры подключения к PostgreSQL
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Iamtaskforce1',
    'host': 'localhost',
    'port': 5432
}


def connect():
    return psycopg2.connect(**DB_PARAMS)


def drop_table_if_exists(conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    conn.commit()


def sql_type_for(attr: Attribute) -> str:
    """
    Перевести тип Attribute.data_type в SQL-тип.
    Предполагается, что data_type — одна из: VARCHAR, INTEGER, DATE, DECIMAL, BOOLEAN.
    """
    dt = attr.data_type.upper()
    if dt.startswith("VARCHAR"):
        return "VARCHAR(255)"
    if dt == "INTEGER":
        return "INTEGER"
    if dt == "DECIMAL":
        return "DECIMAL"
    if dt == "DATE":
        return "DATE"
    if dt == "BOOLEAN":
        return "BOOLEAN"
    # По умолчанию — текст
    return "TEXT"


def create_table(conn, rel: Relation):
    """
    Создать таблицу rel.name со всеми её атрибутами.
    Если Attribute.is_primary_key=True, добавляется в состав PRIMARY KEY.
    """
    drop_table_if_exists(conn, rel.name)
    columns_def = []
    pk_attrs = [attr.name for attr in rel.attributes if attr.is_primary_key]
    for attr in rel.attributes:
        col_def = f"{attr.name} {sql_type_for(attr)}"
        # Если в исходном Relation задано is_primary_key, оставим это.
        # (в случае генерации данных ключевые поля будут уникальны вручную)
        if attr.is_primary_key:
            col_def += " NOT NULL"
        columns_def.append(col_def)

    pk_clause = ""
    if pk_attrs:
        pk_list = ", ".join(pk_attrs)
        pk_clause = f", PRIMARY KEY ({pk_list})"

    ddl = f"CREATE TABLE {rel.name} (\n    " + ",\n    ".join(columns_def) + pk_clause + "\n);"

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def rand_string(length: int = 8) -> str:
    """Сгенерировать случайную строку из букв + цифр длины length."""
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choices(alphabet, k=length))


def rand_date(start_year=2000, end_year=2020) -> datetime.date:
    """Сгенерировать случайную дату в диапазоне start_year–end_year."""
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    delta = end - start
    return start + datetime.timedelta(days=random.randint(0, delta.days))


def generate_value_for(attr: Attribute) -> Any:
    """
    Сгенерировать тестовое значение для атрибута с учетом избыточности.
    Создаем МНОГО повторяющихся значений для демонстрации эффекта нормализации.
    """
    dt = attr.data_type.upper()

    # --- Генерация для НЕКЛЮЧЕВЫХ полей (с ОЧЕНЬ ВЫСОКОЙ избыточностью) ---
    if not attr.is_primary_key:
        if dt == "INTEGER":
            # Очень ограниченный набор значений для создания избыточности
            if "отдел" in attr.name.lower() or "департамент" in attr.name.lower():
                return random.randint(1, 5)  # Всего 5 отделов
            elif "курс" in attr.name.lower() or "проект" in attr.name.lower():
                return random.randint(1, 8)  # 8 курсов/проектов
            elif "клиент" in attr.name.lower():
                return random.randint(1, 20)  # 20 клиентов
            elif "товар" in attr.name.lower():
                return random.randint(1, 15)  # 15 товаров
            elif "оценка" in attr.name.lower():
                return random.randint(2, 5)  # Оценки 2-5
            elif "количество" in attr.name.lower():
                return random.randint(1, 10)  # Количество 1-10
            else:
                return random.randint(1, 8)  # Общий случай
                
        if dt.startswith("VARCHAR"):
            # Очень ограниченный набор названий для максимальной избыточности
            attr_lower = attr.name.lower()
            if "отдел" in attr_lower:
                departments = ["ИТ", "Финансы", "HR", "Маркетинг", "Продажи"]
                return random.choice(departments)
            elif "проект" in attr_lower and "название" in attr_lower:
                projects = ["Проект_Альфа", "Проект_Бета", "Проект_Гамма", "Проект_Дельта", "Проект_Омега"]
                return random.choice(projects)
            elif "курс" in attr_lower and "название" in attr_lower:
                courses = ["Математика", "Физика", "Химия", "Биология", "История", "Литература"]
                return random.choice(courses)
            elif "имя" in attr_lower and "сотрудник" in attr_lower:
                names = ["Иванов И.И.", "Петров П.П.", "Сидоров С.С.", "Козлов К.К.", "Новиков Н.Н."]
                return random.choice(names)
            elif "имя" in attr_lower and "студент" in attr_lower:
                names = ["Алексеев А.", "Борисов Б.", "Васильев В.", "Григорьев Г.", "Дмитриев Д."]
                return random.choice(names)
            elif "имя" in attr_lower and "клиент" in attr_lower:
                names = ["ООО Рога", "ЗАО Копыта", "ИП Хвостов", "ООО Перья", "ЗАО Крылья"]
                return random.choice(names)
            elif "начальник" in attr_lower or "куратор" in attr_lower:
                managers = ["Петров П.П.", "Иванов И.И.", "Сидоров С.С.", "Козлов К.К.", "Новиков Н.Н."]
                return random.choice(managers)
            elif "преподаватель" in attr_lower:
                teachers = ["Профессор Знаев", "Доцент Умнов", "Ассистент Мудров", "Лектор Ученый"]
                return random.choice(teachers)
            elif "товар" in attr_lower and "название" in attr_lower:
                products = ["Товар_А", "Товар_Б", "Товар_В", "Товар_Г", "Товар_Д"]
                return random.choice(products)
            elif "категория" in attr_lower:
                categories = ["Электроника", "Одежда", "Книги", "Спорт", "Дом"]
                return random.choice(categories)
            elif "город" in attr_lower:
                cities = ["Москва", "СПб", "Казань", "Екатеринбург", "Новосибирск"]
                return random.choice(cities)
            elif "группа" in attr_lower:
                groups = ["ИТ-101", "ИТ-102", "ИТ-201", "ИТ-202", "ИТ-301"]
                return random.choice(groups)
            else:
                # Общий случай - очень ограниченный набор
                values = [f"Значение_{i}" for i in range(1, 6)]
                return random.choice(values)
                
        if dt == "DECIMAL":
            # Стандартные значения для создания избыточности
            if "бюджет" in attr.name.lower():
                budgets = [50000.00, 100000.00, 150000.00, 200000.00, 250000.00]
                return random.choice(budgets)
            elif "цена" in attr.name.lower():
                prices = [99.99, 199.99, 299.99, 499.99, 999.99]
                return random.choice(prices)
            else:
                standard_values = [100.50, 250.00, 500.75, 1000.00, 1500.25]
                return random.choice(standard_values)
                
        if dt == "DATE":
            # Даты в очень узком диапазоне для создания дубликатов
            base_dates = [
                datetime.date(2023, 1, 15),
                datetime.date(2023, 3, 20),
                datetime.date(2023, 6, 10),
                datetime.date(2023, 9, 5),
                datetime.date(2023, 12, 1)
            ]
            return random.choice(base_dates)
            
        if dt == "BOOLEAN":
            return random.choice([True, False])
        
        # По умолчанию для неключевых полей - очень ограниченный набор
        return f"Общее_значение_{random.randint(1, 5)}"

    # --- Генерация для КЛЮЧЕВЫХ полей ---
    # Для ключевых полей нужна уникальность, но в разумных пределах
    if dt == "INTEGER":
        # Увеличиваем диапазон для составных ключей
        return random.randint(1, 500)
    if dt.startswith("VARCHAR"):
        return f"key_{random.randint(1, 500)}"
    if dt == "DECIMAL":
        return round(random.uniform(1, 500), 2)
    if dt == "DATE":
        return rand_date(start_year=2020, end_year=2024)
    if dt == "BOOLEAN":
        return random.choice([True, False])

    # По умолчанию для ключевых полей
    return f"key_{random.randint(1, 500)}"


def insert_random_data(conn, rel: Relation, num_rows: int):
    """
    Заполнить таблицу rel.name сгенерированными данными.
    Обеспечить уникальность значений для комбинации атрибутов, помеченных is_primary_key.
    """
    cols = [attr.name for attr in rel.attributes]
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO {rel.name} ({col_list}) VALUES ({placeholders});"

    # Определяем атрибуты первичного ключа
    pk_attrs = [attr for attr in rel.attributes if attr.is_primary_key]
    used_pk_values = set()  # будем хранить уже сгенерированные комбинации PK

    with conn.cursor() as cur:
        for _ in range(num_rows):
            while True:
                # Сгенерировать набор значений для всех атрибутов
                values = [generate_value_for(attr) for attr in rel.attributes]

                # Вытащить только те значения, что соответствуют PK-атрибутам
                if pk_attrs:
                    pk_indices = [rel.attributes.index(pk_attr) for pk_attr in pk_attrs]
                    pk_tuple = tuple(values[i] for i in pk_indices)
                else:
                    # Если первичных ключей нет, то просто считаем, что уникальность не проверяем
                    pk_tuple = None

                # Проверяем, уникальна ли эта комбинация PK
                if pk_tuple is None or pk_tuple not in used_pk_values:
                    if pk_tuple is not None:
                        used_pk_values.add(pk_tuple)
                    break
                # Иначе повторяем генерацию

            # Логируем сам SQL и сгенерированные values
            # print(f"[SQL-INSERT] {insert_sql} → {values}")
            cur.execute(insert_sql, values)

    conn.commit()


def create_and_populate_normalized(conn, orig_rel: Relation, normalized: List[Relation]):
    """
    Для каждого отношения из normalized:
    1. Создать таблицу
    2. Заполнить из исходной таблицы: вставить DISTINCT строки по проекции атрибутов
    3. Добавить индексы для производительности
    """
    # Сначала создаем все таблицы
    for rel in normalized:
        # Удалим любую старую таблицу с таким именем
        drop_table_if_exists(conn, rel.name)

        # Создаем таблицу с правильными типами данных
        columns_sql = []
        for attr in rel.attributes:
            col_def = f"{attr.name} {sql_type_for(attr)}"
            columns_sql.append(col_def)
        create_sql = f"CREATE TABLE {rel.name} (\n    " + ",\n    ".join(columns_sql) + "\n);"
        print(f"[SQL-CREATE-NORM] {create_sql}")
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

    # Теперь заполняем таблицы
    for rel in normalized:
        # Получаем список атрибутов для этой таблицы
        attrs = [attr.name for attr in rel.attributes]
        col_list = ", ".join(attrs)
        
        # Заполняем таблицу уникальными комбинациями
        with conn.cursor() as cur:
            # Проверяем, существуют ли все столбцы в исходной таблице
            # Пробуем найти таблицу сначала с оригинальным именем, потом с нижним регистром
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{orig_rel.name}'
                   OR table_name = '{orig_rel.name.lower()}'
            """)
            existing_columns = {row[0] for row in cur.fetchall()}  # Сохраняем оригинальный регистр
            
            # Фильтруем только существующие столбцы
            valid_attrs = []
            for attr in attrs:
                if attr in existing_columns:
                    valid_attrs.append(attr)
                else:
                    print(f"[WARNING] Столбец {attr} не найден в таблице {orig_rel.name}")
                    print(f"[DEBUG] Доступные столбцы: {list(existing_columns)}")
            
            if valid_attrs:
                # Используем кавычки для имен столбцов и таблиц для корректной работы с кириллицей
                valid_col_list = ", ".join([f'"{attr}"' for attr in valid_attrs])
                insert_sql = f"""
                    INSERT INTO {rel.name} ({valid_col_list})
                    SELECT DISTINCT {valid_col_list} FROM "{orig_rel.name}"
                """
                print(f"[SQL-PROJECT] {insert_sql.strip()}")
                cur.execute(insert_sql)
            else:
                print(f"[ERROR] Нет валидных столбцов для таблицы {rel.name}")
        
        conn.commit()
        
        # Отчёт о количестве строк
        cnt = count_rows(conn, rel.name)
        print(f"[INFO] В таблице {rel.name} после проекции {cnt} строк")
        
        # Добавляем индексы для производительности (но не первичные ключи)
        with conn.cursor() as cur:
            for attr in rel.attributes:
                if attr.name in valid_attrs:
                    try:
                        index_name = f"idx_{rel.name}_{attr.name}"
                        cur.execute(f'CREATE INDEX {index_name} ON {rel.name} ("{attr.name}")')
                        print(f"[INFO] Создан индекс {index_name}")
                    except Exception as e:
                        # Индекс уже существует или другая ошибка - не критично
                        pass
        conn.commit()


def count_rows(conn, table_name: str) -> int:
    """Вернуть число строк в таблице table_name."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0]


def test_decomposition(orig_rel: Relation, normalized: List[Relation], num_rows: int = 1000):
    """
    1. Создать исходную таблицу orig_rel и заполнить её random-данными (num_rows строк).
    2. Создать нормализованные таблицы и заполнить их SELECT DISTINCT из original.
    3. Выполнить JOIN всех normalized таблиц, сравнить количество строк JOIN-результата с исходной.
    """
    conn = connect()
    try:
        # --- Шаг 1: создать и заполнить исходную таблицу ---
        create_table(conn, orig_rel)
        insert_random_data(conn, orig_rel, num_rows)
        orig_count = count_rows(conn, orig_rel.name)
        print(f"[INFO] Вставлено {orig_count} строк в таблицу {orig_rel.name}")

        # --- Шаг 2: создать и заполнить нормализованные таблицы ---
        create_and_populate_normalized(conn, orig_rel, normalized)
        for rel in normalized:
            cnt = count_rows(conn, rel.name)
            print(f"[INFO] В таблице {rel.name} после проекции {cnt} строк")

        # --- Шаг 3: выполнить JOIN всех normalized таблиц ---
        if not normalized or len(normalized) < 1:
            print("[WARNING] Нет нормализованных таблиц для проверки.")
            return

        # Улучшенная логика построения JOIN'ов
        # Начинаем с первой таблицы
        first_rel = normalized[0]
        join_clause = first_rel.name
        # Собираем все атрибуты, доступные в текущем JOIN
        available_attrs = first_rel.get_all_attributes_set()
        # Таблицы, которые уже в JOIN
        joined_relations = {first_rel.name: first_rel}

        for rel_to_join in normalized[1:]:
            rel_attrs = rel_to_join.get_all_attributes_set()
            # Ищем общие атрибуты между новой таблицей и ВСЕМИ уже доступными
            common_attrs = available_attrs.intersection(rel_attrs)

            if not common_attrs:
                # Этого не должно происходить при корректной декомпозиции,
                # но на всякий случай оставляем CROSS JOIN
                join_clause += f" CROSS JOIN {rel_to_join.name}"
                print(f"[WARNING] Не найдены общие столбцы для {rel_to_join.name}. Используется CROSS JOIN.")
            else:
                # Ищем, с какой из уже добавленных таблиц можно соединиться
                target_table_name = None
                for attr in common_attrs:
                    for name, rel in joined_relations.items():
                        if attr in rel.get_all_attributes_set():
                            target_table_name = name
                            break
                    if target_table_name:
                        break

                # Строим условие ON по всем общим атрибутам
                on_conditions = [
                    f"{rel_to_join.name}.{attr.name} = {target_table_name}.{attr.name}"
                    for attr in common_attrs
                ]
                join_clause += f" JOIN {rel_to_join.name} ON {' AND '.join(on_conditions)}"

            # Обновляем информацию о доступных атрибутах и таблицах
            available_attrs.update(rel_attrs)
            joined_relations[rel_to_join.name] = rel_to_join

        join_sql = f"SELECT COUNT(*) FROM {join_clause};"
        print(f"[SQL-JOIN] {join_sql}")
        with conn.cursor() as cur:
            cur.execute(join_sql)
            joined_count = cur.fetchone()[0]

        print(f"[INFO] После JOIN всех нормализованных таблиц получено {joined_count} строк")
        if joined_count == orig_count:
            print("[SUCCESS] Декомпозиция без потерь: ROW_COUNT совпадают")
        else:
            print("[ERROR] ROW_COUNT не совпадают! Исходно:", orig_count, "JOIN:", joined_count)

    finally:
        conn.close()


# ======================= Пример использования =======================

if __name__ == "__main__":
    # Пример: исходное отношение R(a, b, c, d), где a→b, а c→d.
    # Тогда 3NF-декомпозиция даст таблицы R1(a, b) и R2(c, d), и, возможно, таблицу с ключом,
    # но для простоты возьмем две.

    from models import FunctionalDependency

    # Описываем исходное отношение
    attrs_orig = [
        Attribute(name='a', data_type='INTEGER', is_primary_key=True),
        Attribute(name='b', data_type='VARCHAR', is_primary_key=False),
        Attribute(name='c', data_type='INTEGER', is_primary_key=True),
        Attribute(name='d', data_type='VARCHAR', is_primary_key=False),
    ]
    fds_orig = [
        FunctionalDependency(determinant={attrs_orig[0]}, dependent={attrs_orig[1]}),  # a → b
        FunctionalDependency(determinant={attrs_orig[2]}, dependent={attrs_orig[3]}),  # c → d
    ]
    orig_relation = Relation(name='R_original', attributes=attrs_orig, functional_dependencies=fds_orig)

    # Предположим, после нормализации получили два отношения:
    attrs_r1 = [
        Attribute(name='a', data_type='INTEGER', is_primary_key=True),
        Attribute(name='b', data_type='VARCHAR', is_primary_key=False),
    ]
    r1 = Relation(name='R1', attributes=attrs_r1, functional_dependencies=[fds_orig[0]])

    attrs_r2 = [
        Attribute(name='c', data_type='INTEGER', is_primary_key=True),
        Attribute(name='d', data_type='VARCHAR', is_primary_key=False),
    ]
    r2 = Relation(name='R2', attributes=attrs_r2, functional_dependencies=[fds_orig[1]])

    normalized_relations = [r1, r2]

    # Запускаем тест (1000 строк)
    test_decomposition(orig_relation, normalized_relations, num_rows=1000)