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
    """Сгенерировать тестовое значение для данного атрибута."""
    dt = attr.data_type.upper()
    if dt == "INTEGER":
        return random.randint(1, 10_000)
    if dt.startswith("VARCHAR"):
        return rand_string(10)
    if dt == "DECIMAL":
        # Генерируем Decimal как строку, чтобы psycopg2 преобразовал
        return round(random.uniform(0, 10_000), 2)
    if dt == "DATE":
        return rand_date()
    if dt == "BOOLEAN":
        return random.choice([True, False])
    # По умолчанию — текст
    return rand_string(10)


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
            print(f"[SQL-INSERT] {insert_sql} → {values}")
            cur.execute(insert_sql, values)

    conn.commit()




def create_and_populate_normalized(conn, orig_rel: Relation, normalized: List[Relation]):
    """
    Для каждого отношения из normalized:
    1. Создать таблицу (без первичных ключей!)
    2. Заполнить из исходной таблицы: вставить DISTINCT строки по проекции атрибутов
    """
    for rel in normalized:
        # Удалим любую старую таблицу с таким именем
        drop_table_if_exists(conn, rel.name)

        # Шаг 1: создать таблицу БЕЗ определения PRIMARY KEY и NOT NULL
        # Просто перечисляем имена столбцов с типами
        columns_sql = []
        for attr in rel.attributes:
            col_def = f"{attr.name} {sql_type_for(attr)}"
            columns_sql.append(col_def)
        create_sql = f"CREATE TABLE {rel.name} (\n    " + ",\n    ".join(columns_sql) + "\n);"
        print(f"[SQL-CREATE-NORM] {create_sql}")
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

        # Шаг 2: заполнить проекцией SELECT DISTINCT
        attrs = [attr.name for attr in rel.attributes]
        col_list = ", ".join(attrs)
        insert_sql = f"""
            INSERT INTO {rel.name} ({col_list})
            SELECT DISTINCT {col_list} FROM {orig_rel.name};
        """
        # Логируем SQL‐запрос проекции перед выполнением
        print(f"[SQL-PROJECT] {insert_sql.strip()}")
        with conn.cursor() as cur:
            cur.execute(insert_sql)
        conn.commit()

        # Отчёт о количестве строк
        cnt = count_rows(conn, rel.name)
        print(f"[INFO] В таблице {rel.name} после проекции {cnt} строк")



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
        if not normalized:
            print("[WARNING] Нет нормализованных таблиц для проверки.")
            return

        # Построить цепочку JOIN: начинаем с первой таблицы
        join_clause = f"{normalized[0].name}"
        used_aliases = {normalized[0].name: normalized[0]}
        for rel in normalized[1:]:
            # Найти общее пересечение атрибутов между rel и уже JOIN-нутыми
            common = set(rel.get_all_attributes_set())
            for prev_name, prev_rel in used_aliases.items():
                common = common.intersection(prev_rel.get_all_attributes_set())
            if not common:
                # Если нет общих атрибутов, просто кросс-join (не гарантирует восстановление данных)
                join_clause = f"{join_clause} CROSS JOIN {rel.name}"
            else:
                # Для простоты берем первое общее имя
                join_cols = [attr.name for attr in common]
                # Пишем ON на равенство для всех общих
                on_preds = " AND ".join([f"{rel.name}.{col} = {next(iter(used_aliases))}.{col}" for col in join_cols])
                join_clause = f"{join_clause} JOIN {rel.name} ON {on_preds}"
            used_aliases[rel.name] = rel

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
