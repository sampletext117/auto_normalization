#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ТЕСТ ПАМЯТИ С ВЫСОКОЙ ИЗБЫТОЧНОСТЬЮ

Этот модуль предоставляет инструменты для тестирования эффективности нормализации
базы данных с использованием данных с высокой избыточностью.

ОСНОВНЫЕ ВОЗМОЖНОСТИ:
1. create_highly_redundant_data() - создает данные с контролируемой высокой избыточностью
2. run_memory_test() - выполняет полный цикл тестирования памяти с нормализацией
3. plot_memory_usage() - строит графики для черно-белой печати
4. main() - интерактивный интерфейс для запуска тестов

ОСОБЕННОСТИ ГЕНЕРАЦИИ ДАННЫХ:
- Автоматическое определение типа поля по имени атрибута
- Предопределенные наборы значений для максимальной избыточности
- Поддержка различных предметных областей (сотрудники, студенты, заказы и т.д.)
- Обеспечение уникальности первичных ключей
- Вычисление коэффициента избыточности

ПРИМЕР ИСПОЛЬЗОВАНИЯ:
    python memory_test.py
    
    # Или программно:
    from memory_test import run_memory_test
    results = run_memory_test(my_relation, 10000)
    plot_memory_usage(results)

КОЭФФИЦИЕНТ ИЗБЫТОЧНОСТИ:
- < 10x: Низкая избыточность
- 10-50x: Умеренная избыточность  
- 50-200x: Высокая избыточность
- > 200x: Очень высокая избыточность (идеально для демонстрации)

АВТОР: Система автоматической нормализации БД
ДАТА: 2024
"""

import psycopg2
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import numpy as np
import random

from models import Relation, NormalForm, Attribute, FunctionalDependency
from data_test import connect, drop_table_if_exists, create_table, insert_random_data, create_and_populate_normalized, \
    count_rows, sql_type_for
from decomposition import Decomposer
from analyzer import NormalFormAnalyzer


def get_table_size_info(conn, table_name: str) -> Dict[str, int]:
    """
    Получить информацию о размере таблицы и её индексов в PostgreSQL.
    """
    with conn.cursor() as cur:
        query = f"""
            SELECT 
                pg_table_size('{table_name}') as table_size,
                pg_indexes_size('{table_name}') as indexes_size,
                pg_total_relation_size('{table_name}') as total_size,
                (SELECT COUNT(*) FROM {table_name}) as row_count
        """
        cur.execute(query)
        result = cur.fetchone()
        if result:
            table_size, indexes_size, total_size, row_count = result
            return {
                'table_size': table_size or 0,
                'indexes_size': indexes_size or 0,
                'total_size': total_size or 0,
                'row_count': row_count or 0
            }
    return {'table_size': 0, 'indexes_size': 0, 'total_size': 0, 'row_count': 0}


def create_realistic_indexes(conn, relation: Relation):
    """
    Создает реалистичные индексы для отношения:
    1. Пытается добавить PRIMARY KEY для первого кандидатного ключа.
    2. Создает обычные индексы для полей, которые вероятно являются внешними ключами.
    """
    analyzer = NormalFormAnalyzer(relation)
    pk_created = False

    with conn.cursor() as cur:
        # 1. Попытка создать первичный ключ
        if analyzer.candidate_keys:
            pk_candidate = list(analyzer.candidate_keys)[0]
            pk_cols = [attr.name for attr in pk_candidate]

            # Попытка добавить PRIMARY KEY constraint
            try:
                cur.execute(f"ALTER TABLE {relation.name} ADD PRIMARY KEY ({', '.join(pk_cols)})")
                print(f"  [INDEX] Создан PRIMARY KEY для {relation.name} на ({', '.join(pk_cols)})")
                pk_created = True
            except psycopg2.Error as e:
                # Если данные не уникальны, PK создать не получится. Это нормально для теста.
                # Откатываем транзакцию, чтобы продолжить.
                conn.rollback()
                print(f"  [WARNING] Не удалось создать PK для {relation.name}: {e}")
                # Создаем обычный уникальный индекс, если возможно, или просто индекс
                try:
                    cur.execute(f"CREATE UNIQUE INDEX ON {relation.name} ({', '.join(pk_cols)})")
                    print(f"  [INDEX] Создан UNIQUE INDEX для {relation.name} на ({', '.join(pk_cols)})")
                except psycopg2.Error:
                    conn.rollback()
                    cur.execute(f"CREATE INDEX ON {relation.name} ({', '.join(pk_cols)})")
                    print(f"  [INDEX] Создан обычный INDEX для {relation.name} на ({', '.join(pk_cols)})")

        # 2. Создание индексов для потенциальных внешних ключей
        # Простая эвристика: индексируем целочисленные поля, не входящие в PK
        pk_col_names = {attr.name for attr in (pk_candidate if pk_created else [])}

        for attr in relation.attributes:
            if attr.name not in pk_col_names and "int" in attr.data_type.lower():
                try:
                    cur.execute(f"CREATE INDEX ON {relation.name} ({attr.name})")
                    print(f"  [INDEX] Создан индекс для потенциального FK: {relation.name}({attr.name})")
                except psycopg2.Error as e:
                    conn.rollback()
                    print(f"  [WARNING] Не удалось создать индекс для {attr.name}: {e}")

    conn.commit()


def create_highly_redundant_data(conn, rel: Relation, num_rows: int) -> float:
    """
    Создает данные с высокой избыточностью для любого отношения.
    Возвращает коэффициент избыточности.
    """
    print(f"[INFO] Создание {num_rows} строк с высокой избыточностью...")
    print(f"[CREATE_DATA] Отношение: {rel.name}, атрибуты: {[attr.name for attr in rel.attributes]}")
    
    # Создаем таблицу
    print(f"[CREATE_DATA] Удаление существующей таблицы {rel.name}...")
    drop_table_if_exists(conn, rel.name)
    columns_def = []
    for attr in rel.attributes:
        col_def = f"{attr.name} {sql_type_for(attr)}"
        if attr.is_primary_key:
            col_def += " NOT NULL"
        columns_def.append(col_def)

    # Добавляем первичный ключ
    pk_attrs = [attr.name for attr in rel.attributes if attr.is_primary_key]
    pk_clause = ""
    if pk_attrs:
        pk_list = ", ".join(pk_attrs)
        pk_clause = f", PRIMARY KEY ({pk_list})"

    ddl = f"CREATE TABLE {rel.name} (\n    " + ",\n    ".join(columns_def) + pk_clause + "\n);"
    
    print(f"[CREATE_DATA] Создание таблицы {rel.name}...")
    print(f"[CREATE_DATA] DDL: {ddl}")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print(f"[CREATE_DATA] Таблица {rel.name} создана")
    
    # Предопределенные наборы значений для высокой избыточности
    departments = ["ИТ", "Финансы", "HR", "Маркетинг", "Продажи"]
    projects = ["Проект_А", "Проект_Б", "Проект_В", "Проект_Г", "Проект_Д"]
    managers = ["Петров И.И.", "Иванов А.А.", "Сидорова М.М.", "Козлов К.К.", "Новиков Н.Н."]
    employee_names = [
        "Александров А.А.", "Борисов Б.Б.", "Васильев В.В.", "Григорьев Г.Г.",
        "Дмитриев Д.Д.", "Егоров Е.Е.", "Жуков Ж.Ж.", "Зайцев З.З."
    ]
    student_names = [
        "Алексеев А.", "Борисов Б.", "Васильев В.", "Григорьев Г.",
        "Дмитриев Д.", "Егоров Е.", "Жуков Ж.", "Зайцев З."
    ]
    courses = ["Математика", "Физика", "Химия", "Биология", "История", "Литература"]
    teachers = ["Профессор Знаев", "Доцент Умнов", "Ассистент Мудров", "Лектор Ученый"]
    clients = ["ООО Рога", "ЗАО Копыта", "ИП Хвостов", "ООО Перья", "ЗАО Крылья"]
    products = ["Товар_А", "Товар_Б", "Товар_В", "Товар_Г", "Товар_Д"]
    categories = ["Электроника", "Одежда", "Книги", "Спорт", "Дом"]
    cities = ["Москва", "СПб", "Казань", "Екатеринбург", "Новосибирск"]
    groups = ["ИТ-101", "ИТ-102", "ИТ-201", "ИТ-202", "ИТ-301"]
    
    # Стандартные значения для числовых полей
    budgets = [50000.00, 100000.00, 150000.00, 200000.00, 250000.00]
    prices = [99.99, 199.99, 299.99, 499.99, 999.99]
    grades = [2, 3, 4, 5]
    quantities = [1, 2, 3, 5, 10]
    
    # Функция для генерации значения с высокой избыточностью
    def generate_redundant_value(attr: Attribute, row_id: int):
        dt = attr.data_type.upper()
        attr_lower = attr.name.lower()
        
        # Для первичных ключей обеспечиваем уникальность
        if attr.is_primary_key:
            if dt == "INTEGER":
                return row_id
            elif dt.startswith("VARCHAR"):
                return f"key_{row_id}"
            else:
                return row_id
        
        # Для неключевых полей создаем избыточность на основе имени атрибута
        if dt == "INTEGER":
            if any(word in attr_lower for word in ["отдел", "департамент"]):
                return random.randint(1, len(departments))
            elif any(word in attr_lower for word in ["проект", "курс"]):
                return random.randint(1, len(projects if "проект" in attr_lower else courses))
            elif any(word in attr_lower for word in ["клиент", "заказчик"]):
                return random.randint(1, len(clients))
            elif any(word in attr_lower for word in ["товар", "продукт"]):
                return random.randint(1, len(products))
            elif any(word in attr_lower for word in ["оценка", "балл"]):
                return random.choice(grades)
            elif any(word in attr_lower for word in ["количество", "кол"]):
                return random.choice(quantities)
            else:
                return random.randint(1, 8)
                
        elif dt.startswith("VARCHAR"):
            if any(word in attr_lower for word in ["отдел", "департамент"]):
                return random.choice(departments)
            elif "проект" in attr_lower and any(word in attr_lower for word in ["название", "наименование"]):
                return random.choice(projects)
            elif "курс" in attr_lower and any(word in attr_lower for word in ["название", "наименование"]):
                return random.choice(courses)
            elif any(word in attr_lower for word in ["имя", "фио"]) and any(word in attr_lower for word in ["сотрудник", "работник"]):
                return random.choice(employee_names)
            elif any(word in attr_lower for word in ["имя", "фио"]) and any(word in attr_lower for word in ["студент", "учащийся"]):
                return random.choice(student_names)
            elif any(word in attr_lower for word in ["имя", "название"]) and any(word in attr_lower for word in ["клиент", "заказчик"]):
                return random.choice(clients)
            elif any(word in attr_lower for word in ["начальник", "руководитель", "менеджер", "куратор"]):
                return random.choice(managers)
            elif any(word in attr_lower for word in ["преподаватель", "учитель", "лектор"]):
                return random.choice(teachers)
            elif any(word in attr_lower for word in ["товар", "продукт"]) and any(word in attr_lower for word in ["название", "наименование"]):
                return random.choice(products)
            elif any(word in attr_lower for word in ["категория", "тип"]):
                return random.choice(categories)
            elif any(word in attr_lower for word in ["город", "населенный"]):
                return random.choice(cities)
            elif any(word in attr_lower for word in ["группа", "класс"]):
                return random.choice(groups)
            else:
                # Общий случай - ограниченный набор значений
                return f"Значение_{random.randint(1, 6)}"
                
        elif dt == "DECIMAL":
            if any(word in attr_lower for word in ["бюджет", "стоимость"]):
                return random.choice(budgets)
            elif any(word in attr_lower for word in ["цена", "стоимость"]):
                return random.choice(prices)
            else:
                return random.choice([100.50, 250.00, 500.75, 1000.00, 1500.25])
                
        elif dt == "DATE":
            import datetime
            base_dates = [
                datetime.date(2023, 1, 15),
                datetime.date(2023, 3, 20),
                datetime.date(2023, 6, 10),
                datetime.date(2023, 9, 5),
                datetime.date(2023, 12, 1)
            ]
            return random.choice(base_dates)
            
        elif dt == "BOOLEAN":
            return random.choice([True, False])
        
        # По умолчанию
        return f"Общее_значение_{random.randint(1, 5)}"
    
    # Генерируем данные с обеспечением уникальности первичных ключей
    cols = [attr.name for attr in rel.attributes]
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO {rel.name} ({col_list}) VALUES ({placeholders});"
    
    pk_attrs = [attr for attr in rel.attributes if attr.is_primary_key]
    used_pk_values = set()
    
    print(f"[CREATE_DATA] Начало вставки данных...")
    with conn.cursor() as cur:
        inserted_rows = 0
        attempts = 0
        max_attempts = num_rows * 3  # Ограничиваем количество попыток
        
        while inserted_rows < num_rows and attempts < max_attempts:
            attempts += 1
            
            # Генерируем значения для всех атрибутов
            values = []
            for attr in rel.attributes:
                value = generate_redundant_value(attr, inserted_rows + 1)
                values.append(value)
            
            # Проверяем уникальность первичного ключа
            if pk_attrs:
                pk_indices = [rel.attributes.index(pk_attr) for pk_attr in pk_attrs]
                pk_tuple = tuple(values[i] for i in pk_indices)
                
                if pk_tuple in used_pk_values:
                    continue  # Пропускаем дубликат
                    
                used_pk_values.add(pk_tuple)
            
            cur.execute(insert_sql, values)
            inserted_rows += 1
            
            # Логируем прогресс
            if inserted_rows % 1000 == 0 or inserted_rows == num_rows:
                print(f"[CREATE_DATA] Вставлено {inserted_rows}/{num_rows} строк...")
    
    conn.commit()
    print(f"[INFO] Создано {inserted_rows} строк")
    print(f"[CREATE_DATA] Всего попыток: {attempts}, успешных вставок: {inserted_rows}")
    
    # Вычисляем статистику избыточности
    total_redundancy = 0
    redundancy_count = 0
    
    with conn.cursor() as cur:
        non_pk_attrs = [attr for attr in rel.attributes if not attr.is_primary_key]
        
        for attr in non_pk_attrs:
            cur.execute(f'SELECT COUNT(DISTINCT "{attr.name}") FROM {rel.name}')
            unique_count = cur.fetchone()[0]
            if unique_count > 0:
                field_redundancy = inserted_rows / unique_count
                total_redundancy += field_redundancy
                redundancy_count += 1
            print(f"  • Уникальных значений в {attr.name}: {unique_count}")
    
    # Средний коэффициент избыточности
    if redundancy_count > 0:
        redundancy_coeff = total_redundancy / redundancy_count
    else:
        redundancy_coeff = 1.0
    
    print(f"  • Средний коэффициент избыточности: {redundancy_coeff:.1f}x")
    return redundancy_coeff


def run_memory_test(
        orig_rel: Relation,
        num_rows: int = 10000
) -> Dict[str, Dict[str, any]]:
    """
    Тестирование использования памяти с высокой избыточностью.
    """
    print(f"[MEMORY_TEST] Начало функции run_memory_test с {num_rows} строками")
    
    try:
        conn = connect()
        print(f"[MEMORY_TEST] Подключение к БД успешно")
    except Exception as e:
        print(f"[MEMORY_TEST] ОШИБКА подключения к БД: {e}")
        raise
    
    results: Dict[str, Dict[str, any]] = {}

    try:
        print(f"\n{'=' * 50}")
        print(f"[INFO] Начало теста памяти с {num_rows} строками (высокая избыточность)...")
        print(f"{'=' * 50}")

        # 1. ИСХОДНОЕ ОТНОШЕНИЕ С ВЫСОКОЙ ИЗБЫТОЧНОСТЬЮ
        print("\n--- Уровень: Original ---")
        print(f"[MEMORY_TEST] Создание данных с высокой избыточностью...")
        
        redundancy_coeff = create_highly_redundant_data(conn, orig_rel, num_rows)
        print(f"[MEMORY_TEST] Данные созданы, коэффициент избыточности: {redundancy_coeff:.1f}x")
        
        print(f"[MEMORY_TEST] Создание индексов...")
        create_realistic_indexes(conn, orig_rel)  # Создаем индексы и PK
        print(f"[MEMORY_TEST] Индексы созданы")

        print(f"[MEMORY_TEST] Получение информации о размере таблицы...")
        results["Original"] = get_table_size_info(conn, orig_rel.name)
        results["Original"]["table_count"] = 1
        results["Original"]["redundancy_coeff"] = redundancy_coeff
        print(f"  Размер: {results['Original']['total_size'] / 1024:.2f} KB | "
              f"Строк: {results['Original']['row_count']} | "
              f"Избыточность: {redundancy_coeff:.1f}x")

        # 2. ОПРЕДЕЛЕНИЕ НОРМАЛЬНОЙ ФОРМЫ
        print(f"[MEMORY_TEST] Анализ нормальной формы...")
        analyzer = NormalFormAnalyzer(orig_rel)
        current_nf, _ = analyzer.determine_normal_form()
        print(f"\n[INFO] Исходная нормальная форма: {current_nf.value}")

        # 3. ДЕКОМПОЗИЦИЯ И ЗАМЕРЫ
        nf_order = [
            (NormalForm.SECOND_NF, "2NF", Decomposer.decompose_to_2nf),
            (NormalForm.THIRD_NF, "3NF", Decomposer.decompose_to_3nf),
            (NormalForm.BCNF, "BCNF", Decomposer.decompose_to_bcnf),
            (NormalForm.FOURTH_NF, "4NF", Decomposer.decompose_to_4nf)
        ]

        for target_nf, level_name, decompose_func in nf_order:
            if target_nf.value <= current_nf.value:
                print(f"[MEMORY_TEST] Пропускаем {level_name} (уже в более высокой НФ)")
                continue

            print(f"\n--- Уровень: {level_name} ---")
            print(f"[MEMORY_TEST] Выполнение декомпозиции в {level_name}...")
            
            try:
                decomp_result = decompose_func(orig_rel)
                decomposed_rels = decomp_result.decomposed_relations
                print(f"[MEMORY_TEST] Декомпозиция в {level_name} завершена")
            except Exception as e:
                print(f"[MEMORY_TEST] ОШИБКА декомпозиции в {level_name}: {e}")
                continue
            
            print(f"  [DEBUG] Декомпозиция дала {len(decomposed_rels)} отношений:")
            for i, rel in enumerate(decomposed_rels):
                attrs_str = [attr.name for attr in rel.attributes]
                print(f"    {i+1}. {rel.name}: {attrs_str}")

            # Создаем и заполняем таблицы
            print(f"[MEMORY_TEST] Создание и заполнение нормализованных таблиц для {level_name}...")
            try:
                create_and_populate_normalized(conn, orig_rel, decomposed_rels)
                print(f"[MEMORY_TEST] Таблицы для {level_name} созданы и заполнены")
            except Exception as e:
                print(f"  [WARNING] Ошибка при создании нормализованных таблиц: {e}")
                print(f"[MEMORY_TEST] Пропускаем уровень {level_name} из-за ошибки")
                # Если не удалось создать нормализованные таблицы, пропускаем этот уровень
                continue

            level_total_size = 0
            level_table_size = 0
            level_indexes_size = 0
            level_row_count = 0

            # Создаем индексы для всех таблиц
            print(f"[MEMORY_TEST] Создание индексов для {len(decomposed_rels)} таблиц...")
            for i, rel in enumerate(decomposed_rels):
                print(f"[MEMORY_TEST] Создание индексов для таблицы {i+1}/{len(decomposed_rels)}: {rel.name}")
                create_realistic_indexes(conn, rel)
                size_info = get_table_size_info(conn, rel.name)
                print(f"[MEMORY_TEST] Размер таблицы {rel.name}: {size_info['total_size'] / 1024:.2f} KB, строк: {size_info['row_count']}")

                level_total_size += size_info['total_size']
                level_table_size += size_info['table_size']
                level_indexes_size += size_info['indexes_size']
                level_row_count += size_info['row_count']

            # Проверяем целостность данных через JOIN
            with conn.cursor() as cur:
                # Строим JOIN всех таблиц
                if len(decomposed_rels) > 1:
                    # Находим таблицу с наибольшим количеством атрибутов как основную
                    main_table = max(decomposed_rels, key=lambda r: len(r.attributes))
                    other_tables = [r for r in decomposed_rels if r != main_table]
                    
                    join_clause = main_table.name
                    main_attrs = set(attr.name for attr in main_table.attributes)
                    
                    for rel in other_tables:
                        rel_attrs = set(attr.name for attr in rel.attributes)
                        common_attr_names = main_attrs & rel_attrs
                        
                        if common_attr_names:
                            join_conditions = []
                            for attr_name in common_attr_names:
                                join_conditions.append(f"{main_table.name}.{attr_name} = {rel.name}.{attr_name}")
                            join_clause += f" JOIN {rel.name} ON {' AND '.join(join_conditions)}"
                        else:
                            # Если нет общих атрибутов, используем CROSS JOIN
                            join_clause += f" CROSS JOIN {rel.name}"
                            print(f"  [WARNING] Нет общих атрибутов между {main_table.name} и {rel.name}")

                    # Проверяем количество строк после JOIN
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {join_clause}")
                        join_count = cur.fetchone()[0]
                        print(f"  [INFO] Количество строк после JOIN: {join_count}")
                    except Exception as e:
                        print(f"  [WARNING] Ошибка при выполнении JOIN: {e}")
                        # Попробуем простой подсчет без JOIN
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {main_table.name}")
                            join_count = cur.fetchone()[0]
                            print(f"  [INFO] Используем количество строк основной таблицы: {join_count}")
                        except:
                            join_count = 0
                else:
                    join_count = level_row_count

            results[level_name] = {
                'total_size': level_total_size,
                'table_size': level_table_size,
                'indexes_size': level_indexes_size,
                'row_count': level_row_count,
                'table_count': len(decomposed_rels),
                'join_count': join_count
            }
            print(f"  Размер: {results[level_name]['total_size'] / 1024:.2f} KB | "
                  f"Строк: {results[level_name]['row_count']} | "
                  f"Таблиц: {results[level_name]['table_count']}")
            print(f"[MEMORY_TEST] Уровень {level_name} завершен успешно")

        print(f"\n[MEMORY_TEST] Тест памяти завершен. Обработано уровней: {len(results)}")

    except Exception as e:
        print(f"[ERROR] Ошибка в тесте памяти: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"[MEMORY_TEST] Закрытие соединения с БД")
        conn.close()

    print(f"[MEMORY_TEST] Возвращаем результаты: {list(results.keys())}")
    return results


def plot_memory_usage(results: Dict[str, Dict[str, any]]):
    """
    Построение графиков для черно-белой печати с штриховкой вместо цветов.
    """
    levels = [level for level in ["Original", "2NF", "3NF", "BCNF", "4NF"] if level in results]
    if not levels:
        print("[ОШИБКА] Нет данных для построения графиков.")
        return

    # Подготовка данных (в мегабайтах для наглядности)
    data_sizes = [results[level]["table_size"] / (1024 * 1024) for level in levels]
    index_sizes = [results[level]["indexes_size"] / (1024 * 1024) for level in levels]
    total_sizes = [results[level]["total_size"] / (1024 * 1024) for level in levels]
    row_counts = [results[level]["row_count"] for level in levels]
    table_counts = [results[level]["table_count"] for level in levels]

    # Определяем паттерны штриховки для черно-белой печати
    hatches = ['', '///', '\\\\\\', '|||', '---', '+++', 'xxx', '...']
    colors = ['white', 'lightgray', 'gray', 'darkgray', 'black']

    # Создание фигуры
    fig = plt.figure(figsize=(16, 12))
    fig.suptitle('АНАЛИЗ ЭФФЕКТИВНОСТИ НОРМАЛИЗАЦИИ БАЗЫ ДАННЫХ (ВЫСОКАЯ ИЗБЫТОЧНОСТЬ)', 
                 fontsize=18, fontweight='bold', y=0.98)

    # --- График 1: Составной график размеров ---
    ax1 = plt.subplot(2, 3, 1)
    x = np.arange(len(levels))
    width = 0.6

    # Используем разную штриховку для данных и индексов
    bar1 = ax1.bar(x, data_sizes, width, label='Данные', 
                   color='white', edgecolor='black', hatch='///')
    bar2 = ax1.bar(x, index_sizes, width, bottom=data_sizes, label='Индексы', 
                   color='lightgray', edgecolor='black', hatch='\\\\\\')

    ax1.set_ylabel('Размер (МБ)', fontsize=12)
    ax1.set_title('Размер данных и индексов', fontsize=14, pad=15)
    ax1.set_xticks(x)
    ax1.set_xticklabels(levels, rotation=45)
    ax1.legend()
    ax1.grid(True, axis='y', linestyle='--', alpha=0.6)

    # Добавляем значения на столбцы
    for i, total_size in enumerate(total_sizes):
        ax1.text(i, total_size + 0.01, f'{total_size:.2f}', ha='center', va='bottom', 
                fontsize=10, fontweight='bold')

    # --- График 2: Количество строк с трендом ---
    ax2 = plt.subplot(2, 3, 2)
    
    # Используем разную штриховку для каждого уровня
    bars = []
    for i, count in enumerate(row_counts):
        bar = ax2.bar(i, count, color='white', edgecolor='black', 
                     hatch=hatches[i % len(hatches)])
        bars.append(bar)
    
    # Добавляем линию тренда
    ax2.plot(range(len(levels)), row_counts, 'ko-', linewidth=2, markersize=6)
    
    ax2.set_ylabel('Количество строк', fontsize=12)
    ax2.set_title('Общее количество строк', fontsize=14, pad=15)
    ax2.set_xticks(range(len(levels)))
    ax2.set_xticklabels(levels, rotation=45)
    ax2.grid(True, axis='y', linestyle='--', alpha=0.6)

    # Добавляем процентное изменение
    original_rows = row_counts[0] if row_counts else 0
    for i, (bar, count) in enumerate(zip(bars, row_counts)):
        height = bar[0].get_height()
        if i == 0:
            label = f'{count:,}'
        else:
            change = ((count - original_rows) / original_rows * 100) if original_rows > 0 else 0
            label = f'{count:,}\n({change:+.1f}%)'
        ax2.text(i, height, label.replace(',', ' '),
                ha='center', va='bottom', fontsize=9, fontweight='bold')

    # --- График 3: Сложность схемы ---
    ax3 = plt.subplot(2, 3, 3)
    bars = []
    for i, count in enumerate(table_counts):
        bar = ax3.bar(i, count, color='white', edgecolor='black', 
                     hatch=hatches[i % len(hatches)])
        bars.append(bar)
        
    ax3.set_ylabel('Количество таблиц', fontsize=12)
    ax3.set_title('Сложность схемы', fontsize=14, pad=15)
    ax3.set_xticks(range(len(levels)))
    ax3.set_xticklabels(levels, rotation=45)
    ax3.grid(True, axis='y', linestyle='--', alpha=0.6)
    ax3.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    for i, bar in enumerate(bars):
        height = bar[0].get_height()
        ax3.text(i, height, f'{int(height)}',
                ha='center', va='bottom', fontsize=11, fontweight='bold')

    # --- График 4: Эффективность нормализации ---
    ax4 = plt.subplot(2, 3, 4)
    original_size = total_sizes[0] if total_sizes else 0
    efficiency = []
    efficiency_labels = []
    
    for i, (level, size) in enumerate(zip(levels, total_sizes)):
        if i == 0:
            eff = 0
        else:
            eff = ((original_size - size) / original_size * 100) if original_size > 0 else 0
        efficiency.append(eff)
        efficiency_labels.append(f'{eff:.1f}%')

    # Используем разную штриховку для положительных и отрицательных значений
    bars = []
    for i, eff in enumerate(efficiency):
        if eff >= 0:
            # Положительная эффективность - диагональная штриховка
            bar = ax4.bar(i, eff, color='white', edgecolor='black', hatch='///')
        else:
            # Отрицательная эффективность - обратная диагональная штриховка
            bar = ax4.bar(i, eff, color='lightgray', edgecolor='black', hatch='\\\\\\')
        bars.append(bar)
        
    ax4.set_ylabel('Экономия размера (%)', fontsize=12)
    ax4.set_title('Эффективность нормализации', fontsize=14, pad=15)
    ax4.set_xticks(range(len(levels)))
    ax4.set_xticklabels(levels, rotation=45)
    ax4.grid(True, axis='y', linestyle='--', alpha=0.6)
    ax4.axhline(y=0, color='black', linestyle='-', alpha=0.8)

    for i, (bar, label) in enumerate(zip(bars, efficiency_labels)):
        height = bar[0].get_height()
        ax4.text(i, height + (1 if height >= 0 else -3), 
                label, ha='center', va='bottom' if height >= 0 else 'top', 
                fontsize=10, fontweight='bold')

    # --- График 5: Сводная таблица ---
    ax5 = plt.subplot(2, 3, (5, 6))
    ax5.axis('tight')
    ax5.axis('off')

    table_data = []
    headers = ['Уровень', 'Размер\n(МБ)', 'Изменение\nразмера', 'Всего\nстрок', 
               'Изменение\nстрок', 'Таблиц', 'Эффективность']
    
    for i, level in enumerate(levels):
        size_mb = total_sizes[i]
        rows = row_counts[i]
        tables = table_counts[i]
        
        # Изменение размера
        if i == 0:
            size_change = "—"
            row_change = "—"
            efficiency_text = "Базовый"
        else:
            size_change_pct = ((size_mb - original_size) / original_size * 100) if original_size > 0 else 0
            size_change = f"{size_change_pct:+.1f}%"
            
            row_change_pct = ((rows - original_rows) / original_rows * 100) if original_rows > 0 else 0
            row_change = f"{row_change_pct:+.1f}%"
            
            if size_change_pct < -5:
                efficiency_text = "Отлично"
            elif size_change_pct < 0:
                efficiency_text = "Хорошо"
            elif size_change_pct < 10:
                efficiency_text = "Средне"
            else:
                efficiency_text = "Плохо"

        row_str = f'{rows:,}'.replace(',', ' ')

        row = [
            level,
            f"{size_mb:.2f}",
            size_change,
            row_str,
            row_change,
            f"{tables}",
            efficiency_text
        ]
        table_data.append(row)

    table = ax5.table(cellText=table_data,
                      colLabels=headers,
                      cellLoc='center',
                      loc='center',
                      bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 2.0)
    
    # Стилизация таблицы для черно-белой печати
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('lightgray')
        table[(0, i)].set_text_props(weight='bold')
    
    for i in range(1, len(table_data) + 1):
        for j in range(len(headers)):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('white')
            else:
                table[(i, j)].set_facecolor('#F0F0F0')

    ax5.set_title('СВОДНАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ', pad=20, fontsize=16, fontweight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    # Добавляем общую статистику внизу
    original_size_mb = original_size
    final_size_mb = total_sizes[-1] if total_sizes else 0
    total_savings = ((original_size_mb - final_size_mb) / original_size_mb * 100) if original_size_mb > 0 else 0
    
    fig.text(0.5, 0.02, 
             f'ИТОГО: Экономия памяти {total_savings:.1f}% | '
             f'Исходный размер: {original_size_mb:.2f} МБ | '
             f'Финальный размер: {final_size_mb:.2f} МБ',
             ha='center', fontsize=12, fontweight='bold',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.8))
    
    plt.show()
    
    # Выводим текстовую сводку
    print(f"\nТЕКСТОВАЯ СВОДКА:")
    print(f"   • Исходный размер: {original_size_mb:.2f} МБ")
    print(f"   • Финальный размер: {final_size_mb:.2f} МБ")
    print(f"   • Экономия памяти: {total_savings:.1f}%")
    print(f"   • Количество таблиц: {table_counts[0]} → {table_counts[-1]}")
    print(f"   • Количество строк: {original_rows:,} → {row_counts[-1]:,}")
    
    # Показываем избыточность если доступна
    if "Original" in results and "redundancy_coeff" in results["Original"]:
        redundancy = results["Original"]["redundancy_coeff"]
        print(f"   • Коэффициент избыточности: {redundancy:.1f}x")
    
    if total_savings > 0:
        print(f"   [УСПЕХ] Нормализация ЭФФЕКТИВНА - экономия {total_savings:.1f}% памяти")
    else:
        print(f"   [ВНИМАНИЕ] Нормализация увеличила размер на {abs(total_savings):.1f}%")
        if "Original" in results and "redundancy_coeff" in results["Original"]:
            redundancy = results["Original"]["redundancy_coeff"]
            if redundancy < 10:
                print(f"       [СОВЕТ] Низкая избыточность ({redundancy:.1f}x) - увеличьте количество строк или уменьшите разнообразие значений")
            else:
                print(f"       [ПРОБЛЕМА] Высокая избыточность ({redundancy:.1f}x), но нормализация все равно увеличила размер")


def main():
    """
    Главная функция для запуска тестов памяти с высокой избыточностью.
    """
    print("=" * 60)
    print("ТЕСТ ПАМЯТИ С ВЫСОКОЙ ИЗБЫТОЧНОСТЬЮ")
    print("=" * 60)
    print("Этот тест создает данные с высокой избыточностью для")
    print("демонстрации эффективности нормализации базы данных.")
    print("=" * 60)
    
    # Пример отношения "Сотрудники-Проекты" с высокой избыточностью
    from models import Attribute
    
    employees_projects = Relation(
        name="СотрудникиПроекты",
        attributes=[
            Attribute("КодСотрудника", "INTEGER", is_primary_key=True),
            Attribute("ИмяСотрудника", "VARCHAR(100)"),
            Attribute("Отдел", "VARCHAR(50)"),
            Attribute("НачальникОтдела", "VARCHAR(100)"),
            Attribute("КодПроекта", "INTEGER"),
            Attribute("НазваниеПроекта", "VARCHAR(100)"),
            Attribute("Бюджет", "DECIMAL(10,2)")
        ]
    )
    
    # Добавляем функциональные зависимости для корректной нормализации
    employees_projects.functional_dependencies = [
        FunctionalDependency(
            determinant=frozenset([employees_projects.get_attribute_by_name("КодСотрудника")]),
            dependent=frozenset([
                employees_projects.get_attribute_by_name("ИмяСотрудника"),
                employees_projects.get_attribute_by_name("Отдел"),
                employees_projects.get_attribute_by_name("НачальникОтдела")
            ])
        ),
        FunctionalDependency(
            determinant=frozenset([employees_projects.get_attribute_by_name("КодПроекта")]),
            dependent=frozenset([
                employees_projects.get_attribute_by_name("НазваниеПроекта"),
                employees_projects.get_attribute_by_name("Бюджет")
            ])
        ),
        FunctionalDependency(
            determinant=frozenset([employees_projects.get_attribute_by_name("Отдел")]),
            dependent=frozenset([employees_projects.get_attribute_by_name("НачальникОтдела")])
        )
    ]
    
    print(f"\nИспользуемое отношение: {employees_projects.name}")
    print(f"Атрибуты: {[attr.name for attr in employees_projects.attributes]}")
    print(f"Функциональные зависимости: {len(employees_projects.functional_dependencies)}")
    
    # Запрашиваем количество строк
    while True:
        try:
            num_rows = int(input(f"\nВведите количество строк для теста (рекомендуется 5000-20000): "))
            if num_rows > 0:
                break
            else:
                print("Количество строк должно быть положительным числом.")
        except ValueError:
            print("Пожалуйста, введите корректное число.")
    
    # Запускаем тест
    print(f"\nЗапуск теста с {num_rows} строками...")
    results = run_memory_test(employees_projects, num_rows)
    
    if results:
        print(f"\nПостроение графиков...")
        plot_memory_usage(results)
    else:
        print("[ОШИБКА] Не удалось получить результаты теста.")


if __name__ == "__main__":
    from models import FunctionalDependency
    main()