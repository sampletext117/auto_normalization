# файл: performance_test.py
import numpy as np
import psycopg2
import time
from typing import List, Dict, Tuple, Optional
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from models import Relation, Attribute, NormalForm
from data_test import *
from decomposition import Decomposer
from analyzer import NormalFormAnalyzer


# Добавьте эти функции в performance_test.py

def generate_test_value_for_type(data_type: str) -> str:
    """
    Генерирует подходящее тестовое значение для SQL-запроса в зависимости от типа данных

    Args:
        data_type: тип данных атрибута (VARCHAR, INTEGER, DATE, DECIMAL, BOOLEAN)

    Returns:
        Строка с SQL-представлением значения
    """
    data_type_upper = data_type.upper()

    if data_type_upper.startswith('VARCHAR') or data_type_upper == 'TEXT':
        # Для строковых типов возвращаем строку в кавычках
        return "'test%'"
    elif data_type_upper == 'INTEGER':
        return "1"
    elif data_type_upper == 'DECIMAL' or data_type_upper == 'NUMERIC':
        return "100.50"
    elif data_type_upper == 'DATE':
        return "'2020-01-01'"
    elif data_type_upper == 'BOOLEAN':
        return "TRUE"
    else:
        # По умолчанию предполагаем строковый тип
        return "'test'"


def generate_range_condition(attr: Attribute) -> str:
    """
    Генерирует условие диапазона для атрибута в зависимости от его типа

    Args:
        attr: атрибут для которого генерируется условие

    Returns:
        SQL условие для WHERE клаузы
    """
    data_type_upper = attr.data_type.upper()

    if data_type_upper == 'INTEGER':
        return f"{attr.name} BETWEEN 1 AND 100"
    elif data_type_upper == 'DECIMAL' or data_type_upper == 'NUMERIC':
        return f"{attr.name} BETWEEN 0.0 AND 1000.0"
    elif data_type_upper == 'DATE':
        return f"{attr.name} BETWEEN '2020-01-01' AND '2020-12-31'"
    elif data_type_upper.startswith('VARCHAR'):
        # Для строк используем сравнение по алфавиту
        return f"{attr.name} BETWEEN 'A' AND 'M'"
    else:
        # Для других типов используем простое равенство
        return f"{attr.name} = {generate_test_value_for_type(attr.data_type)}"


def generate_filter_condition(attr: Attribute) -> str:
    """
    Генерирует условие фильтрации для атрибута

    Args:
        attr: атрибут для фильтрации

    Returns:
        SQL условие для WHERE клаузы
    """
    data_type_upper = attr.data_type.upper()

    if data_type_upper.startswith('VARCHAR') or data_type_upper == 'TEXT':
        # Для строк используем LIKE
        return f"{attr.name} LIKE 'test%'"
    elif data_type_upper == 'BOOLEAN':
        return f"{attr.name} = TRUE"
    else:
        # Для остальных типов используем точное сравнение
        test_value = generate_test_value_for_type(attr.data_type)
        return f"{attr.name} = {test_value}"


def _build_realistic_select_queries(orig_rel: Relation, normalized: List[Relation]) -> Dict[str, Dict[str, str]]:
    """
    Построить реалистичные запросы для тестирования производительности с учетом типов данных
    """
    queries = {}

    # Определяем атрибуты для запросов
    pk_attrs = [attr for attr in orig_rel.attributes if attr.is_primary_key]
    non_pk_attrs = [attr for attr in orig_rel.attributes if not attr.is_primary_key]

    # Выбираем первый PK и non-PK атрибут для тестов
    pk_attr = pk_attrs[0] if pk_attrs else None
    non_pk_attr = non_pk_attrs[0] if non_pk_attrs else None

    # Запросы для исходной таблицы
    queries["Original"] = {}

    # 1. Точечный поиск по первичному ключу
    if pk_attr:
        pk_value = generate_test_value_for_type(pk_attr.data_type)
        queries["Original"]["pk_point_lookup"] = (
            f"SELECT * FROM {orig_rel.name} WHERE {pk_attr.name} = {pk_value}"
        )

    # 2. Диапазонный поиск по первичному ключу
    if pk_attr:
        range_condition = generate_range_condition(pk_attr)
        queries["Original"]["pk_range_scan"] = (
            f"SELECT * FROM {orig_rel.name} WHERE {range_condition}"
        )

    # 3. Полное сканирование с фильтрацией по non-PK атрибуту
    if non_pk_attr:
        filter_condition = generate_filter_condition(non_pk_attr)
        queries["Original"]["non_pk_filter"] = (
            f"SELECT * FROM {orig_rel.name} WHERE {filter_condition}"
        )

    # 4. Агрегирующий запрос
    if pk_attr and non_pk_attr:
        # Для агрегации выбираем подходящий числовой атрибут если есть
        numeric_attr = next((attr for attr in non_pk_attrs
                             if attr.data_type.upper() in ['INTEGER', 'DECIMAL', 'NUMERIC']),
                            non_pk_attr)

        if numeric_attr.data_type.upper() in ['INTEGER', 'DECIMAL', 'NUMERIC']:
            # Если есть числовой атрибут, используем SUM
            queries["Original"]["aggregation"] = (
                f"SELECT {pk_attr.name}, SUM({numeric_attr.name}) as total "
                f"FROM {orig_rel.name} "
                f"GROUP BY {pk_attr.name}"
            )
        else:
            # Иначе используем COUNT
            queries["Original"]["aggregation"] = (
                f"SELECT {non_pk_attr.name}, COUNT(*) as cnt "
                f"FROM {orig_rel.name} "
                f"GROUP BY {non_pk_attr.name}"
            )

    # 5. Сортировка с лимитом (пагинация)
    if pk_attr:
        queries["Original"]["ordered_limit"] = (
            f"SELECT * FROM {orig_rel.name} "
            f"ORDER BY {pk_attr.name} "
            f"LIMIT 50 OFFSET 100"
        )

    # Для нормализованных таблиц
    if normalized:
        level = classify_normalization_level(normalized)
        queries[level] = {}

        # Строим оптимальный JOIN
        join_clause = build_optimal_join(normalized)

        # Определяем главную таблицу (содержащую первичный ключ)
        main_table = None
        for rel in normalized:
            if pk_attr and any(attr.name == pk_attr.name for attr in rel.attributes):
                main_table = rel
                break

        if not main_table:
            main_table = normalized[0]

        # 1. Точечный поиск с JOIN
        if pk_attr:
            pk_value = generate_test_value_for_type(pk_attr.data_type)
            queries[level]["pk_point_lookup"] = (
                f"SELECT * FROM {join_clause} "
                f"WHERE {main_table.name}.{pk_attr.name} = {pk_value}"
            )

        # 2. Диапазонный поиск с JOIN
        if pk_attr:
            range_condition = generate_range_condition(pk_attr)
            # Добавляем префикс таблицы к имени атрибута
            prefixed_range = range_condition.replace(pk_attr.name, f"{main_table.name}.{pk_attr.name}")
            queries[level]["pk_range_scan"] = (
                f"SELECT * FROM {join_clause} "
                f"WHERE {prefixed_range}"
            )

        # 3. Фильтрация по non-PK с JOIN
        if non_pk_attr:
            # Находим таблицу с non-PK атрибутом
            non_pk_table = None
            for rel in normalized:
                if any(attr.name == non_pk_attr.name for attr in rel.attributes):
                    non_pk_table = rel
                    break

            if non_pk_table:
                filter_condition = generate_filter_condition(non_pk_attr)
                # Добавляем префикс таблицы
                prefixed_filter = filter_condition.replace(non_pk_attr.name,
                                                           f"{non_pk_table.name}.{non_pk_attr.name}")
                queries[level]["non_pk_filter"] = (
                    f"SELECT * FROM {join_clause} "
                    f"WHERE {prefixed_filter}"
                )

        # 4. Агрегация с JOIN
        if pk_attr and non_pk_attr:
            # Ищем числовой атрибут для агрегации
            numeric_attr = None
            numeric_table = None

            for rel in normalized:
                for attr in rel.attributes:
                    if (not attr.is_primary_key and
                            attr.data_type.upper() in ['INTEGER', 'DECIMAL', 'NUMERIC']):
                        numeric_attr = attr
                        numeric_table = rel
                        break
                if numeric_attr:
                    break

            if numeric_attr and numeric_table:
                queries[level]["aggregation"] = (
                    f"SELECT {main_table.name}.{pk_attr.name}, "
                    f"SUM({numeric_table.name}.{numeric_attr.name}) as total "
                    f"FROM {join_clause} "
                    f"GROUP BY {main_table.name}.{pk_attr.name}"
                )
            elif non_pk_table:
                queries[level]["aggregation"] = (
                    f"SELECT {non_pk_table.name}.{non_pk_attr.name}, COUNT(*) as cnt "
                    f"FROM {join_clause} "
                    f"GROUP BY {non_pk_table.name}.{non_pk_attr.name}"
                )

        # 5. Сортировка с JOIN
        if pk_attr:
            queries[level]["ordered_limit"] = (
                f"SELECT * FROM {join_clause} "
                f"ORDER BY {main_table.name}.{pk_attr.name} "
                f"LIMIT 50 OFFSET 100"
            )

    return queries


def create_query_indexes(conn, relation: Relation):
    """
    Создать индексы, оптимизированные для тестовых запросов с учетом типов данных
    """
    with conn.cursor() as cur:
        # Индексы для различных типов атрибутов
        for attr in relation.attributes:
            if attr.is_primary_key:
                continue  # Для PK индексы создаются автоматически

            data_type_upper = attr.data_type.upper()

            if data_type_upper.startswith('VARCHAR') or data_type_upper == 'TEXT':
                # Индекс для LIKE запросов по текстовым полям
                index_name = f"idx_{relation.name}_{attr.name}_pattern"
                try:
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {index_name} "
                        f"ON {relation.name} ({attr.name} varchar_pattern_ops)"
                    )
                    print(f"[INFO] Создан индекс для LIKE запросов: {index_name}")
                except Exception as e:
                    print(f"[WARNING] Не удалось создать индекс {index_name}: {e}")

            elif data_type_upper in ['INTEGER', 'DECIMAL', 'NUMERIC', 'DATE']:
                # B-tree индекс для числовых и временных типов (для диапазонных запросов)
                index_name = f"idx_{relation.name}_{attr.name}_btree"
                try:
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {index_name} "
                        f"ON {relation.name} ({attr.name})"
                    )
                    print(f"[INFO] Создан B-tree индекс: {index_name}")
                except Exception as e:
                    print(f"[WARNING] Не удалось создать индекс {index_name}: {e}")

            elif data_type_upper == 'BOOLEAN':
                # Для булевых полей индекс имеет смысл только при неравномерном распределении
                # Проверяем селективность
                try:
                    cur.execute(
                        f"SELECT COUNT(*) FILTER (WHERE {attr.name} = TRUE) as true_count, "
                        f"COUNT(*) FILTER (WHERE {attr.name} = FALSE) as false_count "
                        f"FROM {relation.name}"
                    )
                    true_count, false_count = cur.fetchone()
                    total = true_count + false_count

                    # Создаем индекс только если распределение сильно неравномерное
                    if total > 0:
                        true_ratio = true_count / total
                        if true_ratio < 0.1 or true_ratio > 0.9:
                            index_name = f"idx_{relation.name}_{attr.name}_bool"
                            cur.execute(
                                f"CREATE INDEX IF NOT EXISTS {index_name} "
                                f"ON {relation.name} ({attr.name})"
                            )
                            print(f"[INFO] Создан индекс для boolean поля: {index_name}")
                except Exception as e:
                    print(f"[WARNING] Ошибка при анализе boolean поля {attr.name}: {e}")

        # Составные индексы для часто используемых комбинаций
        pk_attrs = [attr for attr in relation.attributes if attr.is_primary_key]
        if len(pk_attrs) > 1:
            pk_cols = ", ".join([a.name for a in pk_attrs])
            index_name = f"idx_{relation.name}_pk_composite"
            try:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} "
                    f"ON {relation.name} ({pk_cols})"
                )
                print(f"[INFO] Создан составной индекс: {index_name}")
            except Exception as e:
                print(f"[WARNING] Ошибка создания составного индекса: {e}")

        conn.commit()


def build_optimal_join(relations: List[Relation]) -> str:
    """
    Построить оптимальный JOIN с учетом общих атрибутов
    """
    if len(relations) == 1:
        return relations[0].name

    # Анализируем граф соединений
    join_graph = {}
    for i, rel1 in enumerate(relations):
        join_graph[rel1.name] = []
        for j, rel2 in enumerate(relations):
            if i != j:
                common = set(a.name for a in rel1.attributes) & set(a.name for a in rel2.attributes)
                if common:
                    join_graph[rel1.name].append((rel2.name, list(common)[0]))

    # Строим JOIN начиная с таблицы с наибольшим количеством связей
    start_table = max(join_graph.keys(), key=lambda k: len(join_graph[k]))

    joined = {start_table}
    join_clause = start_table

    while len(joined) < len(relations):
        for rel_name in join_graph:
            if rel_name not in joined:
                # Ищем, с какой из уже присоединенных таблиц можно соединить
                for joined_rel in joined:
                    for target, attr in join_graph[joined_rel]:
                        if target == rel_name:
                            join_clause += f" JOIN {rel_name} ON {joined_rel}.{attr} = {rel_name}.{attr}"
                            joined.add(rel_name)
                            break
                    if rel_name in joined:
                        break

    return join_clause


def classify_normalization_level(relations: List[Relation]) -> str:
    """Определить уровень нормализации по именам таблиц"""
    for rel in relations:
        name_lower = rel.name.lower()
        if '4nf' in name_lower:
            return "4NF"
        elif 'bcnf' in name_lower:
            return "BCNF"
        elif '3nf' in name_lower:
            return "3NF"
        elif any(x in name_lower for x in ['2nf', 'partial', 'main']):
            return "2NF"
    return "Unknown"


def _time_query(conn: psycopg2.extensions.connection, sql: str, repeats: int = 5) -> float:
    """
    Запустить запрос и измерить время выполнения с улучшенной диагностикой
    """
    if not sql or sql.strip() == "":
        print(f"[WARNING] Пустой SQL запрос")
        return float("nan")

    total_time = 0.0
    successful_runs = 0

    # Сохраняем текущий уровень изоляции
    old_isolation_level = conn.isolation_level

    try:
        # Устанавливаем режим autocommit
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        for run in range(repeats):
            try:
                with conn.cursor() as cur:
                    # Очищаем кеш
                    cur.execute("DISCARD ALL;")

                    # Замеряем время выполнения
                    start = time.perf_counter()
                    cur.execute(sql)
                    rows = cur.fetchall()
                    end = time.perf_counter()

                    elapsed = end - start
                    total_time += elapsed
                    successful_runs += 1

                    # Диагностика первого запуска
                    if run == 0:
                        print(f"    [DEBUG] Запрос выполнен за {elapsed:.4f} сек, получено {len(rows)} строк")

            except Exception as e:
                print(f"    [ERROR] Ошибка выполнения запроса (попытка {run + 1}): {e}")
                print(f"    [ERROR] SQL: {sql[:100]}...")

    except Exception as e:
        print(f"    [CRITICAL] Ошибка настройки соединения: {e}")
    finally:
        # Восстанавливаем уровень изоляции
        try:
            conn.set_isolation_level(old_isolation_level)
        except:
            pass

    if successful_runs == 0:
        print(f"    [ERROR] Ни одна попытка выполнения запроса не удалась")
        return float("nan")

    avg_time = total_time / successful_runs
    print(f"    [INFO] Среднее время: {avg_time:.4f} сек ({successful_runs} успешных запусков)")
    return avg_time


def run_performance_test(
        orig_rel: Relation,
        num_rows: int = 1000,
        repeats: int = 5
) -> Dict[str, Dict[str, float]]:
    """
    Тестирование производительности с улучшенной диагностикой
    """
    conn = connect()
    results: Dict[str, Dict[str, float]] = {}

    try:
        print(f"\n[INFO] === НАЧАЛО ТЕСТА ПРОИЗВОДИТЕЛЬНОСТИ ===")
        print(f"[INFO] Параметры: {num_rows} строк, {repeats} повторений")

        # Создание и заполнение исходной таблицы
        drop_table_if_exists(conn, orig_rel.name)
        create_table(conn, orig_rel)
        insert_random_data(conn, orig_rel, num_rows)

        # Создание индексов для оптимизации запросов
        create_query_indexes(conn, orig_rel)

        # Проверка данных
        actual_count = count_rows(conn, orig_rel.name)
        print(f"[INFO] Таблица {orig_rel.name} создана, строк: {actual_count}")

        # Определяем текущую нормальную форму
        analyzer = NormalFormAnalyzer(orig_rel)
        current_nf, _ = analyzer.determine_normal_form()
        print(f"[INFO] Текущая нормальная форма: {current_nf.value}")

        # Определяем уровни для тестирования
        nf_order = [NormalForm.SECOND_NF, NormalForm.THIRD_NF, NormalForm.BCNF, NormalForm.FOURTH_NF]
        levels_to_test = [nf for nf in nf_order if current_nf.value < nf.value]

        if not levels_to_test:
            print(f"[WARNING] Отношение уже в максимальной форме {current_nf.value}")
            levels_to_test = nf_order  # Тестируем все уровни для демонстрации

        print(f"[INFO] Будут протестированы уровни: {[nf.value for nf in levels_to_test]}")

        # Декомпозиция
        decompositions = {}

        if NormalForm.SECOND_NF in levels_to_test:
            print(f"\n[INFO] Выполняется декомпозиция в 2НФ...")
            decompositions["2NF"] = Decomposer.decompose_to_2nf(orig_rel)

        if NormalForm.THIRD_NF in levels_to_test:
            print(f"\n[INFO] Выполняется декомпозиция в 3НФ...")
            decompositions["3NF"] = Decomposer.decompose_to_3nf(orig_rel)

        if NormalForm.BCNF in levels_to_test:
            print(f"\n[INFO] Выполняется декомпозиция в НФБК...")
            decompositions["BCNF"] = Decomposer.decompose_to_bcnf(orig_rel)

        if NormalForm.FOURTH_NF in levels_to_test:
            print(f"\n[INFO] Выполняется декомпозиция в 4НФ...")
            decompositions["4NF"] = Decomposer.decompose_to_4nf(orig_rel)

        # Создание таблиц для каждого уровня
        for level_name, decomp_result in decompositions.items():
            print(f"\n[INFO] Создание таблиц для {level_name}...")

            for rel in decomp_result.decomposed_relations:
                drop_table_if_exists(conn, rel.name)

            create_and_populate_normalized(conn, orig_rel, decomp_result.decomposed_relations)

            for rel in decomp_result.decomposed_relations:
                create_query_indexes(conn, rel)
                count = count_rows(conn, rel.name)
                print(f"  - {rel.name}: {count} строк")

        # Измерение производительности
        print("\n[INFO] === ИЗМЕРЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ===")

        # Запросы для исходной таблицы
        print("\n[INFO] Тестирование Original...")
        orig_queries = _build_realistic_select_queries(orig_rel, [])
        results["Original"] = {}

        for query_type, sql in orig_queries["Original"].items():
            if sql and sql.strip():
                print(f"\n[INFO] Original/{query_type}:")
                print(f"  SQL: {sql[:100]}...")
                results["Original"][query_type] = _time_query(conn, sql, repeats)
            else:
                print(f"[WARNING] Пропущен пустой запрос Original/{query_type}")
                results["Original"][query_type] = float("nan")

        # Запросы для нормализованных таблиц
        for level_name, decomp_result in decompositions.items():
            print(f"\n[INFO] Тестирование {level_name}...")
            results[level_name] = {}

            level_queries = _build_realistic_select_queries(orig_rel, decomp_result.decomposed_relations)

            if level_name in level_queries:
                for query_type, sql in level_queries[level_name].items():
                    if sql and sql.strip():
                        print(f"\n[INFO] {level_name}/{query_type}:")
                        print(f"  SQL: {sql[:100]}...")
                        results[level_name][query_type] = _time_query(conn, sql, repeats)
                    else:
                        print(f"[WARNING] Пропущен пустой запрос {level_name}/{query_type}")
                        results[level_name][query_type] = float("nan")
            else:
                print(f"[ERROR] Не удалось построить запросы для {level_name}")
                # Заполняем NaN значениями
                for query_type in ["pk_point_lookup", "pk_range_scan", "non_pk_filter", "aggregation", "ordered_limit"]:
                    results[level_name][query_type] = float("nan")

        print("\n[INFO] === ТЕСТ ЗАВЕРШЕН ===")

        # Выводим итоговую таблицу результатов
        print("\nИТОГОВАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ (время в секундах):")
        print("-" * 80)
        print(
            f"{'Уровень':<10} | {'PK lookup':<12} | {'PK range':<12} | {'Non-PK':<12} | {'Aggregation':<12} | {'Order+Limit':<12}")
        print("-" * 80)

        for level in ["Original", "2NF", "3NF", "BCNF", "4NF"]:
            if level in results:
                row = f"{level:<10} |"
                for qt in ["pk_point_lookup", "pk_range_scan", "non_pk_filter", "aggregation", "ordered_limit"]:
                    val = results[level].get(qt, float("nan"))
                    if not np.isnan(val):
                        row += f" {val:>11.4f} |"
                    else:
                        row += f" {'N/A':>11} |"
                print(row)

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    return results


def plot_performance(results: Dict[str, Dict[str, float]]):
    """
    Построение графиков производительности с использованием паттернов заполнения
    вместо цветов для лучшей различимости.
    """
    # Определяем доступные уровни из результатов
    available_levels = []
    for level in ["Original", "2NF", "3NF", "BCNF", "4NF"]:
        if level in results:
            available_levels.append(level)

    query_types = ["all", "pk", "non_pk"]

    # Паттерны заполнения для различения столбцов
    hatches = ['', '///', '\\\\\\', '|||', '---', '+++', 'xxx', 'ooo']

    # Подготовим данные
    x = []
    data = {q: [] for q in query_types}
    for lvl in available_levels:
        x.append(lvl)
        for qt in query_types:
            data[qt].append(results[lvl].get(qt, float("nan")))

    # Создаем общую фигуру с подграфиками
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    for idx, qt in enumerate(query_types):
        ax = axes[idx]
        y = data[qt]

        # Создаем столбчатую диаграмму с паттернами
        bars = ax.bar(x, y, color='lightgray', edgecolor='black', linewidth=1.5)

        # Применяем разные паттерны к столбцам
        for i, bar in enumerate(bars):
            bar.set_hatch(hatches[i % len(hatches)])

        ax.set_title(f"Время выполнения SELECT-{qt}", fontsize=12, fontweight='bold')
        ax.set_xlabel("Уровень нормализации", fontsize=10)
        ax.set_ylabel("Время (сек)", fontsize=10)
        ax.grid(True, axis='y', alpha=0.3)

        # Добавляем значения на столбцы
        for i, v in enumerate(y):
            if not np.isnan(v):
                ax.text(i, v, f'{v:.3f}', ha='center', va='bottom', fontsize=9)

    # Добавляем легенду с паттернами
    legend_elements = []
    for i, level in enumerate(available_levels):
        patch = mpatches.Patch(facecolor='lightgray', edgecolor='black',
                               hatch=hatches[i % len(hatches)], label=level)
        legend_elements.append(patch)

    fig.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(0.98, 0.98))

    plt.tight_layout()
    plt.show()