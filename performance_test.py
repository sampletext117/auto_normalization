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


def build_realistic_queries(orig_rel: Relation, normalized_rels: List[Relation]) -> Dict[str, List[Tuple[str, str]]]:
    """
    Построить реалистичные запросы для тестирования с учетом типов данных
    """
    queries = {}

    # Атрибуты
    all_attrs = [a for a in orig_rel.attributes]
    pk_attrs = [a for a in orig_rel.attributes if a.is_primary_key]
    non_pk_attrs = [a for a in orig_rel.attributes if not a.is_primary_key]

    # --- Запросы для исходной таблицы ---
    orig_queries = []

    # 1. Полная выборка
    orig_queries.append(("full_scan", f"SELECT * FROM {orig_rel.name}"))

    # 2. Выборка по первичному ключу
    if pk_attrs:
        pk_attr = pk_attrs[0]
        # Используем правильное значение в зависимости от типа
        if pk_attr.data_type == "INTEGER":
            pk_value = "1"
        elif pk_attr.data_type == "VARCHAR":
            pk_value = "'a%'"
            orig_queries.append(
                ("pk_lookup", f"SELECT * FROM {orig_rel.name} WHERE {pk_attr.name} LIKE {pk_value} LIMIT 10"))
        else:
            pk_value = "'2020-01-01'"
            orig_queries.append(
                ("pk_lookup", f"SELECT * FROM {orig_rel.name} WHERE {pk_attr.name} = {pk_value}::date LIMIT 10"))

    # 3. Агрегация
    if non_pk_attrs:
        # Выбираем атрибут для агрегации
        agg_attr = non_pk_attrs[0]
        orig_queries.append(("aggregation", f"SELECT COUNT(*), COUNT(DISTINCT {agg_attr.name}) FROM {orig_rel.name}"))

    # 4. Группировка
    if len(non_pk_attrs) >= 2:
        group_attr = non_pk_attrs[0]
        orig_queries.append(("group_by",
                             f"SELECT {group_attr.name}, COUNT(*) FROM {orig_rel.name} GROUP BY {group_attr.name} LIMIT 50"))

    # 5. Фильтрация по неиндексированному полю
    if non_pk_attrs:
        filter_attr = non_pk_attrs[0]
        if filter_attr.data_type == "VARCHAR":
            orig_queries.append(
                ("filter_non_indexed", f"SELECT * FROM {orig_rel.name} WHERE {filter_attr.name} LIKE 'a%' LIMIT 100"))
        elif filter_attr.data_type == "INTEGER":
            orig_queries.append(
                ("filter_non_indexed", f"SELECT * FROM {orig_rel.name} WHERE {filter_attr.name} > 5000 LIMIT 100"))

    # 6. Сортировка
    if non_pk_attrs:
        sort_attr = non_pk_attrs[0]
        orig_queries.append(("order_by", f"SELECT * FROM {orig_rel.name} ORDER BY {sort_attr.name} LIMIT 100"))

    queries["Original"] = orig_queries

    # --- Запросы для нормализованных таблиц ---
    if not normalized_rels:
        return queries

    # Определяем уровень нормализации
    level = "3NF"  # По умолчанию
    for r in normalized_rels:
        if "_4nf" in r.name.lower():
            level = "4NF"
            break
        elif "_bcnf" in r.name.lower():
            level = "BCNF"
            break
        elif "_3nf" in r.name.lower():
            level = "3NF"
            break
        elif "_2nf" in r.name.lower() or "_partial" in r.name.lower():
            level = "2NF"
            break

    norm_queries = []

    # 1. JOIN для восстановления полных данных
    if len(normalized_rels) > 1:
        # Строим JOIN
        join_clause = normalized_rels[0].name
        used_tables = {normalized_rels[0].name}

        for i in range(1, len(normalized_rels)):
            rel = normalized_rels[i]
            # Ищем общие атрибуты
            common_attrs = []
            for attr in rel.attributes:
                for prev_rel in normalized_rels[:i]:
                    if any(a.name == attr.name for a in prev_rel.attributes):
                        common_attrs.append((attr.name, prev_rel.name))
                        break

            if common_attrs:
                attr_name, join_table = common_attrs[0]
                join_clause += f" JOIN {rel.name} ON {rel.name}.{attr_name} = {join_table}.{attr_name}"
            else:
                # Если нет общих атрибутов, пропускаем эту таблицу
                continue

        # Выбираем некоторые столбцы (не все, чтобы не перегружать)
        select_cols = []
        col_count = 0
        for rel in normalized_rels:
            for attr in rel.attributes:
                if col_count < 5:  # Ограничиваем количество столбцов
                    select_cols.append(f"{rel.name}.{attr.name}")
                    col_count += 1

        if len(select_cols) > 0:
            norm_queries.append(("full_join", f"SELECT {', '.join(select_cols)} FROM {join_clause} LIMIT 100"))

        # 2. JOIN с фильтрацией
        if pk_attrs:
            # Находим таблицу с PK атрибутом
            pk_attr = pk_attrs[0]
            pk_table = None
            for rel in normalized_rels:
                if any(a.name == pk_attr.name for a in rel.attributes):
                    pk_table = rel.name
                    break

            if pk_table and len(select_cols) > 0:
                if pk_attr.data_type == "VARCHAR":
                    filter_condition = f"{pk_table}.{pk_attr.name} LIKE 'a%'"
                elif pk_attr.data_type == "INTEGER":
                    filter_condition = f"{pk_table}.{pk_attr.name} < 100"
                else:
                    filter_condition = f"{pk_table}.{pk_attr.name} = '2020-01-01'::date"

                norm_queries.append(("join_with_filter",
                                     f"SELECT {', '.join(select_cols[:3])} FROM {join_clause} WHERE {filter_condition} LIMIT 50"))

    # 3. Запрос к отдельной таблице (без JOIN)
    if normalized_rels:
        first_table = normalized_rels[0]
        norm_queries.append(("single_table", f"SELECT * FROM {first_table.name} LIMIT 100"))

    # 4. Агрегация после JOIN
    if len(normalized_rels) > 1 and non_pk_attrs:
        # Находим таблицу с non-PK атрибутом
        agg_attr = None
        agg_table = None
        for attr in non_pk_attrs:
            for rel in normalized_rels:
                if any(a.name == attr.name for a in rel.attributes):
                    agg_attr = attr.name
                    agg_table = rel.name
                    break
            if agg_attr:
                break

        if agg_attr and 'join_clause' in locals():
            norm_queries.append(("join_aggregation",
                                 f"SELECT COUNT(*), COUNT(DISTINCT {agg_table}.{agg_attr}) FROM {join_clause} LIMIT 1"))

    # 5. Запрос с подзапросом
    if len(normalized_rels) >= 2:
        table1 = normalized_rels[0]
        table2 = normalized_rels[1]
        # Ищем общий атрибут
        common = None
        for a1 in table1.attributes:
            for a2 in table2.attributes:
                if a1.name == a2.name:
                    common = a1.name
                    break
            if common:
                break

        if common:
            norm_queries.append(("subquery",
                                 f"SELECT COUNT(*) FROM {table1.name} WHERE {common} IN (SELECT {common} FROM {table2.name} LIMIT 50)"))

    # 6. Сортировка в отдельной таблице
    if normalized_rels and len(normalized_rels[0].attributes) > 0:
        first_table = normalized_rels[0]
        first_attr = first_table.attributes[0]
        norm_queries.append(("order_by_single",
                             f"SELECT * FROM {first_table.name} ORDER BY {first_attr.name} LIMIT 100"))

    queries[level] = norm_queries

    return queries


def create_realistic_indexes(conn, orig_rel: Relation, decomposed_rels: List[Relation]):
    """
    Создать реалистичные индексы для нормализованных таблиц
    """
    with conn.cursor() as cur:
        # Для каждой декомпозированной таблицы
        for rel in decomposed_rels:
            analyzer = NormalFormAnalyzer(rel)

            # 1. Первичные ключи (обычно создаются автоматически при CREATE TABLE с PRIMARY KEY)
            # Но мы создаем их явно, так как таблицы создаются без PK
            if analyzer.candidate_keys:
                pk_attrs = list(analyzer.candidate_keys)[0]
                if pk_attrs:  # Если есть кандидатный ключ
                    pk_cols = ", ".join([a.name for a in pk_attrs])
                    idx_name = f"idx_{rel.name}_pk"
                    try:
                        cur.execute(f"CREATE UNIQUE INDEX {idx_name} ON {rel.name} ({pk_cols})")
                        print(f"  [INDEX] Создан уникальный индекс {idx_name}")
                    except Exception as e:
                        if "already exists" not in str(e):
                            print(f"  [INDEX] Ошибка создания индекса {idx_name}: {e}")

            # 2. Внешние ключи (атрибуты, которые есть в других таблицах и используются для JOIN)
            for attr in rel.attributes:
                # Проверяем, есть ли этот атрибут в других таблицах
                is_join_attr = False
                for other_rel in decomposed_rels:
                    if other_rel.name != rel.name:
                        if any(a.name == attr.name for a in other_rel.attributes):
                            is_join_attr = True
                            break

                # Создаем индекс для атрибута, используемого в JOIN, если он не входит в PK
                if is_join_attr:
                    # Проверяем, не входит ли атрибут уже в какой-либо из candidate keys
                    is_in_key = False
                    if analyzer.candidate_keys:
                        for key in analyzer.candidate_keys:
                            if attr in key:
                                is_in_key = True
                                break

                    if not is_in_key:
                        idx_name = f"idx_{rel.name}_{attr.name}"
                        try:
                            cur.execute(f"CREATE INDEX {idx_name} ON {rel.name} ({attr.name})")
                            print(f"  [INDEX] Создан индекс {idx_name} для JOIN")
                        except Exception as e:
                            if "already exists" not in str(e):
                                print(f"  [INDEX] Ошибка создания индекса {idx_name}: {e}")

        conn.commit()


def _time_query(conn: psycopg2.extensions.connection, sql: str, repeats: int = 5) -> float:
    """
    Запустить запрос и измерить время выполнения
    """
    if not sql or sql.strip() == "":
        return float("nan")

    times = []

    for i in range(repeats):
        try:
            with conn.cursor() as cur:
                start = time.perf_counter()
                cur.execute(sql)
                rows = cur.fetchall()
                end = time.perf_counter()

                elapsed = end - start
                times.append(elapsed)

                if i == 0:  # Отладочная информация только для первого запуска
                    print(f"({len(rows)} строк, {elapsed:.3f}с)", end=' ')

        except Exception as e:
            print(f"\n    [ERROR] {e}")
            return float("nan")

    if times:
        return np.mean(times)
    else:
        return float("nan")


def run_performance_test(
        orig_rel: Relation,
        num_rows: int = 10000,
        repeats: int = 10
) -> Dict[str, Dict[str, float]]:
    """
    Тестирование производительности с реалистичными запросами
    """
    conn = connect()
    results: Dict[str, Dict[str, float]] = {}

    try:
        print(f"\n{'=' * 60}")
        print(f"ТЕСТ ПРОИЗВОДИТЕЛЬНОСТИ ЗАПРОСОВ")
        print(f"{'=' * 60}")
        print(f"Количество строк: {num_rows}")
        print(f"Повторений на запрос: {repeats}")
        print(f"{'=' * 60}\n")

        # Создание и заполнение исходной таблицы
        drop_table_if_exists(conn, orig_rel.name)
        create_table(conn, orig_rel)

        # ВАЖНО: Генерируем данные с реальной избыточностью
        # Модифицируем generate_value_for для создания повторяющихся значений
        import data_test
        original_generate = data_test.generate_value_for

        def generate_value_with_redundancy(attr: Attribute):
            """Генерация значений с контролируемой избыточностью"""
            import random

            # Для некоторых атрибутов создаем ограниченный набор значений
            if not attr.is_primary_key:
                if attr.data_type == "VARCHAR":
                    # Ограниченный набор значений для создания избыточности
                    values = [f"value_{i}" for i in range(20)]
                    return random.choice(values)
                elif attr.data_type == "INTEGER":
                    # Числа из ограниченного диапазона
                    return random.randint(1, 50)

            # Для первичных ключей и остальных - уникальные значения
            return original_generate(attr)

        # Временно заменяем функцию генерации
        data_test.generate_value_for = generate_value_with_redundancy
        insert_random_data(conn, orig_rel, num_rows)
        # Восстанавливаем оригинальную функцию
        data_test.generate_value_for = original_generate

        # Анализ статистики для оптимизатора
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {orig_rel.name}")
        conn.commit()

        print(f"[INFO] Создана таблица {orig_rel.name} с {num_rows} строками (с контролируемой избыточностью)")

        # Определяем текущую нормальную форму
        analyzer = NormalFormAnalyzer(orig_rel)
        current_nf, _ = analyzer.determine_normal_form()
        print(f"[INFO] Текущая нормальная форма: {current_nf.value}\n")

        # Декомпозиции
        decompositions = {}

        if current_nf.value < NormalForm.SECOND_NF.value:
            decompositions["2NF"] = Decomposer.decompose_to_2nf(orig_rel)

        if current_nf.value < NormalForm.THIRD_NF.value:
            decompositions["3NF"] = Decomposer.decompose_to_3nf(orig_rel)

        if current_nf.value < NormalForm.BCNF.value:
            decompositions["BCNF"] = Decomposer.decompose_to_bcnf(orig_rel)

        if current_nf.value < NormalForm.FOURTH_NF.value:
            decompositions["4NF"] = Decomposer.decompose_to_4nf(orig_rel)

        # Создание таблиц для каждого уровня
        all_queries = {}

        for level_name, decomp_result in decompositions.items():
            print(f"[INFO] Создание таблиц для {level_name}...")

            # Удаляем старые таблицы
            for rel in decomp_result.decomposed_relations:
                drop_table_if_exists(conn, rel.name)

            # Создаем и заполняем новые
            create_and_populate_normalized(conn, orig_rel, decomp_result.decomposed_relations)

            # Создаем индексы
            print(f"[INFO] Создание индексов для {level_name}...")
            create_realistic_indexes(conn, orig_rel, decomp_result.decomposed_relations)

            # Анализ статистики
            for rel in decomp_result.decomposed_relations:
                with conn.cursor() as cur:
                    cur.execute(f"ANALYZE {rel.name}")
            conn.commit()

            # Информация о таблицах
            for rel in decomp_result.decomposed_relations:
                count = count_rows(conn, rel.name)
                print(f"  - {rel.name}: {count} строк")

            # Генерируем запросы для этого уровня
            level_queries = build_realistic_queries(orig_rel, decomp_result.decomposed_relations)
            all_queries.update(level_queries)

        # Запросы для исходной таблицы
        orig_queries = build_realistic_queries(orig_rel, [])
        all_queries.update(orig_queries)

        # Измерение производительности
        print(f"\n{'=' * 60}")
        print(f"ИЗМЕРЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
        print(f"{'=' * 60}\n")

        for level, queries in all_queries.items():
            print(f"\n[{level}]")
            results[level] = {}

            for query_name, sql in queries:
                print(f"  {query_name}:", end=' ')

                # Прогрев кеша (1 запуск)
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        cur.fetchall()
                except:
                    pass

                # Измерение
                avg_time = _time_query(conn, sql, repeats)
                results[level][query_name] = avg_time

                if not np.isnan(avg_time):
                    print(f"→ {avg_time * 1000:.2f} мс")
                else:
                    print(f"→ ОШИБКА")

        print(f"\n{'=' * 60}\n")

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    return results


def plot_performance_histogram(results: Dict[str, Dict[str, float]]):
    """
    Построение гистограмм производительности
    """
    # Определяем доступные уровни и типы запросов
    levels = list(results.keys())
    query_types = set()
    for level_queries in results.values():
        query_types.update(level_queries.keys())
    query_types = sorted(list(query_types))

    # Фильтруем запросы с ошибками
    valid_query_types = []
    for qt in query_types:
        has_valid_data = False
        for level in levels:
            if qt in results[level] and not np.isnan(results[level][qt]):
                has_valid_data = True
                break
        if has_valid_data:
            valid_query_types.append(qt)

    if not valid_query_types:
        print("Нет валидных данных для построения графиков")
        return

    # Подготовка данных
    data = {}
    for qt in valid_query_types:
        data[qt] = []
        for level in levels:
            if qt in results[level] and not np.isnan(results[level][qt]):
                data[qt].append(results[level][qt] * 1000)  # В миллисекундах
            else:
                data[qt].append(0)

    # Создание фигуры
    n_queries = len(valid_query_types)
    n_cols = 3
    n_rows = (n_queries + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
    if n_rows == 1:
        axes = axes.reshape(1, -1)
    axes = axes.flatten()

    # Цвета для разных уровней нормализации
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57']

    # Описания типов запросов
    query_descriptions = {
        'full_scan': 'Полное сканирование',
        'pk_lookup': 'Поиск по ключу',
        'aggregation': 'Агрегация',
        'group_by': 'Группировка',
        'filter_non_indexed': 'Фильтрация без индекса',
        'order_by': 'Сортировка',
        'full_join': 'Полное соединение',
        'join_with_filter': 'JOIN с фильтром',
        'single_table': 'Одна таблица',
        'join_aggregation': 'Агрегация с JOIN',
        'subquery': 'Подзапрос',
        'order_by_single': 'Сортировка (одна таблица)'
    }

    for idx, (qt, values) in enumerate(data.items()):
        if idx >= len(axes):
            break

        ax = axes[idx]
        x = np.arange(len(levels))

        # Фильтруем только ненулевые значения для этого типа запроса
        valid_indices = [i for i, v in enumerate(values) if v > 0]
        if not valid_indices:
            ax.set_visible(False)
            continue

        valid_levels = [levels[i] for i in valid_indices]
        valid_values = [values[i] for i in valid_indices]
        valid_colors = [colors[i % len(colors)] for i in valid_indices]

        bars = ax.bar(range(len(valid_levels)), valid_values,
                      color=valid_colors, edgecolor='black', linewidth=1)

        title = query_descriptions.get(qt, qt)
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_xlabel('Уровень нормализации')
        ax.set_ylabel('Время выполнения (мс)')
        ax.set_xticks(range(len(valid_levels)))
        ax.set_xticklabels(valid_levels)
        ax.grid(True, axis='y', alpha=0.3)

        # Добавляем значения на столбцы
        for bar, val in zip(bars, valid_values):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        f'{val:.1f}', ha='center', va='bottom', fontsize=9)

        # Автомасштабирование оси Y
        if valid_values:
            max_val = max(valid_values)
            ax.set_ylim(0, max_val * 1.2)

    # Удаляем лишние субплоты
    for idx in range(len(data), len(axes)):
        fig.delaxes(axes[idx])

    plt.suptitle('Производительность запросов по уровням нормализации', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.show()

    # Дополнительная сводная диаграмма - сравнение общей производительности
    fig2, ax = plt.subplots(figsize=(10, 6))

    # Считаем среднее время для каждого уровня (исключая nan)
    avg_times_by_level = {}
    for level in levels:
        times = []
        for qt in valid_query_types:
            if qt in results[level] and not np.isnan(results[level][qt]):
                times.append(results[level][qt] * 1000)
        if times:
            avg_times_by_level[level] = np.mean(times)

    if avg_times_by_level:
        levels_sorted = list(avg_times_by_level.keys())
        avg_times = list(avg_times_by_level.values())

        bars = ax.bar(levels_sorted, avg_times,
                      color=[colors[i % len(colors)] for i in range(len(levels_sorted))],
                      edgecolor='black', linewidth=1)

        ax.set_xlabel('Уровень нормализации', fontsize=12)
        ax.set_ylabel('Среднее время выполнения (мс)', fontsize=12)
        ax.set_title('Общая производительность по уровням нормализации', fontsize=14, fontweight='bold')
        ax.grid(True, axis='y', alpha=0.3)

        # Добавляем значения
        for bar, val in zip(bars, avg_times):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f'{val:.1f}', ha='center', va='bottom')

        # Добавляем линию тренда
        x_pos = np.arange(len(levels_sorted))
        z = np.polyfit(x_pos, avg_times, 1)
        p = np.poly1d(z)
        ax.plot(x_pos, p(x_pos), "r--", alpha=0.8, linewidth=2,
                label=f'Тренд: {"+" if z[0] > 0 else ""}{z[0]:.1f} мс/уровень')
        ax.legend()

    plt.tight_layout()
    plt.show()