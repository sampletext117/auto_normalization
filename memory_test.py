import psycopg2
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from models import Relation, NormalForm, Attribute
from data_test import connect, drop_table_if_exists, create_table, insert_random_data, create_and_populate_normalized, \
    count_rows
from decomposition import Decomposer
from analyzer import NormalFormAnalyzer


def get_table_size_info(conn, table_name: str) -> Dict[str, int]:
    """
    Получить информацию о размере таблицы в PostgreSQL

    Returns:
        Dict с ключами:
        - table_size: размер самой таблицы (без индексов)
        - indexes_size: размер всех индексов
        - total_size: общий размер (таблица + индексы)
        - toast_size: размер TOAST таблицы
        - row_count: количество строк
    """
    with conn.cursor() as cur:
        # Размер таблицы без индексов
        cur.execute(f"""
            SELECT pg_relation_size('{table_name}') as table_size,
                   pg_indexes_size('{table_name}') as indexes_size,
                   pg_total_relation_size('{table_name}') as total_size,
                   (SELECT COUNT(*) FROM {table_name}) as row_count
        """)

        result = cur.fetchone()
        table_size, indexes_size, total_size, row_count = result

        # TOAST размер (для больших объектов)
        cur.execute(f"""
            SELECT pg_total_relation_size(reltoastrelid) as toast_size
            FROM pg_class
            WHERE relname = '{table_name}' AND reltoastrelid != 0
        """)

        toast_result = cur.fetchone()
        toast_size = toast_result[0] if toast_result else 0

        return {
            'table_size': table_size or 0,
            'indexes_size': indexes_size or 0,
            'total_size': total_size or 0,
            'toast_size': toast_size or 0,
            'row_count': row_count or 0
        }


def create_smart_indexes_for_relation(conn, relation: Relation, is_normalized: bool = False):
    """
    Создать только необходимые индексы с учетом типа таблицы
    """
    from analyzer import NormalFormAnalyzer

    with conn.cursor() as cur:
        # Анализируем структуру отношения
        analyzer = NormalFormAnalyzer(relation)
        candidate_keys = analyzer.candidate_keys

        # Для нормализованных таблиц создаем минимум индексов
        if is_normalized:
            # Только первичный ключ
            if candidate_keys:
                first_key = list(candidate_keys)[0]
                if len(first_key) > 1:  # Составной ключ
                    pk_cols = ", ".join([attr.name for attr in first_key])
                    index_name = f"idx_{relation.name}_pk"
                    try:
                        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {relation.name} ({pk_cols})")
                    except Exception as e:
                        print(f"[WARNING] Не удалось создать индекс {index_name}: {e}")
        else:
            # Для исходной таблицы создаем индексы как раньше
            pk_attrs = [attr.name for attr in relation.attributes if attr.is_primary_key]
            if pk_attrs and len(pk_attrs) > 1:
                pk_cols = ", ".join(pk_attrs)
                index_name = f"idx_{relation.name}_pk"
                try:
                    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {relation.name} ({pk_cols})")
                except Exception as e:
                    print(f"[WARNING] Не удалось создать индекс {index_name}: {e}")

            # Индексы для детерминантов ФЗ только если они часто используются
            for i, fd in enumerate(relation.functional_dependencies[:3]):  # Ограничиваем количество
                if len(fd.determinant) == 1:  # Только одноатрибутные детерминанты
                    det_col = list(fd.determinant)[0].name
                    index_name = f"idx_{relation.name}_fd{i}"
                    try:
                        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {relation.name} ({det_col})")
                    except Exception as e:
                        print(f"[WARNING] Не удалось создать индекс {index_name}: {e}")

        conn.commit()


def calculate_true_data_redundancy(conn, orig_rel: Relation, decomposed_rels: List[Relation]) -> Dict[str, float]:
    """
    Вычислить реальные метрики избыточности данных, а не просто подсчет атрибутов
    """
    with conn.cursor() as cur:
        # Подсчет реальной избыточности в исходной таблице
        # Для примера возьмем первый непервичный атрибут
        non_pk_attrs = [attr for attr in orig_rel.attributes if not attr.is_primary_key]

        redundancy_metrics = {}

        if non_pk_attrs:
            # Измеряем избыточность как отношение уникальных значений к общему количеству
            test_attr = non_pk_attrs[0].name

            # Для исходной таблицы
            cur.execute(f"SELECT COUNT(DISTINCT {test_attr}), COUNT({test_attr}) FROM {orig_rel.name}")
            unique_orig, total_orig = cur.fetchone()
            orig_redundancy = 1 - (unique_orig / total_orig) if total_orig > 0 else 0

            redundancy_metrics['original_redundancy'] = orig_redundancy * 100

            # Для нормализованных таблиц проверяем, устранена ли избыточность
            normalized_redundancy = 0
            tables_checked = 0

            for rel in decomposed_rels:
                # Ищем таблицу, содержащую этот атрибут
                if any(attr.name == test_attr for attr in rel.attributes):
                    cur.execute(f"SELECT COUNT(DISTINCT {test_attr}), COUNT({test_attr}) FROM {rel.name}")
                    unique_norm, total_norm = cur.fetchone()
                    if total_norm > 0:
                        norm_redundancy = 1 - (unique_norm / total_norm)
                        normalized_redundancy += norm_redundancy
                        tables_checked += 1

            if tables_checked > 0:
                redundancy_metrics['normalized_redundancy'] = (normalized_redundancy / tables_checked) * 100
            else:
                redundancy_metrics['normalized_redundancy'] = 0

            # Реальная экономия от устранения избыточности
            redundancy_metrics['redundancy_reduction'] = orig_redundancy - (
                        normalized_redundancy / max(tables_checked, 1))

        # Подсчет дублирования ключей (это ожидаемые накладные расходы)
        key_overhead = 0
        for rel in decomposed_rels:
            # Подсчитываем, сколько раз ключевые атрибуты появляются в разных таблицах
            for attr in rel.attributes:
                if any(a.name == attr.name and a.is_primary_key for a in orig_rel.attributes):
                    key_overhead += 1

        redundancy_metrics['key_duplication_overhead'] = key_overhead - len(
            [a for a in orig_rel.attributes if a.is_primary_key])

        return redundancy_metrics


def calculate_duplication_factor(orig_rel: Relation, decomposed_rels: List[Relation]) -> float:
    """
    Вычислить коэффициент дублирования данных при декомпозиции.
    Возвращает отношение общего количества атрибутов после декомпозиции
    к количеству атрибутов в исходном отношении.
    """
    original_attrs = len(orig_rel.attributes)
    total_decomposed_attrs = sum(len(rel.attributes) for rel in decomposed_rels)

    return total_decomposed_attrs / original_attrs if original_attrs > 0 else 1.0


def measure_data_redundancy(conn, table_name: str, attributes: List[Attribute]) -> Dict[str, float]:
    """
    Измерить реальную избыточность данных в таблице

    Returns:
        Dict с метриками избыточности:
        - redundancy_ratio: общий коэффициент избыточности (0-1)
        - duplicate_rows: количество полных дубликатов строк
        - attribute_redundancy: избыточность по каждому атрибуту
    """
    with conn.cursor() as cur:
        # Общее количество строк
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cur.fetchone()[0]

        if total_rows == 0:
            return {
                'redundancy_ratio': 0.0,
                'duplicate_rows': 0,
                'attribute_redundancy': {}
            }

        # Количество уникальных строк
        all_cols = ", ".join([attr.name for attr in attributes])
        cur.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT {all_cols} FROM {table_name}) AS unique_rows")
        unique_rows = cur.fetchone()[0]

        # Полные дубликаты строк
        duplicate_rows = total_rows - unique_rows

        # Избыточность по отдельным атрибутам
        attribute_redundancy = {}
        total_redundancy = 0.0

        for attr in attributes:
            # Пропускаем первичные ключи, так как они по определению уникальны
            if attr.is_primary_key:
                attribute_redundancy[attr.name] = 0.0
                continue

            # Количество уникальных значений
            cur.execute(f"SELECT COUNT(DISTINCT {attr.name}) FROM {table_name}")
            unique_values = cur.fetchone()[0]

            # Теоретический минимум строк для хранения этих значений
            min_rows = unique_values

            # Избыточность = (фактические строки - минимальные строки) / фактические строки
            redundancy = (total_rows - min_rows) / total_rows if total_rows > 0 else 0
            attribute_redundancy[attr.name] = redundancy
            total_redundancy += redundancy

        # Средняя избыточность по всем непервичным атрибутам
        non_pk_count = len([a for a in attributes if not a.is_primary_key])
        avg_redundancy = total_redundancy / non_pk_count if non_pk_count > 0 else 0

        return {
            'redundancy_ratio': avg_redundancy,
            'duplicate_rows': duplicate_rows,
            'attribute_redundancy': attribute_redundancy,
            'total_rows': total_rows,
            'unique_rows': unique_rows
        }


def calculate_normalization_efficiency(conn, orig_rel: Relation, decomposed_rels: List[Relation]) -> Dict[str, float]:
    """
    Вычислить эффективность нормализации на основе реальных данных

    Returns:
        Dict с метриками эффективности:
        - space_efficiency: эффективность использования пространства
        - redundancy_elimination: процент устраненной избыточности
        - key_overhead: накладные расходы на дублирование ключей
    """
    # Измеряем избыточность в исходной таблице
    orig_redundancy = measure_data_redundancy(conn, orig_rel.name, orig_rel.attributes)

    # Измеряем избыточность в нормализованных таблицах
    normalized_redundancy_sum = 0.0
    normalized_total_rows = 0
    key_duplication_count = 0

    # Подсчитываем атрибуты, которые стали ключами в нормализованных таблицах
    normalized_key_attrs = set()

    for rel in decomposed_rels:
        # Измеряем избыточность каждой нормализованной таблицы
        rel_redundancy = measure_data_redundancy(conn, rel.name, rel.attributes)

        # Взвешиваем по количеству строк
        rel_weight = rel_redundancy['total_rows'] / max(orig_redundancy['total_rows'], 1)
        normalized_redundancy_sum += rel_redundancy['redundancy_ratio'] * rel_weight
        normalized_total_rows += rel_redundancy['total_rows']

        # Анализируем дублирование ключей
        from analyzer import NormalFormAnalyzer
        analyzer = NormalFormAnalyzer(rel)
        for key_set in analyzer.candidate_keys:
            for key_attr in key_set:
                # Если этот атрибут был в исходной таблице
                if any(a.name == key_attr.name for a in orig_rel.attributes):
                    normalized_key_attrs.add(key_attr.name)

    # Подсчитываем, сколько раз каждый ключевой атрибут появляется в разных таблицах
    for attr_name in normalized_key_attrs:
        appearances = sum(1 for rel in decomposed_rels
                          if any(a.name == attr_name for a in rel.attributes))
        if appearances > 1:
            key_duplication_count += appearances - 1

    # Вычисляем метрики эффективности
    redundancy_elimination = max(0, orig_redundancy['redundancy_ratio'] - normalized_redundancy_sum)
    redundancy_elimination_percent = (redundancy_elimination / max(orig_redundancy['redundancy_ratio'], 0.001)) * 100

    # Эффективность использования пространства
    # Учитываем как устранение избыточности, так и накладные расходы на ключи
    space_overhead = key_duplication_count * 0.1  # Предполагаем 10% накладных расходов на каждое дублирование ключа
    space_efficiency = max(0, redundancy_elimination_percent - space_overhead)

    return {
        'space_efficiency': space_efficiency,
        'redundancy_elimination': redundancy_elimination_percent,
        'key_overhead': key_duplication_count,
        'original_redundancy': orig_redundancy['redundancy_ratio'] * 100,
        'normalized_redundancy': normalized_redundancy_sum * 100,
        'row_multiplication_factor': normalized_total_rows / max(orig_redundancy['total_rows'], 1)
    }


def get_detailed_table_metrics(conn, table_name: str, attributes: List[Attribute]) -> Dict[str, any]:
    """
    Получить детальные метрики таблицы включая размеры и статистику
    """
    metrics = {}

    with conn.cursor() as cur:
        # Базовые размеры
        cur.execute(f"""
            SELECT 
                pg_relation_size('{table_name}') as table_size,
                pg_indexes_size('{table_name}') as indexes_size,
                pg_total_relation_size('{table_name}') as total_size,
                (SELECT COUNT(*) FROM {table_name}) as row_count
        """)

        table_size, indexes_size, total_size, row_count = cur.fetchone()

        metrics['table_size'] = table_size or 0
        metrics['indexes_size'] = indexes_size or 0
        metrics['total_size'] = total_size or 0
        metrics['row_count'] = row_count or 0
        metrics['toast_size'] = 0  # Добавляем для совместимости

        # Средний размер строки
        metrics['avg_row_size'] = metrics['table_size'] / max(metrics['row_count'], 1)

        # Статистика по атрибутам - исправленная версия
        attribute_stats = {}
        for attr in attributes:
            try:
                # Считаем количество уникальных и дублированных значений
                cur.execute(f"""
                    SELECT 
                        COUNT(DISTINCT {attr.name}) as distinct_values,
                        COUNT({attr.name}) as total_values,
                        COUNT({attr.name}) - COUNT(DISTINCT {attr.name}) as duplicate_values
                    FROM {table_name}
                    WHERE {attr.name} IS NOT NULL
                """)

                result = cur.fetchone()
                if result:
                    distinct_values, total_values, duplicate_values = result

                    # Для вычисления среднего размера значения берем небольшую выборку
                    cur.execute(f"""
                        SELECT AVG(pg_column_size({attr.name}::text)) as avg_size
                        FROM (
                            SELECT {attr.name} 
                            FROM {table_name} 
                            WHERE {attr.name} IS NOT NULL 
                            LIMIT 100
                        ) as sample
                    """)

                    avg_size_result = cur.fetchone()
                    avg_value_size = avg_size_result[0] if avg_size_result and avg_size_result[0] else 0

                    attribute_stats[attr.name] = {
                        'distinct_values': distinct_values or 0,
                        'duplicate_values': duplicate_values or 0,
                        'avg_value_size': avg_value_size,
                        'selectivity': (distinct_values or 0) / max(row_count, 1)
                    }
            except Exception as e:
                print(f"[WARNING] Не удалось получить статистику для атрибута {attr.name}: {e}")
                attribute_stats[attr.name] = {
                    'distinct_values': 0,
                    'duplicate_values': 0,
                    'avg_value_size': 0,
                    'selectivity': 0
                }

        metrics['attribute_stats'] = attribute_stats

    return metrics


# Модифицированная функция run_memory_test
def run_memory_test(
        orig_rel: Relation,
        num_rows: int = 10000
) -> Dict[str, Dict[str, Dict[str, any]]]:
    """
    Тестирование использования памяти с реальными измерениями избыточности
    """
    conn = connect()
    results: Dict[str, Dict[str, Dict[str, any]]] = {}

    try:
        print(f"[INFO] Начало теста памяти с {num_rows} строками...")

        # 1. Создание и заполнение исходной таблицы
        drop_table_if_exists(conn, orig_rel.name)
        create_table(conn, orig_rel)
        insert_random_data(conn, orig_rel, num_rows)

        # Измерение без индексов
        results["Original"] = {}
        results["Original"]["without_indexes"] = get_detailed_table_metrics(conn, orig_rel.name, orig_rel.attributes)

        # Измерение избыточности
        redundancy_metrics = measure_data_redundancy(conn, orig_rel.name, orig_rel.attributes)
        results["Original"]["redundancy_metrics"] = redundancy_metrics

        # Создание индексов и повторное измерение
        create_smart_indexes_for_relation(conn, orig_rel, is_normalized=False)
        results["Original"]["with_indexes"] = get_detailed_table_metrics(conn, orig_rel.name, orig_rel.attributes)

        print(
            f"[INFO] Original - Размер без индексов: {results['Original']['without_indexes']['total_size'] / 1024:.2f} KB")
        print(
            f"[INFO] Original - Размер с индексами: {results['Original']['with_indexes']['total_size'] / 1024:.2f} KB")
        print(f"[INFO] Original - Избыточность данных: {redundancy_metrics['redundancy_ratio'] * 100:.1f}%")

        # 2. Определяем текущую нормальную форму
        analyzer = NormalFormAnalyzer(orig_rel)
        current_nf, _ = analyzer.determine_normal_form()
        print(f"[INFO] Текущая нормальная форма: {current_nf.value}")

        # 3. Декомпозиция для каждого уровня
        nf_order = [
            (NormalForm.SECOND_NF, "2NF", Decomposer.decompose_to_2nf),
            (NormalForm.THIRD_NF, "3NF", Decomposer.decompose_to_3nf),
            (NormalForm.BCNF, "BCNF", Decomposer.decompose_to_bcnf),
            (NormalForm.FOURTH_NF, "4NF", Decomposer.decompose_to_4nf)
        ]

        for target_nf, level_name, decompose_func in nf_order:
            # Пропускаем уровни ниже или равные текущему
            if target_nf.value <= current_nf.value:
                continue

            print(f"\n[INFO] Обработка {level_name}...")

            # Выполняем декомпозицию
            decomp_result = decompose_func(orig_rel)

            results[level_name] = {"without_indexes": {}, "with_indexes": {}}

            # Удаляем старые таблицы
            for rel in decomp_result.decomposed_relations:
                drop_table_if_exists(conn, rel.name)

            # Создаем и заполняем новые таблицы
            create_and_populate_normalized(conn, orig_rel, decomp_result.decomposed_relations)

            # Измерение без индексов
            total_metrics_without = {
                'table_size': 0,
                'indexes_size': 0,
                'total_size': 0,
                'row_count': 0
            }

            for rel in decomp_result.decomposed_relations:
                rel_metrics = get_detailed_table_metrics(conn, rel.name, rel.attributes)
                for key in total_metrics_without:
                    total_metrics_without[key] += rel_metrics[key]
                print(f"  - {rel.name}: {rel_metrics['total_size'] / 1024:.2f} KB ({rel_metrics['row_count']} строк)")

            results[level_name]["without_indexes"] = total_metrics_without

            # Вычисляем эффективность нормализации
            efficiency_metrics = calculate_normalization_efficiency(conn, orig_rel, decomp_result.decomposed_relations)
            results[level_name]["efficiency_metrics"] = efficiency_metrics

            # Создание индексов
            for rel in decomp_result.decomposed_relations:
                create_smart_indexes_for_relation(conn, rel, is_normalized=True)

            # Измерение с индексами
            total_metrics_with = {
                'table_size': total_metrics_without['table_size'],  # Размер таблиц не меняется
                'indexes_size': 0,
                'total_size': 0,
                'row_count': total_metrics_without['row_count']
            }

            for rel in decomp_result.decomposed_relations:
                rel_metrics = get_detailed_table_metrics(conn, rel.name, rel.attributes)
                total_metrics_with['indexes_size'] += rel_metrics['indexes_size']
                total_metrics_with['total_size'] += rel_metrics['total_size']

            results[level_name]["with_indexes"] = total_metrics_with

            print(
                f"[INFO] {level_name} - Общий размер без индексов: {total_metrics_without['total_size'] / 1024:.2f} KB")
            print(f"[INFO] {level_name} - Общий размер с индексами: {total_metrics_with['total_size'] / 1024:.2f} KB")
            print(f"[INFO] {level_name} - Устранено избыточности: {efficiency_metrics['redundancy_elimination']:.1f}%")
            print(
                f"[INFO] {level_name} - Эффективность использования пространства: {efficiency_metrics['space_efficiency']:.1f}%")

        # VACUUM для точности измерений
        print("\n[INFO] Выполнение VACUUM ANALYZE...")
        old_isolation_level = conn.isolation_level
        try:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE")
        finally:
            conn.set_isolation_level(old_isolation_level)

    except Exception as e:
        print(f"[ERROR] Ошибка в тесте памяти: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    return results


def plot_memory_usage(results: Dict[str, Dict[str, Dict[str, any]]]):
    """
    Построение графиков использования памяти с реальными данными
    """
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np

    # Определяем доступные уровни из результатов
    available_levels = []
    for level in ["Original", "2NF", "3NF", "BCNF", "4NF"]:
        if level in results:
            available_levels.append(level)

    # Извлекаем данные для графиков
    table_sizes_no_idx = []
    total_sizes_no_idx = []
    index_sizes = []
    total_sizes_with_idx = []
    redundancy_percentages = []
    efficiency_percentages = []

    for level in available_levels:
        # Размеры в МБ
        table_sizes_no_idx.append(results[level]["without_indexes"]["table_size"] / 1024 / 1024)
        total_sizes_no_idx.append(results[level]["without_indexes"]["total_size"] / 1024 / 1024)
        index_sizes.append(results[level]["with_indexes"]["indexes_size"] / 1024 / 1024)
        total_sizes_with_idx.append(results[level]["with_indexes"]["total_size"] / 1024 / 1024)

        # Метрики избыточности и эффективности
        if level == "Original":
            redundancy = results[level].get("redundancy_metrics", {}).get("redundancy_ratio", 0) * 100
            redundancy_percentages.append(redundancy)
            efficiency_percentages.append(0)  # Базовый уровень
        else:
            efficiency_metrics = results[level].get("efficiency_metrics", {})
            redundancy_percentages.append(efficiency_metrics.get("normalized_redundancy", 0))
            efficiency_percentages.append(efficiency_metrics.get("redundancy_elimination", 0))

    # Паттерны заполнения для черно-белой печати
    hatches = ['', '///', '\\\\\\', '|||', '---']

    # Создаем фигуру с 4 подграфиками
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    x = np.arange(len(available_levels))
    width = 0.35

    # График 1: Размеры без индексов
    bars1 = ax1.bar(x - width / 2, table_sizes_no_idx, width, label='Размер таблиц',
                    color='lightgray', edgecolor='black', linewidth=1.5)
    bars2 = ax1.bar(x + width / 2, total_sizes_no_idx, width, label='Общий размер',
                    color='white', edgecolor='black', linewidth=1.5)

    for i, (bar1, bar2) in enumerate(zip(bars1, bars2)):
        bar1.set_hatch(hatches[0])
        bar2.set_hatch(hatches[1])
        # Добавляем значения на столбцы
        ax1.text(bar1.get_x() + bar1.get_width() / 2., bar1.get_height(),
                 f'{bar1.get_height():.2f}', ha='center', va='bottom', fontsize=8)
        ax1.text(bar2.get_x() + bar2.get_width() / 2., bar2.get_height(),
                 f'{bar2.get_height():.2f}', ha='center', va='bottom', fontsize=8)

    ax1.set_xlabel('Уровень нормализации')
    ax1.set_ylabel('Размер (МБ)')
    ax1.set_title('Использование памяти без индексов')
    ax1.set_xticks(x)
    ax1.set_xticklabels(available_levels)
    ax1.legend()
    ax1.grid(True, axis='y', alpha=0.3)

    # График 2: Размеры с индексами (составные столбцы)
    bars1 = ax2.bar(x, table_sizes_no_idx, label='Размер таблиц',
                    color='lightgray', edgecolor='black', linewidth=1.5)
    bars2 = ax2.bar(x, index_sizes, bottom=table_sizes_no_idx, label='Размер индексов',
                    color='white', edgecolor='black', linewidth=1.5)

    for bar1, bar2 in zip(bars1, bars2):
        bar1.set_hatch(hatches[0])
        bar2.set_hatch(hatches[2])

    # Добавляем общие значения
    for i, (bar1, bar2) in enumerate(zip(bars1, bars2)):
        total_height = bar1.get_height() + bar2.get_height()
        ax2.text(bar1.get_x() + bar1.get_width() / 2., total_height,
                 f'{total_height:.2f}', ha='center', va='bottom', fontsize=8)

    ax2.set_xlabel('Уровень нормализации')
    ax2.set_ylabel('Размер (МБ)')
    ax2.set_title('Использование памяти с индексами')
    ax2.set_xticks(x)
    ax2.set_xticklabels(available_levels)
    ax2.legend()
    ax2.grid(True, axis='y', alpha=0.3)

    # График 3: Уровень избыточности данных
    bars3 = ax3.bar(x, redundancy_percentages, color='lightgray', edgecolor='black', linewidth=1.5)
    for i, bar in enumerate(bars3):
        bar.set_hatch(hatches[3])
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width() / 2., height,
                 f'{height:.1f}%', ha='center', va='bottom', fontsize=8)

    ax3.set_xlabel('Уровень нормализации')
    ax3.set_ylabel('Избыточность данных (%)')
    ax3.set_title('Уровень избыточности данных')
    ax3.set_xticks(x)
    ax3.set_xticklabels(available_levels)
    ax3.set_ylim(0, max(redundancy_percentages) * 1.2 if redundancy_percentages else 100)
    ax3.grid(True, axis='y', alpha=0.3)

    # График 4: Эффективность устранения избыточности
    bars4 = ax4.bar(x, efficiency_percentages, color='lightgray', edgecolor='black', linewidth=1.5)

    # Раскрашиваем в зависимости от эффективности
    for i, (bar, eff) in enumerate(zip(bars4, efficiency_percentages)):
        if eff > 50:
            bar.set_hatch(hatches[4])  # Высокая эффективность
        else:
            bar.set_hatch(hatches[1])  # Низкая эффективность

        # Добавляем значения
        ax4.text(bar.get_x() + bar.get_width() / 2., bar.get_height(),
                 f'{eff:.1f}%', ha='center', va='bottom', fontsize=8)

    ax4.set_xlabel('Уровень нормализации')
    ax4.set_ylabel('Устранено избыточности (%)')
    ax4.set_title('Эффективность устранения избыточности')
    ax4.set_xticks(x)
    ax4.set_xticklabels(available_levels)
    ax4.set_ylim(0, 100)
    ax4.axhline(y=50, color='red', linestyle='--', alpha=0.5, label='50% порог')
    ax4.legend()
    ax4.grid(True, axis='y', alpha=0.3)

    plt.tight_layout()
    plt.show()

    # Дополнительный график: Сравнительная эффективность
    fig2, ax5 = plt.subplots(figsize=(12, 8))

    # Подготовка данных для радар-диаграммы или сгруппированных столбцов
    metrics_names = ['Размер\n(МБ)', 'Избыточность\n(%)', 'Эффективность\n(%)', 'Индексы\n(МБ)']

    # Нормализуем данные для сравнения (приводим к шкале 0-100)
    max_size = max(total_sizes_with_idx) if total_sizes_with_idx else 1
    max_redundancy = max(redundancy_percentages) if redundancy_percentages else 1
    max_indexes = max(index_sizes) if index_sizes else 1

    bar_width = 0.15
    positions = np.arange(len(metrics_names))

    for i, level in enumerate(available_levels):
        normalized_data = [
            (total_sizes_with_idx[i] / max_size) * 100,
            redundancy_percentages[i],
            efficiency_percentages[i],
            (index_sizes[i] / max_indexes) * 100
        ]

        offset = (i - len(available_levels) / 2) * bar_width
        bars = ax5.bar(positions + offset, normalized_data, bar_width,
                       label=level, edgecolor='black', linewidth=1)

        # Применяем паттерны
        for bar in bars:
            bar.set_hatch(hatches[i % len(hatches)])
            bar.set_facecolor('lightgray' if i % 2 == 0 else 'white')

    ax5.set_xlabel('Метрики')
    ax5.set_ylabel('Нормализованные значения (%)')
    ax5.set_title('Сравнительный анализ метрик по уровням нормализации')
    ax5.set_xticks(positions)
    ax5.set_xticklabels(metrics_names)
    ax5.legend()
    ax5.grid(True, axis='y', alpha=0.3)

    plt.tight_layout()
    plt.show()

    # Вывод детальной статистики
    print("\n" + "=" * 80)
    print("ДЕТАЛЬНАЯ СТАТИСТИКА ИСПОЛЬЗОВАНИЯ ПАМЯТИ")
    print("=" * 80)

    for i, level in enumerate(available_levels):
        print(f"\n{level}:")
        print(f"  Размер таблиц: {table_sizes_no_idx[i]:.2f} МБ")
        print(f"  Размер индексов: {index_sizes[i]:.2f} МБ")
        print(f"  Общий размер: {total_sizes_with_idx[i]:.2f} МБ")
        print(f"  Избыточность данных: {redundancy_percentages[i]:.1f}%")

        if level != "Original":
            eff_metrics = results[level].get("efficiency_metrics", {})
            print(f"  Устранено избыточности: {eff_metrics.get('redundancy_elimination', 0):.1f}%")
            print(f"  Накладные расходы на ключи: {eff_metrics.get('key_overhead', 0)} дублирований")
            print(f"  Множитель строк: {eff_metrics.get('row_multiplication_factor', 1):.2f}x")