import psycopg2
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from models import Relation, NormalForm
from data_test import connect, drop_table_if_exists, create_table, insert_random_data, create_and_populate_normalized, \
    count_rows
from decomposition import Decomposer
from analyzer import NormalFormAnalyzer


def get_table_size_info(conn, table_name: str) -> Dict[str, int]:
    """
    Получить информацию о размере таблицы в PostgreSQL
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


def create_indexes_for_relation(conn, relation: Relation):
    """
    Создать индексы ТОЛЬКО для первичных и внешних ключей
    """
    analyzer = NormalFormAnalyzer(relation)

    with conn.cursor() as cur:
        # 1. Индекс для первичного ключа (если составной и не создан автоматически)
        if analyzer.candidate_keys:
            # Используем первый кандидатный ключ как первичный
            pk_attrs = [attr.name for attr in list(analyzer.candidate_keys)[0]]
            if len(pk_attrs) > 1:  # Составной ключ
                index_name = f"idx_{relation.name}_pk"
                pk_cols = ", ".join(pk_attrs)
                try:
                    cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {relation.name} ({pk_cols})")
                except Exception as e:
                    print(f"[INFO] Индекс PK уже существует или ошибка: {e}")

        # 2. Индексы для внешних ключей
        # Определяем какие атрибуты могут быть внешними ключами
        # (атрибуты, которые есть в других таблицах и не являются частью PK этой таблицы)
        all_attrs = set(attr.name for attr in relation.attributes)
        pk_attr_names = set(
            attr.name for attr in list(analyzer.candidate_keys)[0]) if analyzer.candidate_keys else set()

        potential_fk = all_attrs - pk_attr_names

        for i, attr_name in enumerate(potential_fk):
            index_name = f"idx_{relation.name}_fk_{attr_name}"
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {relation.name} ({attr_name})")
            except Exception as e:
                print(f"[INFO] Не удалось создать индекс FK: {e}")

        conn.commit()


def calculate_data_duplication_stats(conn, orig_rel: Relation, decomposed_rels: List[Relation]) -> Dict[str, float]:
    """
    Вычислить реальную статистику дублирования данных
    """
    stats = {
        'schema_duplication': 0,  # Дублирование атрибутов в схеме
        'data_duplication': 0,  # Реальное дублирование данных
        'space_efficiency': 0,  # Эффективность использования пространства
        'join_complexity': 0  # Сложность восстановления данных
    }

    # 1. Дублирование схемы (атрибутов)
    original_attrs = len(orig_rel.attributes)
    total_decomposed_attrs = sum(len(rel.attributes) for rel in decomposed_rels)
    stats['schema_duplication'] = total_decomposed_attrs / original_attrs if original_attrs > 0 else 1.0

    # 2. Подсчет реального дублирования данных
    # Считаем, сколько раз каждый атрибут встречается в разных таблицах
    attr_occurrences = {}
    for rel in decomposed_rels:
        for attr in rel.attributes:
            attr_occurrences[attr.name] = attr_occurrences.get(attr.name, 0) + 1

    # Атрибуты, которые дублируются
    duplicated_attrs = {k: v for k, v in attr_occurrences.items() if v > 1}

    # 3. Оценка дублирования данных
    # Для каждого дублированного атрибута считаем объем дублированных данных
    total_duplicated_cells = 0
    total_cells = 0

    with conn.cursor() as cur:
        # Получаем количество уникальных значений для дублированных атрибутов
        for attr_name, occurrences in duplicated_attrs.items():
            # Находим таблицы, содержащие этот атрибут
            tables_with_attr = []
            for rel in decomposed_rels:
                if any(a.name == attr_name for a in rel.attributes):
                    tables_with_attr.append(rel.name)

            if len(tables_with_attr) > 1:
                # Считаем уникальные значения в первой таблице
                try:
                    cur.execute(f"SELECT COUNT(DISTINCT {attr_name}) FROM {tables_with_attr[0]}")
                    unique_values = cur.fetchone()[0]

                    # Считаем общее количество значений во всех таблицах
                    total_values = 0
                    for table in tables_with_attr:
                        cur.execute(f"SELECT COUNT({attr_name}) FROM {table}")
                        total_values += cur.fetchone()[0]

                    # Дублирование = (общее количество - уникальные значения)
                    duplicated_values = total_values - unique_values
                    total_duplicated_cells += duplicated_values
                    total_cells += total_values
                except Exception as e:
                    print(f"[WARNING] Ошибка при подсчете дублирования для {attr_name}: {e}")

    # Процент дублированных данных
    stats['data_duplication'] = (total_duplicated_cells / total_cells * 100) if total_cells > 0 else 0

    # 4. Сложность JOIN (количество таблиц для полного восстановления)
    stats['join_complexity'] = len(decomposed_rels)

    # 5. Эффективность пространства (обратная величина от дублирования)
    stats['space_efficiency'] = 100 - stats['data_duplication']

    return stats


def run_memory_test(
        orig_rel: Relation,
        num_rows: int = 10000
) -> Dict[str, Dict[str, any]]:
    """
    Тестирование использования памяти с улучшенными метриками
    """
    conn = connect()
    results: Dict[str, Dict[str, any]] = {}

    try:
        print(f"[INFO] Начало теста памяти с {num_rows} строками...")

        # 1. Создание и заполнение исходной таблицы
        drop_table_if_exists(conn, orig_rel.name)
        create_table(conn, orig_rel)
        insert_random_data(conn, orig_rel, num_rows)

        # Измерение без индексов
        results["Original"] = {}
        results["Original"]["without_indexes"] = get_table_size_info(conn, orig_rel.name)

        # Создание индексов и повторное измерение
        create_indexes_for_relation(conn, orig_rel)
        results["Original"]["with_indexes"] = get_table_size_info(conn, orig_rel.name)

        # Статистика дублирования для исходной таблицы
        results["Original"]["duplication_stats"] = {
            'schema_duplication': 1.0,
            'data_duplication': 0,
            'space_efficiency': 100,
            'join_complexity': 1
        }

        print(f"[INFO] Original - Размер: {results['Original']['with_indexes']['total_size'] / 1024:.2f} KB")

        # 2. Определяем текущую нормальную форму
        analyzer = NormalFormAnalyzer(orig_rel)
        current_nf, _ = analyzer.determine_normal_form()
        print(f"[INFO] Текущая нормальная форма: {current_nf.value}")

        # 3. Декомпозиция для разных уровней
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
            total_size_without = 0
            total_table_size = 0
            total_rows = 0

            for rel in decomp_result.decomposed_relations:
                size_info = get_table_size_info(conn, rel.name)
                total_size_without += size_info['total_size']
                total_table_size += size_info['table_size']
                total_rows += size_info['row_count']
                print(f"  - {rel.name}: {size_info['total_size'] / 1024:.2f} KB ({size_info['row_count']} строк)")

            results[level_name]["without_indexes"] = {
                'table_size': total_table_size,
                'indexes_size': 0,
                'total_size': total_size_without,
                'row_count': total_rows
            }

            # Создание индексов
            for rel in decomp_result.decomposed_relations:
                create_indexes_for_relation(conn, rel)

            # Измерение с индексами
            total_size_with = 0
            total_indexes_size = 0

            for rel in decomp_result.decomposed_relations:
                size_info = get_table_size_info(conn, rel.name)
                total_size_with += size_info['total_size']
                total_indexes_size += size_info['indexes_size']

            results[level_name]["with_indexes"] = {
                'table_size': total_table_size,
                'indexes_size': total_indexes_size,
                'total_size': total_size_with,
                'row_count': total_rows
            }

            # Вычисляем статистику дублирования
            duplication_stats = calculate_data_duplication_stats(
                conn, orig_rel, decomp_result.decomposed_relations
            )
            results[level_name]["duplication_stats"] = duplication_stats

            print(f"[INFO] {level_name} - Размер: {total_size_with / 1024:.2f} KB")
            print(f"[INFO] {level_name} - Дублирование данных: {duplication_stats['data_duplication']:.1f}%")
            print(f"[INFO] {level_name} - Сложность JOIN: {duplication_stats['join_complexity']} таблиц")

    except Exception as e:
        print(f"[ERROR] Ошибка в тесте памяти: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    return results


def plot_memory_usage(results: Dict[str, Dict[str, any]]):
    """
    Построение улучшенных графиков использования памяти
    """
    available_levels = [level for level in ["Original", "2NF", "3NF", "BCNF", "4NF"] if level in results]

    # Подготовка данных
    table_sizes = []
    index_sizes = []
    total_sizes = []
    data_duplication = []
    space_efficiency = []
    join_complexity = []

    for level in available_levels:
        table_sizes.append(results[level]["with_indexes"]["table_size"] / 1024 / 1024)
        index_sizes.append(results[level]["with_indexes"]["indexes_size"] / 1024 / 1024)
        total_sizes.append(results[level]["with_indexes"]["total_size"] / 1024 / 1024)

        stats = results[level]["duplication_stats"]
        data_duplication.append(stats["data_duplication"])
        space_efficiency.append(stats["space_efficiency"])
        join_complexity.append(stats["join_complexity"])

    # Создание фигуры с подграфиками
    fig = plt.figure(figsize=(16, 10))

    # График 1: Размеры таблиц и индексов
    ax1 = plt.subplot(2, 3, 1)
    x = np.arange(len(available_levels))
    width = 0.6

    # Составная гистограмма
    p1 = ax1.bar(x, table_sizes, width, label='Данные', color='lightblue', edgecolor='black')
    p2 = ax1.bar(x, index_sizes, width, bottom=table_sizes, label='Индексы', color='lightcoral', edgecolor='black')

    ax1.set_xlabel('Уровень нормализации')
    ax1.set_ylabel('Размер (МБ)')
    ax1.set_title('Использование памяти')
    ax1.set_xticks(x)
    ax1.set_xticklabels(available_levels)
    ax1.legend()
    ax1.grid(True, axis='y', alpha=0.3)

    # Добавляем значения на столбцы
    for i, (t_size, i_size) in enumerate(zip(table_sizes, index_sizes)):
        height = t_size + i_size
        ax1.text(i, height, f'{height:.2f}', ha='center', va='bottom')

    # График 2: Дублирование данных
    ax2 = plt.subplot(2, 3, 2)
    bars = ax2.bar(available_levels, data_duplication, color='orange', edgecolor='black')
    ax2.set_xlabel('Уровень нормализации')
    ax2.set_ylabel('Дублирование данных (%)')
    ax2.set_title('Процент дублированных данных')
    ax2.grid(True, axis='y', alpha=0.3)

    # Добавляем значения
    for bar, val in zip(bars, data_duplication):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                 f'{val:.1f}%', ha='center', va='bottom')

    # График 3: Эффективность использования пространства
    ax3 = plt.subplot(2, 3, 3)
    bars = ax3.bar(available_levels, space_efficiency, color='green', edgecolor='black')
    ax3.set_xlabel('Уровень нормализации')
    ax3.set_ylabel('Эффективность (%)')
    ax3.set_title('Эффективность использования пространства')
    ax3.axhline(y=100, color='red', linestyle='--', alpha=0.5)
    ax3.grid(True, axis='y', alpha=0.3)
    ax3.set_ylim(0, 105)

    # График 4: Сложность JOIN
    ax4 = plt.subplot(2, 3, 4)
    bars = ax4.bar(available_levels, join_complexity, color='purple', edgecolor='black')
    ax4.set_xlabel('Уровень нормализации')
    ax4.set_ylabel('Количество таблиц')
    ax4.set_title('Сложность восстановления данных (JOIN)')
    ax4.grid(True, axis='y', alpha=0.3)

    for bar, val in zip(bars, join_complexity):
        ax4.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                 f'{int(val)}', ha='center', va='bottom')

    # График 5: Сравнение с исходным размером
    ax5 = plt.subplot(2, 3, 5)
    original_size = total_sizes[0]
    size_ratio = [(size / original_size * 100) for size in total_sizes]

    bars = ax5.bar(available_levels, size_ratio, color='teal', edgecolor='black')
    ax5.axhline(y=100, color='red', linestyle='--', alpha=0.5, label='Исходный размер')
    ax5.set_xlabel('Уровень нормализации')
    ax5.set_ylabel('Размер относительно исходного (%)')
    ax5.set_title('Изменение общего размера БД')
    ax5.legend()
    ax5.grid(True, axis='y', alpha=0.3)

    for bar, val in zip(bars, size_ratio):
        ax5.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                 f'{val:.1f}%', ha='center', va='bottom')

    # График 6: Сводная таблица
    ax6 = plt.subplot(2, 3, 6)
    ax6.axis('tight')
    ax6.axis('off')

    # Создаем таблицу с ключевыми метриками
    table_data = []
    for i, level in enumerate(available_levels):
        row = [
            level,
            f"{total_sizes[i]:.2f} МБ",
            f"{data_duplication[i]:.1f}%",
            f"{space_efficiency[i]:.1f}%",
            f"{int(join_complexity[i])}"
        ]
        table_data.append(row)

    table = ax6.table(cellText=table_data,
                      colLabels=['Уровень', 'Размер', 'Дубл.', 'Эфф.', 'JOIN'],
                      cellLoc='center',
                      loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    ax6.set_title('Сводная таблица метрик', pad=20)

    plt.tight_layout()
    plt.show()