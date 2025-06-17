import psycopg2
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import numpy as np

from models import Relation, NormalForm
from data_test import connect, drop_table_if_exists, create_table, insert_random_data, create_and_populate_normalized, \
    count_rows
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


def run_memory_test(
        orig_rel: Relation,
        num_rows: int = 10000
) -> Dict[str, Dict[str, any]]:
    """
    Тестирование использования памяти.
    """
    conn = connect()
    results: Dict[str, Dict[str, any]] = {}

    try:
        print(f"\n{'=' * 50}\n[INFO] Начало теста памяти с {num_rows} строками...\n{'=' * 50}")

        # 1. ИСХОДНОЕ ОТНОШЕНИЕ
        print("\n--- Уровень: Original ---")
        drop_table_if_exists(conn, orig_rel.name)
        create_table(conn, orig_rel)
        insert_random_data(conn, orig_rel, num_rows)
        create_realistic_indexes(conn, orig_rel)  # Создаем индексы и PK

        results["Original"] = get_table_size_info(conn, orig_rel.name)
        results["Original"]["table_count"] = 1
        print(f"  Размер: {results['Original']['total_size'] / 1024:.2f} KB | "
              f"Строк: {results['Original']['row_count']}")

        # 2. ОПРЕДЕЛЕНИЕ НОРМАЛЬНОЙ ФОРМЫ
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
                continue

            print(f"\n--- Уровень: {level_name} ---")
            decomp_result = decompose_func(orig_rel)
            decomposed_rels = decomp_result.decomposed_relations

            # Создаем и заполняем таблицы
            create_and_populate_normalized(conn, orig_rel, decomposed_rels)

            level_total_size = 0
            level_table_size = 0
            level_indexes_size = 0
            level_row_count = 0

            # Создаем индексы для всех таблиц
            for rel in decomposed_rels:
                create_realistic_indexes(conn, rel)
                size_info = get_table_size_info(conn, rel.name)

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

    except Exception as e:
        print(f"[ERROR] Ошибка в тесте памяти: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    return results


def plot_memory_usage(results: Dict[str, Dict[str, any]]):
    """
    Построение графиков для черно-белой печати с штриховкой вместо цветов.
    """
    levels = [level for level in ["Original", "2NF", "3NF", "BCNF", "4NF"] if level in results]
    if not levels:
        print("❌ Нет данных для построения графиков.")
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
    fig.suptitle('АНАЛИЗ ЭФФЕКТИВНОСТИ НОРМАЛИЗАЦИИ БАЗЫ ДАННЫХ', 
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
    print(f"\n📋 ТЕКСТОВАЯ СВОДКА:")
    print(f"   • Исходный размер: {original_size_mb:.2f} МБ")
    print(f"   • Финальный размер: {final_size_mb:.2f} МБ")
    print(f"   • Экономия памяти: {total_savings:.1f}%")
    print(f"   • Количество таблиц: {table_counts[0]} → {table_counts[-1]}")
    print(f"   • Количество строк: {original_rows:,} → {row_counts[-1]:,}")
    
    if total_savings > 0:
        print(f"   ✅ Нормализация ЭФФЕКТИВНА - экономия {total_savings:.1f}% памяти")
    else:
        print(f"   ⚠️  Нормализация увеличила размер на {abs(total_savings):.1f}%")
        print(f"       (возможно, нужно больше избыточных данных для демонстрации эффекта)")