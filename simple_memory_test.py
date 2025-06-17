#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Простой тест памяти с гарантированной избыточностью
"""

import psycopg2
import random
import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, Any
from models import Relation, Attribute, FunctionalDependency, NormalForm
from data_test import connect, drop_table_if_exists, sql_type_for, count_rows
from memory_test import get_table_size_info
from decomposition import Decomposer
from analyzer import NormalFormAnalyzer


def create_configurable_redundant_data(conn, rel: Relation, num_rows: int, 
                                       num_departments: int, num_managers: int, 
                                       num_projects: int, num_employee_names: int,
                                       test_name: str):
    """
    Создает данные с настраиваемой избыточностью.
    """
    print(f"[INFO] {test_name}: Создание {num_rows} строк...")
    
    # Создаем таблицу
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
    
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    
    # Генерируем отделы
    base_departments = [
        "ИТ", "Финансы", "HR", "Маркетинг", "Продажи", "Логистика", 
        "Производство", "Закупки", "Качество", "Безопасность", "Юридический",
        "Планирование", "Аналитика", "Поддержка", "Исследования", "Развитие",
        "Консалтинг", "Обучение", "Аудит", "Контроль", "Координация",
        "Мониторинг", "Оптимизация", "Инновации", "Стратегия", "Операции",
        "Интеграция", "Автоматизация", "Цифровизация", "Трансформация"
    ]
    departments = base_departments[:num_departments]
    
    # Генерируем начальников
    base_managers = [
        "Петров И.И.", "Иванов А.А.", "Сидорова М.М.", "Козлов К.К.", 
        "Новиков Н.Н.", "Федоров Ф.Ф.", "Смирнов С.С.", "Волков В.В.",
        "Морозова М.А.", "Лебедев Л.Л.", "Орлов О.О.", "Павлов П.П.",
        "Романов Р.Р.", "Соколов С.С.", "Тимофеев Т.Т.", "Устинов У.У.",
        "Харитонов Х.Х.", "Цветков Ц.Ц.", "Чернов Ч.Ч.", "Шестаков Ш.Ш.",
        "Щукин Щ.Щ.", "Эльдаров Э.Э.", "Юдин Ю.Ю.", "Яковлев Я.Я.",
        "Абрамов А.Б.", "Богданов Б.Г.", "Викторов В.К.", "Глебов Г.Л.",
        "Денисов Д.Н.", "Емельянов Е.М."
    ]
    all_managers = base_managers[:num_managers]
    
    # Генерируем проекты
    project_letters = "АБВГДЕЖЗИКЛМНОПРСТУФХЦЧШЩЭЮЯ"
    projects = [f"Проект_{project_letters[i]}" for i in range(min(num_projects, len(project_letters)))]
    
    # Генерируем бюджеты для проектов
    project_budgets = {}
    for i, project in enumerate(projects):
        project_budgets[project] = 100000.00 + (i * 50000.00) + random.randint(-20000, 20000)
    
    # Генерируем имена сотрудников
    base_names = [
        "Александров А.А.", "Борисов Б.Б.", "Васильев В.В.", "Григорьев Г.Г.",
        "Дмитриев Д.Д.", "Егоров Е.Е.", "Жуков Ж.Ж.", "Зайцев З.З.",
        "Кузнецов К.К.", "Лебедев Л.Л.", "Морозов М.М.", "Николаев Н.Н.",
        "Орлов О.О.", "Павлов П.П.", "Романов Р.Р.", "Соколов С.С.",
        "Тимофеев Т.Т.", "Устинов У.У.", "Федоров Ф.Ф.", "Харитонов Х.Х.",
        "Цветков Ц.Ц.", "Чернов Ч.Ч.", "Шестаков Ш.Ш.", "Щукин Щ.Щ.",
        "Эльдаров Э.Э.", "Юдин Ю.Ю.", "Яковлев Я.Я.", "Абрамов А.Б.",
        "Богданов Б.Г.", "Викторов В.К.", "Глебов Г.Л.", "Денисов Д.Н.",
        "Емельянов Е.М.", "Жданов Ж.Д.", "Захаров З.Х.", "Игнатов И.Г.",
        "Калинин К.Л.", "Лазарев Л.З.", "Максимов М.К.", "Назаров Н.З.",
        "Осипов О.С.", "Панфилов П.Н.", "Рыбаков Р.Б.", "Степанов С.Т.",
        "Тарасов Т.Р.", "Ульянов У.Л.", "Филиппов Ф.Л.", "Хромов Х.Р.",
        "Царев Ц.Р.", "Черкасов Ч.Р.", "Шаров Ш.Р.", "Щеглов Щ.Г.",
        "Эйзенштейн Э.З.", "Южаков Ю.Ж.", "Ярославский Я.С.", "Аверин А.В.",
        "Белов Б.Л.", "Власов В.Л.", "Гусев Г.С.", "Данилов Д.Н.",
        "Елисеев Е.Л.", "Жигалов Ж.Г.", "Зуев З.У.", "Ильин И.Л.",
        "Киселев К.С.", "Ларионов Л.Р.", "Медведев М.Д.", "Нестеров Н.С.",
        "Овчинников О.В.", "Петухов П.Т.", "Рогов Р.Г.", "Савельев С.В.",
        "Титов Т.Т.", "Уваров У.В.", "Фролов Ф.Р.", "Холодов Х.Л.",
        "Цыганов Ц.Г.", "Чугунов Ч.Г.", "Шилов Ш.Л.", "Щербаков Щ.Р.",
        "Эдуардов Э.Д.", "Юрьев Ю.Р.", "Янковский Я.Н.", "Антонов А.Н.",
        "Бородин Б.Р.", "Виноградов В.Н.", "Горбунов Г.Р.", "Дорофеев Д.Р.",
        "Ермолов Е.Р.", "Жаров Ж.Р.", "Зотов З.Т.", "Исаев И.С.",
        "Комаров К.М.", "Леонов Л.Н.", "Миронов М.Р.", "Никитин Н.К.",
        "Олегов О.Л.", "Прохоров П.Р.", "Русаков Р.С.", "Семенов С.М.",
        "Терентьев Т.Р.", "Ушаков У.Ш.", "Фомин Ф.М.", "Храмов Х.Р.",
        "Чайков Ч.Й.", "Шумаков Ш.М.", "Щепкин Щ.П.", "Эрнст Э.Р.",
        "Юшков Ю.Ш.", "Ясенев Я.С.", "Агафонов А.Г.", "Блинов Б.Л.",
        "Воронов В.Р.", "Герасимов Г.Р.", "Добрынин Д.Б.", "Ефимов Е.Ф.",
        "Жестков Ж.С.", "Зверев З.В.", "Игоревич И.Г.", "Кондратьев К.Н.",
        "Логинов Л.Г.", "Молчанов М.Л.", "Никифоров Н.К.", "Остапов О.С.",
        "Покровский П.К.", "Родионов Р.Д.", "Сергеевич С.Г.", "Трофимов Т.Р.",
        "Ульрих У.Л.", "Филатов Ф.Л.", "Ходасевич Х.Д.", "Чистяков Ч.С.",
        "Шубин Ш.Б.", "Щукарев Щ.К.", "Эльконин Э.Л.", "Юрков Ю.Р.",
        "Ященко Я.Щ.", "Авдеев А.В.", "Быков Б.К.", "Воробьев В.Р.",
        "Гаврилов Г.В.", "Дегтярев Д.Г.", "Ерофеев Е.Р.", "Жилин Ж.Л.",
        "Зубов З.Б.", "Ильясов И.Л.", "Карпов К.Р.", "Лукин Л.К.",
        "Макаров М.К.", "Носов Н.С.", "Оленев О.Л.", "Попов П.П.",
        "Разумов Р.З.", "Сизов С.З.", "Тюрин Т.Ю.", "Угрюмов У.Г.",
        "Фадеев Ф.Д.", "Хлебников Х.Л.", "Цуканов Ц.К.", "Чудаков Ч.Д.",
        "Шведов Ш.В.", "Щавлев Щ.В.", "Эмиров Э.М.", "Юшин Ю.Ш.",
        "Ядрин Я.Д."
    ]
    employee_names = base_names[:num_employee_names]
    
    # Генерируем данные
    cols = [attr.name for attr in rel.attributes]
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO {rel.name} ({col_list}) VALUES ({placeholders});"
    
    with conn.cursor() as cur:
        for i in range(num_rows):
            values = []
            
            for attr in rel.attributes:
                if attr.name == "КодСотрудника":
                    values.append(i + 1)
                elif attr.name == "ИмяСотрудника":
                    values.append(random.choice(employee_names))
                elif attr.name == "Отдел":
                    dept = random.choice(departments)
                    values.append(dept)
                elif attr.name == "НачальникОтдела":
                    values.append(random.choice(all_managers))
                elif attr.name == "КодПроекта":
                    values.append(random.randint(1, len(projects)))
                elif attr.name == "НазваниеПроекта":
                    project_id = values[values.__len__() - 1]
                    project_name = projects[project_id - 1]
                    values.append(project_name)
                elif attr.name == "Бюджет":
                    project_name = values[values.__len__() - 1]
                    values.append(project_budgets[project_name])
                else:
                    values.append(f"Значение_{random.randint(1, 12)}")
            
            cur.execute(insert_sql, values)
    
    conn.commit()
    print(f"[INFO] {test_name}: Создано {num_rows} строк")
    
    # Показываем статистику избыточности
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(DISTINCT \"Отдел\") FROM {rel.name}")
        unique_depts = cur.fetchone()[0]
        
        cur.execute(f"SELECT COUNT(DISTINCT \"НазваниеПроекта\") FROM {rel.name}")
        unique_projects = cur.fetchone()[0]
        
        cur.execute(f"SELECT COUNT(DISTINCT \"НачальникОтдела\") FROM {rel.name}")
        unique_managers = cur.fetchone()[0]
        
        cur.execute(f"SELECT COUNT(DISTINCT \"ИмяСотрудника\") FROM {rel.name}")
        unique_names = cur.fetchone()[0]
        
        redundancy_coeff = num_rows / (unique_depts * unique_projects) if (unique_depts * unique_projects) > 0 else 0
        
        print(f"[СТАТИСТИКА] {test_name}:")
        print(f"  • Уникальных отделов: {unique_depts}")
        print(f"  • Уникальных проектов: {unique_projects}")
        print(f"  • Уникальных начальников: {unique_managers}")
        print(f"  • Уникальных имен: {unique_names}")
        print(f"  • Коэффициент избыточности: {redundancy_coeff:.1f}x")
        
        return redundancy_coeff


def calculate_memory_efficiency(results: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """
    Вычисляет метрики эффективности использования памяти:
    1. Плотность данных (байт на строку)
    2. Эффективность нормализации (экономия памяти)
    3. Коэффициент сжатия (отношение размеров)
    """
    metrics = {}
    original_size = results["Original"]["total_size"]
    original_rows = results["Original"]["row_count"]
    
    for level, data in results.items():
        size = data["total_size"]
        rows = data["row_count"]
        tables = data["table_count"]
        
        # Плотность данных (байт на строку)
        density = size / rows if rows > 0 else 0
        
        # Эффективность нормализации (% экономии памяти)
        efficiency = ((original_size - size) / original_size * 100) if original_size > 0 else 0
        
        # Коэффициент сжатия
        compression_ratio = original_size / size if size > 0 else 0
        
        # Накладные расходы на таблицы (приблизительно 8КБ на таблицу в PostgreSQL)
        table_overhead = tables * 8192  # 8KB на таблицу
        data_without_overhead = size - table_overhead
        net_efficiency = ((original_size - data_without_overhead) / original_size * 100) if original_size > 0 else 0
        
        metrics[level] = {
            'density': density,
            'efficiency': efficiency,
            'compression_ratio': compression_ratio,
            'net_efficiency': net_efficiency,
            'table_overhead': table_overhead
        }
    
    return metrics


def plot_memory_efficiency_enhanced(results: Dict[str, Dict[str, Any]], metrics: Dict[str, float], test_name: str, redundancy_coeff: float):
    """
    Построение улучшенных графиков для черно-белой печати с увеличенными размерами и лучшим размещением текста.
    """
    levels = [level for level in ["Original", "2NF", "3NF", "BCNF", "4NF"] if level in results]
    if not levels:
        print("Нет данных для построения графиков.")
        return

    # Подготовка данных (в мегабайтах)
    sizes_mb = [results[level]["total_size"] / (1024 * 1024) for level in levels]
    densities = [metrics[level]["density"] for level in levels]
    efficiencies = [metrics[level]["efficiency"] for level in levels]
    
    # Определяем паттерны штриховки для черно-белой печати
    hatches = ['', '///', '\\\\\\', '|||', '---', '+++', 'xxx', '...']
    
    # Создание фигуры с увеличенными размерами для лучшей читаемости
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(24, 10))

    # --- График 1: Размер данных ---
    x = np.arange(len(levels))
    bars1 = []
    for i, size in enumerate(sizes_mb):
        bar = ax1.bar(i, size, color='white', edgecolor='black', linewidth=2,
                     hatch=hatches[i % len(hatches)])
        bars1.append(bar)
    
    # Добавляем линию тренда
    ax1.plot(range(len(levels)), sizes_mb, 'ko-', linewidth=4, markersize=10)
    
    ax1.set_ylabel('Размер (МБ)', fontsize=18, fontweight='bold')
    ax1.set_title('Размер базы данных', fontsize=20, fontweight='bold', pad=40)
    ax1.set_xticks(range(len(levels)))
    ax1.set_xticklabels(levels, rotation=0, fontsize=16, fontweight='bold')
    ax1.grid(True, axis='y', linestyle='--', alpha=0.7)
    ax1.tick_params(axis='y', labelsize=14)

    # Добавляем значения на столбцы с улучшенным размещением
    for i, (bar, size) in enumerate(zip(bars1, sizes_mb)):
        height = bar[0].get_height()
        # Размещаем текст выше столбца с большим отступом
        ax1.text(i, height + max(sizes_mb) * 0.05, f'{size:.3f}', 
                ha='center', va='bottom', fontsize=15, fontweight='bold')

    # --- График 2: Плотность данных ---
    bars2 = []
    for i, density in enumerate(densities):
        bar = ax2.bar(i, density, color='white', edgecolor='black', linewidth=2,
                     hatch=hatches[i % len(hatches)])
        bars2.append(bar)
        
    ax2.set_ylabel('Байт на строку', fontsize=18, fontweight='bold')
    ax2.set_title('Плотность данных', fontsize=20, fontweight='bold', pad=40)
    ax2.set_xticks(range(len(levels)))
    ax2.set_xticklabels(levels, rotation=0, fontsize=16, fontweight='bold')
    ax2.grid(True, axis='y', linestyle='--', alpha=0.7)
    ax2.tick_params(axis='y', labelsize=14)

    for i, (bar, density) in enumerate(zip(bars2, densities)):
        height = bar[0].get_height()
        # Размещаем текст выше столбца с большим отступом
        ax2.text(i, height + max(densities) * 0.08, f'{density:.0f}', 
                ha='center', va='bottom', fontsize=15, fontweight='bold')

    # --- График 3: Эффективность нормализации ---
    bars3 = []
    for i, eff in enumerate(efficiencies):
        # Используем разную штриховку для каждого уровня
        bar = ax3.bar(i, eff, color='white', edgecolor='black', linewidth=2,
                     hatch=hatches[i % len(hatches)])
        bars3.append(bar)
        
    ax3.set_ylabel('Экономия памяти (%)', fontsize=18, fontweight='bold')
    ax3.set_title('Эффективность нормализации', fontsize=20, fontweight='bold', pad=40)
    ax3.set_xticks(range(len(levels)))
    ax3.set_xticklabels(levels, rotation=0, fontsize=16, fontweight='bold')
    ax3.grid(True, axis='y', linestyle='--', alpha=0.7)
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.8, linewidth=3)
    ax3.tick_params(axis='y', labelsize=14)

    for i, (bar, eff) in enumerate(zip(bars3, efficiencies)):
        height = bar[0].get_height()
        # Улучшенное размещение текста с большими отступами
        if height >= 0:
            y_pos = height + max(max(efficiencies), 0) * 0.08
            va = 'bottom'
        else:
            y_pos = height - abs(min(min(efficiencies), 0)) * 0.08
            va = 'top'
        
        ax3.text(i, y_pos, f'{eff:.1f}%', ha='center', va=va, 
                fontsize=15, fontweight='bold')

    # Улучшенное размещение графиков с увеличенными отступами сверху для заголовка
    plt.tight_layout(rect=[0, 0.15, 1, 0.88])
    
    # Добавляем информацию о тесте и статистику
    original_size_mb = sizes_mb[0]
    final_size_mb = sizes_mb[-1] if sizes_mb else 0
    total_savings = efficiencies[-1] if efficiencies else 0
    best_efficiency = max(efficiencies) if efficiencies else 0
    
    # Заголовок теста - размещаем выше с большим отступом
    fig.text(0.5, 0.94, f'{test_name.upper()}', ha='center', fontsize=24, fontweight='bold')
    
    # Статистика избыточности
    fig.text(0.5, 0.10, 
             f'Коэффициент избыточности: {redundancy_coeff:.1f}x | '
             f'Лучшая эффективность: {best_efficiency:.1f}% | '
             f'Финальная экономия: {total_savings:.1f}%',
             ha='center', fontsize=16, fontweight='bold',
             bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.9))
    
    # Размеры базы данных
    fig.text(0.5, 0.04, 
             f'Размер базы данных: {original_size_mb:.3f} → {final_size_mb:.3f} МБ',
             ha='center', fontsize=14, fontweight='bold')
    
    plt.show()


def run_comparative_memory_tests():
    """Запуск сравнительных тестов памяти с высокой и низкой избыточностью"""
    
    print("="*100)
    print("СРАВНИТЕЛЬНЫЙ ТЕСТ ЭФФЕКТИВНОСТИ НОРМАЛИЗАЦИИ")
    print("="*100)
    
    # Создаем отношение
    attrs = [
        Attribute('КодСотрудника', 'INTEGER', True),
        Attribute('ИмяСотрудника', 'VARCHAR', False),
        Attribute('Отдел', 'VARCHAR', False),
        Attribute('НачальникОтдела', 'VARCHAR', False),
        Attribute('КодПроекта', 'INTEGER', True),
        Attribute('НазваниеПроекта', 'VARCHAR', False),
        Attribute('Бюджет', 'DECIMAL', False)
    ]
    
    fds = [
        FunctionalDependency({attrs[0]}, {attrs[1], attrs[2]}),
        FunctionalDependency({attrs[2]}, {attrs[3]}),
        FunctionalDependency({attrs[4]}, {attrs[5], attrs[6]})
    ]
    
    relation = Relation('СравнительныйТест', attrs, fds)
    num_rows = 3000
    
    # ТЕСТ 1: ВЫСОКАЯ ИЗБЫТОЧНОСТЬ
    print(f"\n🔴 ТЕСТ 1: ВЫСОКАЯ ИЗБЫТОЧНОСТЬ")
    print("-" * 60)
    
    conn = connect()
    try:
        # Создаем данные с высокой избыточностью - 200 имен сотрудников
        redundancy_high = create_configurable_redundant_data(
            conn, relation, num_rows, 
            num_departments=5, num_managers=5, 
            num_projects=10, num_employee_names=200,
            test_name="ВЫСОКАЯ ИЗБЫТОЧНОСТЬ"
        )
        
        # Тестируем нормализацию
        results_high = run_normalization_test(conn, relation, "ВЫСОКАЯ ИЗБЫТОЧНОСТЬ")
        metrics_high = calculate_memory_efficiency(results_high)
        
        # Показываем результаты
        show_results_table(results_high, metrics_high, "ВЫСОКАЯ ИЗБЫТОЧНОСТЬ")
        
    finally:
        conn.close()
    
    # Строим графики для высокой избыточности
    plot_memory_efficiency_enhanced(results_high, metrics_high, "ВЫСОКАЯ ИЗБЫТОЧНОСТЬ", redundancy_high)
    
    # ТЕСТ 2: НИЗКАЯ ИЗБЫТОЧНОСТЬ
    print(f"\n🔵 ТЕСТ 2: НИЗКАЯ ИЗБЫТОЧНОСТЬ")
    print("-" * 60)
    
    conn = connect()
    try:
        # Создаем данные с низкой избыточностью - 500 имен сотрудников
        redundancy_low = create_configurable_redundant_data(
            conn, relation, num_rows,
            num_departments=30, num_managers=30,
            num_projects=50, num_employee_names=500,
            test_name="НИЗКАЯ ИЗБЫТОЧНОСТЬ"
        )
        
        # Тестируем нормализацию
        results_low = run_normalization_test(conn, relation, "НИЗКАЯ ИЗБЫТОЧНОСТЬ")
        metrics_low = calculate_memory_efficiency(results_low)
        
        # Показываем результаты
        show_results_table(results_low, metrics_low, "НИЗКАЯ ИЗБЫТОЧНОСТЬ")
        
    finally:
        conn.close()
    
    # Строим графики для низкой избыточности
    plot_memory_efficiency_enhanced(results_low, metrics_low, "НИЗКАЯ ИЗБЫТОЧНОСТЬ", redundancy_low)
    
    # СРАВНИТЕЛЬНЫЙ АНАЛИЗ
    print(f"\n📊 СРАВНИТЕЛЬНЫЙ АНАЛИЗ")
    print("=" * 80)
    
    best_high = max(metrics_high.keys(), key=lambda k: metrics_high[k]["efficiency"] if k != "Original" else -999)
    best_low = max(metrics_low.keys(), key=lambda k: metrics_low[k]["efficiency"] if k != "Original" else -999)
    
    print(f"ВЫСОКАЯ ИЗБЫТОЧНОСТЬ ({redundancy_high:.1f}x):")
    print(f"  • Лучший результат: {best_high} с эффективностью {metrics_high[best_high]['efficiency']:.1f}%")
    print(f"  • Размер: {results_high['Original']['total_size']/1024:.0f} → {results_high[best_high]['total_size']/1024:.0f} КБ")
    
    print(f"\nНИЗКАЯ ИЗБЫТОЧНОСТЬ ({redundancy_low:.1f}x):")
    print(f"  • Лучший результат: {best_low} с эффективностью {metrics_low[best_low]['efficiency']:.1f}%")
    print(f"  • Размер: {results_low['Original']['total_size']/1024:.0f} → {results_low[best_low]['total_size']/1024:.0f} КБ")
    
    print(f"\n🎯 ВЫВОД:")
    if metrics_high[best_high]['efficiency'] > metrics_low[best_low]['efficiency']:
        print(f"   Высокая избыточность показывает лучшие результаты нормализации")
        print(f"   Разница в эффективности: {metrics_high[best_high]['efficiency'] - metrics_low[best_low]['efficiency']:.1f}%")
    else:
        print(f"   Низкая избыточность показывает лучшие результаты нормализации")
        print(f"   Разница в эффективности: {metrics_low[best_low]['efficiency'] - metrics_high[best_high]['efficiency']:.1f}%")


def run_normalization_test(conn, relation: Relation, test_name: str):
    """Выполняет тест нормализации и возвращает результаты"""
    results = {}
    
    # Исходное отношение
    results["Original"] = get_table_size_info(conn, relation.name)
    results["Original"]["table_count"] = 1
    print(f"  Исходный размер: {results['Original']['total_size'] / 1024:.2f} КБ | "
          f"Строк: {results['Original']['row_count']}")
    
    # Определяем нормальную форму
    analyzer = NormalFormAnalyzer(relation)
    current_nf, _ = analyzer.determine_normal_form()
    print(f"  Текущая нормальная форма: {current_nf.value}")
    
    # Нормализация до всех форм
    nf_tests = [
        ("2NF", Decomposer.decompose_to_2nf),
        ("3NF", Decomposer.decompose_to_3nf),
        ("BCNF", Decomposer.decompose_to_bcnf),
        ("4NF", Decomposer.decompose_to_4nf)
    ]
    
    for nf_name, decompose_func in nf_tests:
        print(f"  Нормализация до {nf_name}...", end=" ")
        decomp_result = decompose_func(relation)
        normalized_rels = decomp_result.decomposed_relations
        
        # Создаем нормализованные таблицы
        create_normalized_tables_manually(conn, relation, normalized_rels, nf_name)
        
        # Считаем размеры
        total_size = 0
        total_table_size = 0
        total_index_size = 0
        total_rows = 0
        
        for rel in normalized_rels:
            size_info = get_table_size_info(conn, rel.name)
            total_size += size_info['total_size']
            total_table_size += size_info['table_size']
            total_index_size += size_info['indexes_size']
            total_rows += size_info['row_count']
        
        results[nf_name] = {
            'total_size': total_size,
            'table_size': total_table_size,
            'indexes_size': total_index_size,
            'row_count': total_rows,
            'table_count': len(normalized_rels)
        }
        
        efficiency = ((results["Original"]["total_size"] - total_size) / results["Original"]["total_size"] * 100) if results["Original"]["total_size"] > 0 else 0
        print(f"{total_size / 1024:.0f} КБ ({efficiency:+.1f}%)")
    
    return results


def show_results_table(results: Dict[str, Dict[str, Any]], metrics: Dict[str, float], test_name: str):
    """Показывает таблицу результатов"""
    print(f"\n📋 РЕЗУЛЬТАТЫ: {test_name}")
    print("-" * 80)
    print(f"{'Уровень':<10} {'Размер (КБ)':<12} {'Плотность':<12} {'Эффективность':<15} {'Таблиц':<8}")
    print("-" * 80)
    
    for level in ["Original", "2NF", "3NF", "BCNF", "4NF"]:
        if level in results:
            size_kb = results[level]["total_size"] / 1024
            density = metrics[level]["density"]
            efficiency = metrics[level]["efficiency"]
            tables = results[level]["table_count"]
            
            print(f"{level:<10} {size_kb:<12.0f} {density:<12.0f} {efficiency:<15.1f}% {tables:<8}")
    
    # Лучший результат
    best_level = max(results.keys(), key=lambda k: metrics[k]["efficiency"] if k != "Original" else -999)
    best_efficiency = metrics[best_level]["efficiency"]
    print(f"\n🏆 Лучший результат: {best_level} с эффективностью {best_efficiency:.1f}%")


def create_normalized_tables_manually(conn, orig_rel: Relation, normalized_rels, nf_suffix: str):
    """Создает нормализованные таблицы вручную с правильными данными"""
    
    # Определяем типы таблиц по атрибутам
    dept_table_name = None
    employee_table_name = None
    project_table_name = None
    relationship_table_name = None
    
    for rel in normalized_rels:
        attr_names = [attr.name for attr in rel.attributes]
        if "Отдел" in attr_names and "НачальникОтдела" in attr_names and len(attr_names) == 2:
            dept_table_name = rel.name
        elif "КодСотрудника" in attr_names and "ИмяСотрудника" in attr_names:
            employee_table_name = rel.name
        elif "КодПроекта" in attr_names and "НазваниеПроекта" in attr_names:
            project_table_name = rel.name
        elif "КодСотрудника" in attr_names and "КодПроекта" in attr_names and len(attr_names) == 2:
            relationship_table_name = rel.name
    
    # Создаем таблицы
    for rel in normalized_rels:
        drop_table_if_exists(conn, rel.name)
        columns_sql = []
        for attr in rel.attributes:
            col_def = f"{attr.name} {sql_type_for(attr)}"
            columns_sql.append(col_def)
        
        create_sql = f"CREATE TABLE {rel.name} (\n    " + ",\n    ".join(columns_sql) + "\n);"
        
        with conn.cursor() as cur:
            cur.execute(create_sql)
    
    conn.commit()
    
    # Заполняем таблицы
    with conn.cursor() as cur:
        # 1. Таблица отделов
        if dept_table_name:
            cur.execute(f"""
                INSERT INTO {dept_table_name} ("Отдел", "НачальникОтдела")
                SELECT DISTINCT "Отдел", "НачальникОтдела" 
                FROM {orig_rel.name}
            """)
        
        # 2. Таблица сотрудников
        if employee_table_name:
            cur.execute(f"""
                INSERT INTO {employee_table_name} ("КодСотрудника", "ИмяСотрудника", "Отдел")
                SELECT DISTINCT "КодСотрудника", "ИмяСотрудника", "Отдел"
                FROM {orig_rel.name}
            """)
        
        # 3. Таблица проектов
        if project_table_name:
            cur.execute(f"""
                INSERT INTO {project_table_name} ("КодПроекта", "НазваниеПроекта", "Бюджет")
                SELECT DISTINCT "КодПроекта", "НазваниеПроекта", "Бюджет"
                FROM {orig_rel.name}
            """)
        
        # 4. Таблица связей
        if relationship_table_name:
            cur.execute(f"""
                INSERT INTO {relationship_table_name} ("КодСотрудника", "КодПроекта")
                SELECT DISTINCT "КодСотрудника", "КодПроекта"
                FROM {orig_rel.name}
            """)
    
    conn.commit()


if __name__ == "__main__":
    run_comparative_memory_tests() 