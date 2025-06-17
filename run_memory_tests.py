#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для запуска тестов памяти из командной строки
"""

import sys
from typing import List
from models import Relation, Attribute, FunctionalDependency
from memory_test import run_memory_test, plot_memory_usage


def create_example_relations() -> List[tuple]:
    """Создание примеров отношений для тестирования"""
    
    examples = []
    
    # Пример 1: Сотрудники-Проекты (классический пример)
    print("📋 Создание примера 1: Сотрудники-Проекты")
    attrs1 = [
        Attribute('КодСотрудника', 'INTEGER', True),
        Attribute('ИмяСотрудника', 'VARCHAR', False),
        Attribute('Отдел', 'VARCHAR', False),
        Attribute('НачальникОтдела', 'VARCHAR', False),
        Attribute('КодПроекта', 'INTEGER', True),
        Attribute('НазваниеПроекта', 'VARCHAR', False),
        Attribute('Бюджет', 'DECIMAL', False)
    ]
    
    fds1 = [
        FunctionalDependency({attrs1[0]}, {attrs1[1], attrs1[2]}),  # КодСотрудника → ИмяСотрудника, Отдел
        FunctionalDependency({attrs1[2]}, {attrs1[3]}),            # Отдел → НачальникОтдела
        FunctionalDependency({attrs1[4]}, {attrs1[5], attrs1[6]})  # КодПроекта → НазваниеПроекта, Бюджет
    ]
    
    relation1 = Relation('СотрудникиПроекты', attrs1, fds1)
    examples.append(('Сотрудники-Проекты', relation1, 1000))
    
    # Пример 2: Студенты-Курсы
    print("📋 Создание примера 2: Студенты-Курсы")
    attrs2 = [
        Attribute('СтудентID', 'INTEGER', True),
        Attribute('ИмяСтудента', 'VARCHAR', False),
        Attribute('Группа', 'VARCHAR', False),
        Attribute('Куратор', 'VARCHAR', False),
        Attribute('КурсID', 'INTEGER', True),
        Attribute('НазваниеКурса', 'VARCHAR', False),
        Attribute('Преподаватель', 'VARCHAR', False),
        Attribute('Оценка', 'INTEGER', False)
    ]
    
    fds2 = [
        FunctionalDependency({attrs2[0]}, {attrs2[1], attrs2[2]}),  # СтудентID → ИмяСтудента, Группа
        FunctionalDependency({attrs2[2]}, {attrs2[3]}),            # Группа → Куратор
        FunctionalDependency({attrs2[4]}, {attrs2[5], attrs2[6]})  # КурсID → НазваниеКурса, Преподаватель
    ]
    
    relation2 = Relation('СтудентыКурсы', attrs2, fds2)
    examples.append(('Студенты-Курсы', relation2, 1500))
    
    # Пример 3: Заказы-Товары (более сложный)
    print("📋 Создание примера 3: Заказы-Товары")
    attrs3 = [
        Attribute('ЗаказID', 'INTEGER', True),
        Attribute('КлиентID', 'INTEGER', False),
        Attribute('ИмяКлиента', 'VARCHAR', False),
        Attribute('ГородКлиента', 'VARCHAR', False),
        Attribute('ТоварID', 'INTEGER', True),
        Attribute('НазваниеТовара', 'VARCHAR', False),
        Attribute('Категория', 'VARCHAR', False),
        Attribute('Цена', 'DECIMAL', False),
        Attribute('Количество', 'INTEGER', False),
        Attribute('ДатаЗаказа', 'DATE', False)
    ]
    
    fds3 = [
        FunctionalDependency({attrs3[1]}, {attrs3[2], attrs3[3]}),  # КлиентID → ИмяКлиента, ГородКлиента
        FunctionalDependency({attrs3[4]}, {attrs3[5], attrs3[6], attrs3[7]})  # ТоварID → НазваниеТовара, Категория, Цена
    ]
    
    relation3 = Relation('ЗаказыТовары', attrs3, fds3)
    examples.append(('Заказы-Товары', relation3, 2000))
    
    return examples


def run_single_test(name: str, relation: Relation, num_rows: int):
    """Запуск одного теста"""
    print(f"\n{'='*80}")
    print(f"🧪 ТЕСТ: {name}")
    print(f"📊 Количество строк: {num_rows:,}")
    print(f"📋 Атрибуты: {', '.join([attr.name for attr in relation.attributes])}")
    print(f"🔗 Функциональные зависимости:")
    for fd in relation.functional_dependencies:
        det_str = ', '.join([attr.name for attr in fd.determinant])
        dep_str = ', '.join([attr.name for attr in fd.dependent])
        print(f"   {det_str} → {dep_str}")
    print(f"{'='*80}")
    
    try:
        results = run_memory_test(relation, num_rows)
        
        if results:
            print(f"\n📈 РЕЗУЛЬТАТЫ ТЕСТА '{name}':")
            print(f"{'Уровень':<15} {'Размер (КБ)':<12} {'Строк':<10} {'Таблиц':<8} {'Изменение':<12}")
            print("-" * 70)
            
            original_size = results.get('Original', {}).get('total_size', 0) / 1024
            
            for level in ['Original', '2NF', '3NF', 'BCNF', '4NF']:
                if level in results:
                    data = results[level]
                    size_kb = data['total_size'] / 1024
                    rows = data['row_count']
                    tables = data['table_count']
                    
                    if level == 'Original':
                        change = "—"
                    else:
                        change_pct = ((size_kb - original_size) / original_size * 100) if original_size > 0 else 0
                        change = f"{change_pct:+.1f}%"
                    
                    print(f"{level:<15} {size_kb:<12.2f} {rows:<10,} {tables:<8} {change:<12}")
            
            # Строим графики
            print(f"\n📊 Построение графиков для '{name}'...")
            plot_memory_usage(results)
            
        else:
            print(f"❌ Тест '{name}' не дал результатов")
            
    except Exception as e:
        print(f"❌ Ошибка при выполнении теста '{name}': {e}")
        import traceback
        traceback.print_exc()


def run_all_tests():
    """Запуск всех тестов"""
    print("🚀 ЗАПУСК ТЕСТОВ ПАМЯТИ")
    print("=" * 80)
    
    examples = create_example_relations()
    
    for i, (name, relation, num_rows) in enumerate(examples, 1):
        print(f"\n🔄 Выполнение теста {i}/{len(examples)}")
        run_single_test(name, relation, num_rows)
        
        if i < len(examples):
            input(f"\n⏸️  Нажмите Enter для перехода к следующему тесту...")
    
    print(f"\n✅ ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ!")
    print("=" * 80)


def run_interactive_test():
    """Интерактивный режим выбора теста"""
    examples = create_example_relations()
    
    while True:
        print(f"\n📋 ДОСТУПНЫЕ ТЕСТЫ:")
        print("0. Выйти")
        for i, (name, _, num_rows) in enumerate(examples, 1):
            print(f"{i}. {name} ({num_rows:,} строк)")
        print(f"{len(examples) + 1}. Запустить все тесты")
        
        try:
            choice = int(input(f"\n👉 Выберите тест (0-{len(examples) + 1}): "))
            
            if choice == 0:
                print("👋 До свидания!")
                break
            elif choice == len(examples) + 1:
                run_all_tests()
            elif 1 <= choice <= len(examples):
                name, relation, num_rows = examples[choice - 1]
                run_single_test(name, relation, num_rows)
            else:
                print("❌ Неверный выбор!")
                
        except ValueError:
            print("❌ Введите корректное число!")
        except KeyboardInterrupt:
            print("\n👋 Прерывание пользователем. До свидания!")
            break


def main():
    """Главная функция"""
    print("🔬 СИСТЕМА ТЕСТИРОВАНИЯ ПАМЯТИ ПРИ НОРМАЛИЗАЦИИ БД")
    print("=" * 80)
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--all':
            run_all_tests()
        elif sys.argv[1] == '--help':
            print("Использование:")
            print("  python run_memory_tests.py           # Интерактивный режим")
            print("  python run_memory_tests.py --all     # Запустить все тесты")
            print("  python run_memory_tests.py --help    # Показать справку")
        else:
            print(f"❌ Неизвестный параметр: {sys.argv[1]}")
            print("Используйте --help для справки")
    else:
        run_interactive_test()


if __name__ == "__main__":
    main() 