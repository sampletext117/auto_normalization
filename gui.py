## gui.py
"""
Графический интерфейс для программы автоматической нормализации реляционных БД
"""
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from typing import List, Set, Optional, Tuple # Added Tuple
import json

# Импорт модулей программы
from models import (
    Attribute, FunctionalDependency, Relation,
    NormalForm, NormalizationResult
)
from fd_algorithms import FDAlgorithms
from analyzer import NormalFormAnalyzer
from decomposition import Decomposer
from visualization import VisualizationWindow, add_visualization_to_gui # MODIFIED: Added add_visualization_to_gui


class ScrollableFrame(ttk.Frame):
    def __init__(self, container, *args, **kwargs):
        super().__init__(container, *args, **kwargs)
        self.canvas = tk.Canvas(self, highlightthickness=0, bd=0)

        # Вертикальный скроллбар
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)

        # Внутренний фрейм, который будет содержать виджеты и прокручиваться
        self.interior = ttk.Frame(self.canvas)

        self.interior.bind(
            "<Configure>",
            lambda e: self.canvas.configure(
                scrollregion=self.canvas.bbox("all")
            )
        )

        self.canvas.create_window((0, 0), window=self.interior, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Привязка событий колеса мыши к холсту и внутреннему фрейму
        self._bind_mouse_wheel_events(self.canvas)
        self._bind_mouse_wheel_events(self.interior)
        # Примечание: дочерние элементы, добавляемые в self.interior, также должны иметь привязку,
        # если они "перехватывают" события мыши.

    def _on_mouse_wheel(self, event, target_canvas):
        """Обрабатывает прокрутку колеса мыши для Windows и macOS."""
        # Проверяем, находится ли курсор над этим компонентом прокрутки
        widget_under_mouse = self.winfo_containing(event.x_root, event.y_root)
        if widget_under_mouse:
            current_widget = widget_under_mouse
            is_relevant = False
            while current_widget:
                if current_widget == self:  # self это ScrollableFrame
                    is_relevant = True
                    break
                try:
                    current_widget = current_widget.master
                except AttributeError:
                    break
            if not is_relevant:
                return

        if event.delta == 0: return  # Некоторые события могут иметь delta = 0
        target_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _on_mouse_wheel_linux(self, event, target_canvas, direction):
        """Обрабатывает прокрутку колеса мыши для Linux (кнопки 4 и 5)."""
        widget_under_mouse = self.winfo_containing(event.x_root, event.y_root)
        if widget_under_mouse:
            current_widget = widget_under_mouse
            is_relevant = False
            while current_widget:
                if current_widget == self:  # self это ScrollableFrame
                    is_relevant = True
                    break
                try:
                    current_widget = current_widget.master
                except AttributeError:
                    break
            if not is_relevant:
                return
        target_canvas.yview_scroll(direction, "units")

    def _bind_mouse_wheel_events(self, widget_to_bind):
        """Привязывает события прокрутки к указанному виджету."""
        # Для Windows и macOS
        widget_to_bind.bind("<MouseWheel>",
                            lambda e: self._on_mouse_wheel(e, self.canvas), add="+")
        # Для Linux
        widget_to_bind.bind("<Button-4>",
                            lambda e: self._on_mouse_wheel_linux(e, self.canvas, -1), add="+")
        widget_to_bind.bind("<Button-5>",
                            lambda e: self._on_mouse_wheel_linux(e, self.canvas, 1), add="+")

    def bind_child_for_scrolling(self, child_widget):
        """Рекурсивно привязывает события прокрутки к дочернему виджету и его потомкам."""
        self._bind_mouse_wheel_events(child_widget)
        for child in child_widget.winfo_children():
            self.bind_child_for_scrolling(child)


class NormalizationGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Автоматическая нормализация реляционных БД")
        self.root.geometry("1200x800")

        # Данные
        self.attributes: List[Attribute] = []
        self.functional_dependencies: List[FunctionalDependency] = []
        self.current_relation: Optional[Relation] = None
        self.normalization_result: Optional[NormalizationResult] = None

        # MODIFIED: Data for FD checkboxes
        self.determinant_vars: List[Tuple[Attribute, tk.BooleanVar]] = []
        self.dependent_vars: List[Tuple[Attribute, tk.BooleanVar]] = []


        # Создание интерфейса
        self.create_menu()
        self.create_widgets()

    def create_menu(self):
        """Создание меню"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Файл", menu=file_menu)
        file_menu.add_command(label="Новый проект", command=self.new_project)
        file_menu.add_command(label="Загрузить пример", command=self.load_example)
        file_menu.add_separator()
        file_menu.add_command(label="Выход", command=self.root.quit)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Справка", menu=help_menu)
        help_menu.add_command(label="О программе", command=self.show_about)

    def create_widgets(self):
        """Создание основных виджетов"""
        # Главный контейнер с вкладками
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill='both', expand=True, padx=5, pady=5)

        # Вкладка ввода данных
        self.input_frame = ttk.Frame(notebook)
        notebook.add(self.input_frame, text="Ввод данных")
        self.create_input_widgets()

        # Вкладка анализа
        self.analysis_frame = ttk.Frame(notebook)
        notebook.add(self.analysis_frame, text="Анализ")
        self.create_analysis_widgets()

        # Вкладка нормализации
        self.normalization_frame = ttk.Frame(notebook)
        notebook.add(self.normalization_frame, text="Нормализация")
        self.create_normalization_widgets()

        # Вкладка результатов
        self.results_frame = ttk.Frame(notebook)
        notebook.add(self.results_frame, text="Результаты")
        self.create_results_widgets()

    def create_input_widgets(self):
        """Создание виджетов для ввода данных"""
        # Фрейм для названия отношения (без изменений)
        name_frame = ttk.LabelFrame(self.input_frame, text="Отношение", padding=10)
        name_frame.pack(fill='x', padx=5, pady=5)
        ttk.Label(name_frame, text="Название:").pack(side='left', padx=5)
        self.relation_name_var = tk.StringVar(value="Отношение1")
        ttk.Entry(name_frame, textvariable=self.relation_name_var, width=30).pack(side='left', padx=5)

        # Фрейм для атрибутов (без изменений в этой части)
        attr_frame = ttk.LabelFrame(self.input_frame, text="Атрибуты", padding=10)
        attr_frame.pack(fill='x', padx=5,
                        pady=5)  # Изменено на fill='x', чтобы не занимал слишком много места по вертикали
        # ... (остальная часть фрейма атрибутов без изменений) ...
        attr_control = ttk.Frame(attr_frame)
        attr_control.pack(fill='x', pady=5)
        ttk.Label(attr_control, text="Имя:").pack(side='left', padx=5)
        self.attr_name_var = tk.StringVar()
        ttk.Entry(attr_control, textvariable=self.attr_name_var, width=20).pack(side='left', padx=5)
        ttk.Label(attr_control, text="Тип:").pack(side='left', padx=5)
        self.attr_type_var = tk.StringVar(value="VARCHAR")
        type_combo = ttk.Combobox(attr_control, textvariable=self.attr_type_var, width=15)
        type_combo['values'] = ['VARCHAR', 'INTEGER', 'DATE', 'DECIMAL', 'BOOLEAN']
        type_combo.pack(side='left', padx=5)
        self.is_pk_var = tk.BooleanVar()
        ttk.Checkbutton(attr_control, text="Первичный ключ", variable=self.is_pk_var).pack(side='left', padx=5)
        ttk.Button(attr_control, text="Добавить атрибут", command=self.add_attribute).pack(side='left', padx=5)
        self.attr_listbox = tk.Listbox(attr_frame, height=6)  # Уменьшена высота списка атрибутов
        self.attr_listbox.pack(fill='x', expand=False, pady=5)  # fill='x', expand=False
        attr_buttons = ttk.Frame(attr_frame)
        attr_buttons.pack(fill='x')
        ttk.Button(attr_buttons, text="Удалить выбранный", command=self.remove_attribute).pack(side='left', padx=5)
        ttk.Button(attr_buttons, text="Очистить все", command=self.clear_attributes).pack(side='left', padx=5)

        # --- MODIFIED: Фрейм для функциональных зависимостей с измененным макетом ---
        fd_main_label_frame = ttk.LabelFrame(self.input_frame, text="Функциональные зависимости", padding=10)
        # fd_main_label_frame теперь будет занимать оставшееся место
        fd_main_label_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # Верхний контейнер для горизонтального расположения: [Выбор ФЗ] | [Список ФЗ]
        top_fd_container = ttk.Frame(fd_main_label_frame)
        top_fd_container.pack(fill='both', expand=True, pady=(0, 5))

        # Левая часть: Панели выбора атрибутов и кнопка "Добавить"
        fd_selection_panel = ttk.Frame(top_fd_container)
        fd_selection_panel.pack(side='left', fill='y', padx=(0, 10))  # Fill vertically

        # Контейнер для детерминанта
        determinant_outer_container = ttk.Frame(fd_selection_panel, width=170, height=130)  # Задаем размеры здесь
        determinant_outer_container.pack(side='left', fill='none', expand=False,
                                         padx=2)  # Не расширяем, фиксированный размер
        determinant_outer_container.pack_propagate(False)  # Важно!
        ttk.Label(determinant_outer_container, text="Детерминант:").pack(anchor='nw', pady=(0, 1))
        self.determinant_cb_frame_scrollable = ScrollableFrame(determinant_outer_container, relief="sunken",
                                                               borderwidth=1)
        self.determinant_cb_frame_scrollable.pack(fill='both', expand=True)
        self.determinant_cb_frame = self.determinant_cb_frame_scrollable.interior

        # Панель для стрелки и кнопки "Добавить ФЗ"
        arrow_add_fd_panel = ttk.Frame(fd_selection_panel)
        arrow_add_fd_panel.pack(side='left', fill='y', padx=7, anchor='center')  # fill='y'
        ttk.Label(arrow_add_fd_panel, text="→", font=("Arial", 18)).pack(expand=True,
                                                                         anchor='center')  # Центрируем стрелку
        ttk.Button(arrow_add_fd_panel, text="Добавить ФЗ", command=self.add_functional_dependency).pack(side='bottom',
                                                                                                        pady=(5, 0))

        # Контейнер для зависимой части
        dependent_outer_container = ttk.Frame(fd_selection_panel, width=170, height=130)  # Задаем размеры здесь
        dependent_outer_container.pack(side='left', fill='none', expand=False, padx=2)  # Не расширяем
        dependent_outer_container.pack_propagate(False)  # Важно!
        ttk.Label(dependent_outer_container, text="Зависимые:").pack(anchor='nw', pady=(0, 1))
        self.dependent_cb_frame_scrollable = ScrollableFrame(dependent_outer_container, relief="sunken", borderwidth=1)
        self.dependent_cb_frame_scrollable.pack(fill='both', expand=True)
        self.dependent_cb_frame = self.dependent_cb_frame_scrollable.interior

        # Правая часть: Список введенных ФЗ
        fd_list_display_panel = ttk.Frame(top_fd_container)
        fd_list_display_panel.pack(side='left', fill='both', expand=True)

        ttk.Label(fd_list_display_panel, text="Введенные ФЗ:").pack(anchor='nw')
        self.fd_listbox = tk.Listbox(fd_list_display_panel,
                                     height=7)  # Высота списка = высота панелей выбора - место под заголовок
        self.fd_listbox.pack(fill='both', expand=True, pady=(2, 0))

        # Нижняя панель для кнопок управления списком ФЗ (Удалить, Очистить)
        fd_list_buttons_panel = ttk.Frame(fd_main_label_frame)
        fd_list_buttons_panel.pack(fill='x', pady=(8, 0))  # Отступ сверху
        ttk.Button(fd_list_buttons_panel, text="Удалить выбранную ФЗ",
                   command=self.remove_fd).pack(side='left', padx=5)
        ttk.Button(fd_list_buttons_panel, text="Очистить все ФЗ",
                   command=self.clear_fds).pack(side='left', padx=5)
        # --- END MODIFIED FD Layout ---

    def create_analysis_widgets(self):
        """Создание виджетов для анализа"""
        control_frame = ttk.Frame(self.analysis_frame)
        control_frame.pack(fill='x', padx=5, pady=5)

        ttk.Button(control_frame, text="Выполнить анализ",
                   command=self.perform_analysis, style='Accent.TButton').pack(pady=10)

        self.analysis_text = scrolledtext.ScrolledText(self.analysis_frame, wrap=tk.WORD, height=25)
        self.analysis_text.pack(fill='both', expand=True, padx=5, pady=5)

    def create_normalization_widgets(self):
        """Создание виджетов для нормализации"""
        control_frame = ttk.LabelFrame(self.normalization_frame, text="Параметры нормализации", padding=10)
        control_frame.pack(fill='x', padx=5, pady=5)

        ttk.Label(control_frame, text="Целевая нормальная форма:").pack(side='left', padx=5)
        self.target_nf_var = tk.StringVar(value="3НФ")
        nf_combo = ttk.Combobox(control_frame, textvariable=self.target_nf_var, width=15)
        nf_combo['values'] = ['2НФ', '3НФ', 'НФБК', '4НФ']
        nf_combo.pack(side='left', padx=5)

        ttk.Button(control_frame, text="Выполнить нормализацию",
                   command=self.perform_normalization, style='Accent.TButton').pack(side='left', padx=20)

        self.normalization_text = scrolledtext.ScrolledText(self.normalization_frame, wrap=tk.WORD, height=20)
        self.normalization_text.pack(fill='both', expand=True, padx=5, pady=5)

    def create_results_widgets(self):
        """Создание виджетов для отображения результатов"""
        export_frame = ttk.Frame(self.results_frame)
        export_frame.pack(fill='x', padx=5, pady=5)

        ttk.Button(export_frame, text="Экспорт в SQL",
                   command=self.export_to_sql).pack(side='left', padx=5)
        ttk.Button(export_frame, text="Сохранить отчет",
                   command=self.save_report).pack(side='left', padx=5)

        self.results_text = scrolledtext.ScrolledText(self.results_frame, wrap=tk.WORD)
        self.results_text.pack(fill='both', expand=True, padx=5, pady=5)

        # MODIFIED: Add visualization button if the method was injected
        if hasattr(self, 'create_visualization_button'):
            self.create_visualization_button()
        else:
            # This case should ideally not happen if add_visualization_to_gui is called correctly
            print("Warning: Visualization tools not loaded.")


    # MODIFIED: Helper to update FD attribute checkboxes
    def _update_fd_attribute_checkboxes(self):
        """Обновляет чекбоксы для выбора атрибутов ФЗ."""
        # self.determinant_cb_frame и self.dependent_cb_frame теперь внутренние фреймы ScrollableFrame

        for widget in self.determinant_cb_frame.winfo_children():  # Очищаем внутренний фрейм
            widget.destroy()
        for widget in self.dependent_cb_frame.winfo_children():  # Очищаем внутренний фрейм
            widget.destroy()

        self.determinant_vars.clear()
        self.dependent_vars.clear()

        for attr in self.attributes:
            # Чекбокс для детерминанта
            det_var = tk.BooleanVar()
            # Добавляем чекбокс во внутренний фрейм (self.determinant_cb_frame)
            det_cb = ttk.Checkbutton(self.determinant_cb_frame, text=attr.name, variable=det_var)
            det_cb.pack(anchor='w', padx=2, pady=1)
            self.determinant_vars.append((attr, det_var))
            # Привязываем события прокрутки к новому чекбоксу, чтобы они передавались на канвас
            self.determinant_cb_frame_scrollable.bind_child_for_scrolling(det_cb)

            # Чекбокс для зависимой части
            dep_var = tk.BooleanVar()
            # Добавляем чекбокс во внутренний фрейм (self.dependent_cb_frame)
            dep_cb = ttk.Checkbutton(self.dependent_cb_frame, text=attr.name, variable=dep_var)
            dep_cb.pack(anchor='w', padx=2, pady=1)
            self.dependent_vars.append((attr, dep_var))
            # Привязываем события прокрутки к новому чекбоксу
            self.dependent_cb_frame_scrollable.bind_child_for_scrolling(dep_cb)

        # Обновляем scrollregion после добавления всех элементов
        # Это важно, чтобы ScrollableFrame знал об изменении размера содержимого
        self.determinant_cb_frame_scrollable.canvas.update_idletasks()
        self.determinant_cb_frame_scrollable.canvas.config(
            scrollregion=self.determinant_cb_frame_scrollable.canvas.bbox("all"))

        self.dependent_cb_frame_scrollable.canvas.update_idletasks()
        self.dependent_cb_frame_scrollable.canvas.config(
            scrollregion=self.dependent_cb_frame_scrollable.canvas.bbox("all"))

    def add_attribute(self):
        """Добавление атрибута"""
        name = self.attr_name_var.get().strip()
        if not name:
            messagebox.showwarning("Ошибка", "Введите имя атрибута")
            return

        # MODIFIED: Проверка уникальности по self.attributes
        for existing_attr in self.attributes:
            if existing_attr.name == name:
                messagebox.showwarning("Ошибка", "Атрибут с таким именем уже существует")
                return

        attr = Attribute(
            name=name,
            data_type=self.attr_type_var.get(),
            is_primary_key=self.is_pk_var.get()
        )
        self.attributes.append(attr)

        list_text = f"{attr.name} ({attr.data_type})"
        if attr.is_primary_key:
            list_text += " [PK]"
        self.attr_listbox.insert(tk.END, list_text)

        # MODIFIED: Обновление чекбоксов для ФЗ
        self._update_fd_attribute_checkboxes()

        self.attr_name_var.set("")
        self.is_pk_var.set(False)

    def remove_attribute(self):
        """Удаление выбранного атрибута"""
        selection = self.attr_listbox.curselection()
        if not selection:
            return

        index = selection[0]
        attr_to_remove = self.attributes[index] # Get attribute object

        for fd in self.functional_dependencies:
            if attr_to_remove in fd.determinant or attr_to_remove in fd.dependent:
                messagebox.showwarning("Ошибка",
                                       f"Атрибут '{attr_to_remove.name}' используется в функциональных зависимостях. Сначала удалите ФЗ.")
                return

        self.attributes.pop(index)
        self.attr_listbox.delete(index)

        # MODIFIED: Обновление чекбоксов для ФЗ
        self._update_fd_attribute_checkboxes()

    def clear_attributes(self):
        """Очистка всех атрибутов"""
        if self.functional_dependencies:
            messagebox.showwarning("Ошибка",
                                   "Сначала удалите все функциональные зависимости")
            return

        self.attributes.clear()
        self.attr_listbox.delete(0, tk.END)

        # MODIFIED: Обновление чекбоксов для ФЗ
        self._update_fd_attribute_checkboxes()

    # MODIFIED: Методы для работы с ФЗ
    def add_functional_dependency(self):
        """Добавление функциональной зависимости из чекбоксов"""
        determinant_attrs: Set[Attribute] = set()
        for attr, var in self.determinant_vars:
            if var.get():
                determinant_attrs.add(attr)

        dependent_attrs: Set[Attribute] = set()
        for attr, var in self.dependent_vars:
            if var.get():
                dependent_attrs.add(attr)

        if not determinant_attrs:
            messagebox.showwarning("Ошибка", "Выберите атрибуты детерминанта")
            return
        if not dependent_attrs:
            messagebox.showwarning("Ошибка", "Выберите зависимые атрибуты")
            return

        if dependent_attrs.issubset(determinant_attrs):
            messagebox.showinfo("Информация",
                                "Это тривиальная зависимость (все зависимые атрибуты содержатся в детерминанте).")
            # Можно решить, добавлять ли их. Для анализа они обычно не нужны.
            # return

        # Проверка на то, что атрибут не является одновременно детерминантом и зависимым сам по себе (X -> X)
        # или что детерминант и зависимая часть полностью идентичны
        if determinant_attrs == dependent_attrs:
             messagebox.showwarning("Ошибка", "Детерминант и зависимая часть не могут быть полностью идентичны для нетривиальной ФЗ.")
             return


        fd = FunctionalDependency(determinant_attrs, dependent_attrs)

        # Проверка на дубликат
        if any(fd.determinant == existing_fd.determinant and fd.dependent == existing_fd.dependent
               for existing_fd in self.functional_dependencies):
            messagebox.showwarning("Ошибка", "Такая функциональная зависимость уже существует.")
            return

        self.functional_dependencies.append(fd)
        self.fd_listbox.insert(tk.END, str(fd))

        # Очистка чекбоксов
        for _, var in self.determinant_vars:
            var.set(False)
        for _, var in self.dependent_vars:
            var.set(False)

    def remove_fd(self):
        """Удаление выбранной ФЗ"""
        selection = self.fd_listbox.curselection()
        if not selection:
            return

        index = selection[0]
        self.functional_dependencies.pop(index)
        self.fd_listbox.delete(index)

    def clear_fds(self):
        """Очистка всех ФЗ"""
        self.functional_dependencies.clear()
        self.fd_listbox.delete(0, tk.END)

    def perform_analysis(self):
        """Выполнение анализа отношения"""
        if not self.attributes:
            messagebox.showwarning("Ошибка", "Добавьте атрибуты отношения")
            return

        self.current_relation = Relation(
            name=self.relation_name_var.get(),
            attributes=self.attributes.copy(),
            functional_dependencies=self.functional_dependencies.copy()
        )

        analyzer = NormalFormAnalyzer(self.current_relation)
        report = analyzer.get_analysis_report()

        report += "\n" + "=" * 50 + "\n"
        report += "Дополнительный анализ:\n\n"

        minimal_cover = FDAlgorithms.minimal_cover(self.functional_dependencies)
        report += f"Минимальное покрытие ({len(minimal_cover)} ФЗ):\n"
        for fd in minimal_cover:
            report += f"  - {fd}\n"

        self.analysis_text.delete(1.0, tk.END)
        self.analysis_text.insert(1.0, report)

    def perform_normalization(self):
        """Выполнение нормализации"""
        if not self.current_relation:
            self.perform_analysis() # Попытка создать current_relation, если его нет
            if not self.current_relation: # Если и после этого нет (например, нет атрибутов)
                 messagebox.showwarning("Ошибка", "Сначала выполните анализ или убедитесь, что есть атрибуты.")
                 return


        target = self.target_nf_var.get()

        try:
            if target == "2НФ":
                result = Decomposer.decompose_to_2nf(self.current_relation)
            elif target == "3НФ":
                result = Decomposer.decompose_to_3nf(self.current_relation)
            elif target == "НФБК":
                result = Decomposer.decompose_to_bcnf(self.current_relation)
            elif target == "4НФ":
                result = Decomposer.decompose_to_4nf(self.current_relation)
            else:
                messagebox.showerror("Ошибка", "Неизвестная целевая нормальная форма.")
                return

            self.normalization_result = result

            output = f"Нормализация в {target}\n"
            output += "=" * 50 + "\n\n"
            output += result.get_summary()

            if result.steps:
                output += "\nШаги декомпозиции:\n"
                for i, step in enumerate(result.steps, 1):
                    output += f"\n{i}. {step.reason}\n"
                    output += f"   {step.original_relation} → "
                    output += ", ".join([str(r) for r in step.resulting_relations]) + "\n"
                    if step.violated_dependency:
                         output += f"      Нарушенная ФЗ: {step.violated_dependency}\n"


            output += f"\nСохранение зависимостей:\n"
            output += f"  - Сохранено: {len(result.preserved_dependencies)}\n"
            output += f"  - Потеряно: {len(result.lost_dependencies)}\n"

            if result.lost_dependencies:
                output += "\nПотерянные зависимости:\n"
                for fd in result.lost_dependencies:
                    output += f"  - {fd}\n"

            output += "\nАнализ результирующих отношений:\n"
            for rel in result.decomposed_relations:
                analyzer = NormalFormAnalyzer(rel)
                nf, _ = analyzer.determine_normal_form()
                output += f"  - {rel}: {nf.value}\n"

            self.normalization_text.delete(1.0, tk.END)
            self.normalization_text.insert(1.0, output)

            self.update_results()

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при нормализации: {str(e)}")
            import traceback
            traceback.print_exc()


    def update_results(self):
        """Обновление вкладки результатов"""
        if not self.normalization_result:
            return

        output = "ИТОГОВЫЕ РЕЗУЛЬТАТЫ НОРМАЛИЗАЦИИ\n"
        output += "=" * 60 + "\n\n"

        output += "Исходное отношение:\n"
        output += f"  {self.normalization_result.original_relation}\n"
        output += f"  Нормальная форма: {self.normalization_result.original_form.value}\n\n"

        output += f"Результирующие отношения ({len(self.normalization_result.decomposed_relations)}):\n\n"

        for i, rel in enumerate(self.normalization_result.decomposed_relations, 1):
            output += f"{i}. {rel.name}\n"
            output += f"   Атрибуты: {', '.join([a.name for a in rel.attributes])}\n"

            analyzer = NormalFormAnalyzer(rel)
            keys = analyzer.candidate_keys # Используем вычисленные кандидатные ключи
            if keys:
                output += f"   Ключи: "
                key_strs = ["{" + ", ".join([a.name for a in k]) + "}" for k in keys]
                output += ", ".join(key_strs) + "\n"

            # ФЗ для каждого результирующего отношения
            if rel.functional_dependencies:
                output += f"   Функциональные зависимости ({len(rel.functional_dependencies)}):\n"
                for fd_idx, fd_item in enumerate(rel.functional_dependencies):
                    output += f"     {fd_idx+1}. {fd_item}\n"
            else:
                output += f"   Функциональные зависимости: отсутствуют\n"
            output += "\n"


        output += "\nSQL DDL:\n"
        output += "-" * 40 + "\n"
        output += self.generate_sql()

        self.results_text.delete(1.0, tk.END)
        self.results_text.insert(1.0, output)

    def generate_sql(self):
        """Генерация SQL DDL для результирующих отношений"""
        if not self.normalization_result:
            return ""

        sql = ""
        for rel in self.normalization_result.decomposed_relations:
            sql += f"CREATE TABLE {rel.name} (\n"

            # Собираем атрибуты первичного ключа из анализатора, если is_primary_key не установлен
            # или если хотим использовать кандидатные ключи для PK
            analyzer = NormalFormAnalyzer(rel) # Анализируем каждое декомпозированное отношение
            # Предположим, первый кандидатный ключ становится первичным, если не задан is_primary_key
            pk_attributes_for_table: Set[Attribute] = set()
            if analyzer.candidate_keys:
                pk_attributes_for_table = analyzer.candidate_keys[0]


            attr_definitions = []
            for attr in rel.attributes:
                attr_def = f"    {attr.name} {attr.data_type}"
                # Если атрибут является частью вычисленного PK для этой таблицы
                if attr in pk_attributes_for_table:
                    attr_def += " NOT NULL"
                attr_definitions.append(attr_def)

            sql += ",\n".join(attr_definitions)

            if pk_attributes_for_table:
                pk_names = ", ".join([a.name for a in pk_attributes_for_table])
                sql += f",\n    PRIMARY KEY ({pk_names})\n"
            else:
                sql += "\n" # Если нет ПК, просто завершаем список атрибутов

            sql += ");\n\n"
        return sql

    def export_to_sql(self):
        """Экспорт в SQL файл"""
        if not self.normalization_result:
            messagebox.showwarning("Ошибка", "Нет результатов для экспорта")
            return

        from tkinter import filedialog
        filename = filedialog.asksaveasfilename(
            defaultextension=".sql",
            filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
        )

        if filename:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(self.generate_sql())
            messagebox.showinfo("Успешно", f"SQL экспортирован в {filename}")

    def save_report(self):
        """Сохранение отчета"""
        if not self.normalization_result: # Можно также разрешить сохранение отчета анализа
            messagebox.showwarning("Ошибка", "Нет результатов нормализации для сохранения отчета")
            return

        report_content = self.results_text.get(1.0, tk.END)
        if not report_content.strip():
             report_content = self.normalization_text.get(1.0, tk.END) # Fallback to normalization steps
        if not report_content.strip():
            report_content = self.analysis_text.get(1.0, tk.END) # Fallback to analysis

        if not report_content.strip():
            messagebox.showwarning("Ошибка", "Нет данных для сохранения отчета")
            return


        from tkinter import filedialog
        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )

        if filename:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report_content)
            messagebox.showinfo("Успешно", f"Отчет сохранен в {filename}")

    def new_project(self):
        """Создание нового проекта"""
        if messagebox.askyesno("Новый проект", "Очистить все данные?"):
            self.clear_attributes() # Очистит атрибуты и чекбоксы ФЗ
            self.clear_fds()      # Очистит список ФЗ
            self.relation_name_var.set("Отношение1")
            self.analysis_text.delete(1.0, tk.END)
            self.normalization_text.delete(1.0, tk.END)
            self.results_text.delete(1.0, tk.END)
            self.current_relation = None
            self.normalization_result = None
            self.attr_name_var.set("")
            self.is_pk_var.set(False)


    # MODIFIED: load_example
    def load_example(self):
        """Загрузка примера"""
        # Очистка без диалогового окна
        self.attributes.clear()
        self.attr_listbox.delete(0, tk.END)
        self._update_fd_attribute_checkboxes() # Очистит чекбоксы
        self.clear_fds()
        self.relation_name_var.set("Отношение1") # будет переопределено
        self.analysis_text.delete(1.0, tk.END)
        self.normalization_text.delete(1.0, tk.END)
        self.results_text.delete(1.0, tk.END)
        self.current_relation = None
        self.normalization_result = None
        self.attr_name_var.set("")
        self.is_pk_var.set(False)

        self.relation_name_var.set("СотрудникиПроекты")

        example_attrs_data = [
            ("КодСотрудника", "INTEGER", True),
            ("ИмяСотрудника", "VARCHAR", False),
            ("Отдел", "VARCHAR", False),
            ("НачальникОтдела", "VARCHAR", False),
            ("КодПроекта", "INTEGER", True), # В классическом примере Сотр(КодСотр, ...) Проект(КодПроекта,...) Связь(КодСотр, КодПроекта, Часы)
                                          # здесь это одно отношение, так что PK могут быть оба
            ("НазваниеПроекта", "VARCHAR", False),
            ("Бюджет", "DECIMAL", False)
        ]

        for name, dtype, is_pk in example_attrs_data:
            self.attr_name_var.set(name)
            self.attr_type_var.set(dtype)
            self.is_pk_var.set(is_pk)
            self.add_attribute() # Это обновит чекбоксы ФЗ

        # Helper для установки чекбоксов по имени атрибута
        def set_fd_checkboxes_by_name(det_names: List[str], dep_names: List[str]):
            for attr, var in self.determinant_vars:
                var.set(attr.name in det_names)
            for attr, var in self.dependent_vars:
                var.set(attr.name in dep_names)

        # ФЗ
        # КодСотрудника → ИмяСотрудника, Отдел
        set_fd_checkboxes_by_name(["КодСотрудника"], ["ИмяСотрудника", "Отдел"])
        self.add_functional_dependency()

        # Отдел → НачальникОтдела
        set_fd_checkboxes_by_name(["Отдел"], ["НачальникОтдела"])
        self.add_functional_dependency()

        # КодПроекта → НазваниеПроекта, Бюджет
        set_fd_checkboxes_by_name(["КодПроекта"], ["НазваниеПроекта", "Бюджет"])
        self.add_functional_dependency()

        # (КодСотрудника, КодПроекта) -> Часы (Если бы был атрибут Часы, для примера)
        # Допустим, у нас нет атрибута "Часы", но для полноты картины, как бы это выглядело:
        # self.attr_name_var.set("ЧасыРаботы")
        # self.attr_type_var.set("INTEGER")
        # self.is_pk_var.set(False)
        # self.add_attribute()
        # set_fd_checkboxes_by_name(["КодСотрудника", "КодПроекта"], ["ЧасыРаботы"])
        # self.add_functional_dependency()


        messagebox.showinfo("Пример загружен",
                            "Загружен пример отношения. \n"
                            "Возможные нарушения: \n"
                            "- 2НФ, если (КодСотрудника, КодПроекта) ключ, а есть зависимости от частей ключа.\n"
                            "- 3НФ из-за транзитивной зависимости Отдел -> НачальникОтдела (КодСотрудника -> Отдел -> НачальникОтдела).")


    def show_about(self):
        """Отображение информации о программе"""
        about_text = """Программа автоматической нормализации реляционных БД

Версия: 1.1
Автор: Зуев Тимофей, ИУ7-85Б (доработано Gemini)

Программа позволяет:
- Вводить атрибуты и функциональные зависимости
- Анализировать нормальные формы отношений (1НФ, 2НФ, 3НФ, НФБК, 4НФ)
- Выполнять декомпозицию в 2НФ, 3НФ, НФБК и 4НФ
- Проверять сохранение функциональных зависимостей
- Генерировать SQL DDL для результирующих отношений
- Визуализировать схемы отношений (исходное и результат декомпозиции)

Используемые алгоритмы:
- Алгоритм синтеза для 3НФ
- Алгоритм декомпозиции для НФБК
- Вычисление замыкания атрибутов
- Поиск минимального покрытия ФЗ
- Алгоритмы проверки нормальных форм"""

        messagebox.showinfo("О программе", about_text)


def main():
    """Главная функция"""
    root = tk.Tk()

    style = ttk.Style()
    # Попробуем стандартные темы, если 'clam' доступна, она обычно выглядит неплохо
    try:
        style.theme_use('clam')
    except tk.TclError:
        try:
            style.theme_use('alt')
        except tk.TclError:
            pass # Используем тему по умолчанию, если другие недоступны

    style.configure('Accent.TButton', foreground='white', background='#007bff', relief="raised")
    style.map('Accent.TButton', background=[('active', '#0056b3')])


    # MODIFIED: Применяем патч для добавления функционала визуализации
    add_visualization_to_gui(NormalizationGUI)

    app = NormalizationGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()