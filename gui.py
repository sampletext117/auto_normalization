## gui.py
"""
Графический интерфейс для программы автоматической нормализации реляционных БД
"""
import io
from contextlib import redirect_stdout
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from typing import List, Set, Optional, Tuple, Dict # Added Tuple
import json

# Импорт модулей программы
from models import (
    Attribute, FunctionalDependency, Relation,
    NormalForm, NormalizationResult, MultivaluedDependency
)
from fd_algorithms import FDAlgorithms
from analyzer import NormalFormAnalyzer
from decomposition import Decomposer
from visualization import VisualizationWindow, add_visualization_to_gui # MODIFIED: Added add_visualization_to_gui
from data_test import test_decomposition, DB_PARAMS
from performance_test import run_performance_test, plot_performance_histogram
from memory_test import run_memory_test, plot_memory_usage




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

        self._bind_mouse_wheel_events(self.canvas)
        self._bind_mouse_wheel_events(self.interior)

    def _on_mouse_wheel(self, event, target_canvas):
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

        if event.delta == 0: return
        target_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _on_mouse_wheel_linux(self, event, target_canvas, direction):
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
        widget_to_bind.bind("<MouseWheel>",
                            lambda e: self._on_mouse_wheel(e, self.canvas), add="+")
        # Для Linux
        widget_to_bind.bind("<Button-4>",
                            lambda e: self._on_mouse_wheel_linux(e, self.canvas, -1), add="+")
        widget_to_bind.bind("<Button-5>",
                            lambda e: self._on_mouse_wheel_linux(e, self.canvas, 1), add="+")

    def bind_child_for_scrolling(self, child_widget):
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
        self.multivalued_dependencies: List[MultivaluedDependency] = []
        self.current_relation: Optional[Relation] = None
        self.normalization_result: Optional[NormalizationResult] = None

        self.db_host_var = tk.StringVar(value="")
        self.db_port_var = tk.StringVar(value="")
        self.db_name_var = tk.StringVar(value="")
        self.db_user_var = tk.StringVar(value="")
        self.db_password_var = tk.StringVar(value="")
        # Жёстко захардкоженные "дефолтные" креды
        self.default_db_params = {
            'host': 'localhost',
            'port': '5432',
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'Iamtaskforce1'
        }

        # MODIFIED: Data for FD checkboxes
        self.determinant_vars: List[Tuple[Attribute, tk.BooleanVar]] = []
        self.dependent_vars: List[Tuple[Attribute, tk.BooleanVar]] = []


        # Создание интерфейса
        self.create_menu()
        self.create_widgets()

    def make_text_readonly_but_copyable(self, text_widget):
        """
        Делает текстовый виджет доступным только для чтения, но с возможностью копирования
        """

        # Биндим события для предотвращения редактирования
        def prevent_edit(event):
            if event.keysym not in ['Up', 'Down', 'Left', 'Right', 'Home', 'End',
                                    'Prior', 'Next', 'Control_L', 'Control_R',
                                    'c', 'C', 'a', 'A']:
                if not (event.state & 0x4):  # Не Ctrl
                    return "break"

        text_widget.bind("<Key>", prevent_edit)
        text_widget.bind("<Button-2>", lambda e: "break")  # Запрет вставки средней кнопкой мыши

        # Разрешаем выделение и копирование
        text_widget.bind("<Control-c>", lambda e: None)
        text_widget.bind("<Control-a>", lambda e: text_widget.tag_add("sel", "1.0", "end"))

        # Устанавливаем курсор для индикации что текст можно выделять
        text_widget.config(cursor="arrow")

    def show_copyable_result_window(self, title: str, content: str, width: int = 800, height: int = 600):
        """
        Создает окно с результатами, из которого можно копировать текст
        """
        result_window = tk.Toplevel(self.root)
        result_window.title(title)
        result_window.geometry(f"{width}x{height}")

        # Фрейм для текста
        text_frame = ttk.Frame(result_window)
        text_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # Создаем ScrolledText
        text_widget = scrolledtext.ScrolledText(
            text_frame,
            wrap=tk.WORD,
            font=("Courier", 9)
        )
        text_widget.pack(fill='both', expand=True)

        # Вставляем контент
        text_widget.insert('1.0', content)

        # Делаем текст доступным только для чтения, но копируемым
        self.make_text_readonly_but_copyable(text_widget)

        # Фрейм для кнопок
        button_frame = ttk.Frame(result_window)
        button_frame.pack(fill='x', padx=5, pady=5)

        # Кнопка копирования всего текста
        def copy_all():
            text_widget.tag_add("sel", "1.0", "end")
            text_widget.event_generate("<<Copy>>")
            # Показываем уведомление
            copy_label = ttk.Label(button_frame, text="Скопировано в буфер обмена!",
                                   foreground="green")
            copy_label.pack(side='left', padx=10)
            result_window.after(2000, copy_label.destroy)

        ttk.Button(button_frame, text="Копировать всё", command=copy_all).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Закрыть", command=result_window.destroy).pack(side='right', padx=5)

        # Информационная надпись
        info_label = ttk.Label(button_frame, text="Используйте Ctrl+C для копирования выделенного текста",
                               foreground="gray")
        info_label.pack(side='left', padx=20)

        return result_window, text_widget

    def create_menu(self):
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
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill='both', expand=True, padx=5, pady=5)

        self.input_frame = ttk.Frame(notebook)
        notebook.add(self.input_frame, text="Ввод данных")
        self.create_input_widgets()

        self.analysis_frame = ttk.Frame(notebook)
        notebook.add(self.analysis_frame, text="Анализ")
        self.create_analysis_widgets()

        self.normalization_frame = ttk.Frame(notebook)
        notebook.add(self.normalization_frame, text="Нормализация")
        self.create_normalization_widgets()

        self.results_frame = ttk.Frame(notebook)
        notebook.add(self.results_frame, text="Результаты")
        self.create_results_widgets()

    def create_input_widgets(self):
        name_frame = ttk.LabelFrame(self.input_frame, text="Отношение", padding=10)
        name_frame.pack(fill='x', padx=5, pady=5)
        ttk.Label(name_frame, text="Название:").pack(side='left', padx=5)
        self.relation_name_var = tk.StringVar(value="Отношение1")
        ttk.Entry(name_frame, textvariable=self.relation_name_var, width=30).pack(side='left', padx=5)

        attr_frame = ttk.LabelFrame(self.input_frame, text="Атрибуты", padding=10)
        attr_frame.pack(fill='x', padx=5,
                        pady=5)
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

        fd_main_label_frame = ttk.LabelFrame(self.input_frame, text="Зависимости", padding=10)
        fd_main_label_frame.pack(fill='both', expand=True, padx=5, pady=5)

        main_hbox = ttk.Frame(fd_main_label_frame)
        main_hbox.pack(fill='both', expand=True)

        selection_vbox = ttk.Frame(main_hbox)
        selection_vbox.pack(side='left', fill='y', padx=(0, 10))

        ttk.Label(selection_vbox, text="Детерминант:").pack(anchor='w')
        det_frame_container = ttk.Frame(selection_vbox, width=180, height=140)
        det_frame_container.pack(fill='both', expand=True)
        det_frame_container.pack_propagate(False)
        self.determinant_cb_frame_scrollable = ScrollableFrame(det_frame_container, relief="sunken", borderwidth=1)
        self.determinant_cb_frame_scrollable.pack(fill='both', expand=True)
        self.determinant_cb_frame = self.determinant_cb_frame_scrollable.interior

        ttk.Label(selection_vbox, text="Зависимая часть:").pack(anchor='w', pady=(10, 0))
        dep_frame_container = ttk.Frame(selection_vbox, width=180, height=140)
        dep_frame_container.pack(fill='both', expand=True)
        dep_frame_container.pack_propagate(False)
        self.dependent_cb_frame_scrollable = ScrollableFrame(dep_frame_container, relief="sunken", borderwidth=1)
        self.dependent_cb_frame_scrollable.pack(fill='both', expand=True)
        self.dependent_cb_frame = self.dependent_cb_frame_scrollable.interior

        action_vbox = ttk.Frame(main_hbox)
        action_vbox.pack(side='left', fill='y', padx=5, anchor='center')

        ttk.Button(action_vbox, text="→\nДобавить ФЗ", command=self.add_functional_dependency, width=12).pack(pady=5)
        ttk.Button(action_vbox, text="→→\nДобавить МЗД", command=self.add_multivalued_dependency, width=12).pack(pady=5)


        display_vbox = ttk.Frame(main_hbox)
        display_vbox.pack(side='left', fill='both', expand=True)

        fd_list_frame = ttk.LabelFrame(display_vbox, text="Функциональные зависимости (ФЗ)")
        fd_list_frame.pack(fill='both', expand=True, pady=(0, 5))
        self.fd_listbox = tk.Listbox(fd_list_frame, height=6)
        self.fd_listbox.pack(side='top', fill='both', expand=True, padx=2, pady=2)
        fd_buttons = ttk.Frame(fd_list_frame)
        fd_buttons.pack(fill='x', side='bottom')
        ttk.Button(fd_buttons, text="Удалить", command=self.remove_fd).pack(side='left', padx=5, pady=2)
        ttk.Button(fd_buttons, text="Очистить", command=self.clear_fds).pack(side='left', padx=5, pady=2)

        mvd_list_frame = ttk.LabelFrame(display_vbox, text="Многозначные зависимости (МЗД)")
        mvd_list_frame.pack(fill='both', expand=True, pady=(5, 0))
        self.mvd_listbox = tk.Listbox(mvd_list_frame, height=6)
        self.mvd_listbox.pack(side='top', fill='both', expand=True, padx=2, pady=2)
        mvd_buttons = ttk.Frame(mvd_list_frame)
        mvd_buttons.pack(fill='x', side='bottom')
        ttk.Button(mvd_buttons, text="Удалить", command=self.remove_mvd).pack(side='left', padx=5, pady=2)
        ttk.Button(mvd_buttons, text="Очистить", command=self.clear_mvds).pack(side='left', padx=5, pady=2)


    def create_analysis_widgets(self):
        control_frame = ttk.Frame(self.analysis_frame)
        control_frame.pack(fill='x', padx=5, pady=5)

        ttk.Button(control_frame, text="Выполнить анализ",
                   command=self.perform_analysis, style='Accent.TButton').pack(pady=10)

        self.analysis_text = scrolledtext.ScrolledText(self.analysis_frame, wrap=tk.WORD, height=25)
        self.analysis_text.pack(fill='both', expand=True, padx=5, pady=5)

    def create_normalization_widgets(self):
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
        export_frame = ttk.Frame(self.results_frame)
        export_frame.pack(fill='x', padx=5, pady=5)

        rows_frame = ttk.Frame(self.results_frame)
        rows_frame.pack(fill='x', padx=5, pady=(0,5))
        ttk.Label(rows_frame, text="Количество строк для теста:").pack(side='left', padx=(0,5))
        self.test_rows_var = tk.StringVar(value="1000")
        ttk.Entry(rows_frame, textvariable=self.test_rows_var, width=10).pack(side='left')
        # Кнопка «Тест декомпозиции» (уже у вас есть, но лучше переместить именно сюда):
        ttk.Button(rows_frame, text="Тест декомпозиции", command=self.run_decomposition_test).pack(side='left', padx=10)

        ttk.Button(export_frame, text="Тест производительности", command=self.run_performance_gui).pack(side='left',
                                                                                                        padx=5)
        ttk.Button(export_frame, text="Тест использования памяти", command=self.run_memory_test_gui).pack(side='left',
                                                                                                          padx=5)
        # ========== Блок ввода кредов для БД ==========
        creds_frame = ttk.LabelFrame(self.results_frame, text="Параметры подключения к БД", padding=10)
        creds_frame.pack(fill='x', padx=5, pady=(5, 0))

        # Host
        ttk.Label(creds_frame, text="Host:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        ttk.Entry(creds_frame, textvariable=self.db_host_var, width=15).grid(row=0, column=1, padx=5, pady=2)
        # Port
        ttk.Label(creds_frame, text="Port:").grid(row=0, column=2, sticky='w', padx=5, pady=2)
        ttk.Entry(creds_frame, textvariable=self.db_port_var, width=7).grid(row=0, column=3, padx=5, pady=2)
        # DB Name
        ttk.Label(creds_frame, text="DB Name:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        ttk.Entry(creds_frame, textvariable=self.db_name_var, width=15).grid(row=1, column=1, padx=5, pady=2)
        # User
        ttk.Label(creds_frame, text="User:").grid(row=1, column=2, sticky='w', padx=5, pady=2)
        ttk.Entry(creds_frame, textvariable=self.db_user_var, width=15).grid(row=1, column=3, padx=5, pady=2)
        # Password
        ttk.Label(creds_frame, text="Password:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        ttk.Entry(creds_frame, textvariable=self.db_password_var, show="*", width=15).grid(row=2, column=1, padx=5, pady=2)

        def on_default_db_params():
            self.db_host_var.set(self.default_db_params['host'])
            self.db_port_var.set(self.default_db_params['port'])
            self.db_name_var.set(self.default_db_params['dbname'])
            self.db_user_var.set(self.default_db_params['user'])
            self.db_password_var.set(self.default_db_params['password'])
        ttk.Button(creds_frame, text="По умолчанию", command=on_default_db_params).grid(
            row=2, column=3, padx=5, pady=2, sticky='e'
        )


        ttk.Button(export_frame, text="Экспорт в SQL",
                   command=self.export_to_sql).pack(side='left', padx=5)
        ttk.Button(export_frame, text="Сохранить отчет",
                   command=self.save_report).pack(side='left', padx=5)




        self.results_text = scrolledtext.ScrolledText(self.results_frame, wrap=tk.WORD)
        self.results_text.pack(fill='both', expand=True, padx=5, pady=5)

        if hasattr(self, 'create_visualization_button'):
            self.create_visualization_button()
        else:
            print("Warning: Visualization tools not loaded.")


    def _update_fd_attribute_checkboxes(self):
        """Обновляет чекбоксы для выбора атрибутов ФЗ и МЗД."""
        for widget in self.determinant_cb_frame.winfo_children():
            widget.destroy()
        for widget in self.dependent_cb_frame.winfo_children():
            widget.destroy()

        self.determinant_vars.clear()
        self.dependent_vars.clear()

        for attr in self.attributes:
            det_var = tk.BooleanVar()
            det_cb = ttk.Checkbutton(self.determinant_cb_frame, text=attr.name, variable=det_var)
            det_cb.pack(anchor='w', padx=2)
            self.determinant_vars.append((attr, det_var))
            self.determinant_cb_frame_scrollable.bind_child_for_scrolling(det_cb)

            dep_var = tk.BooleanVar()
            dep_cb = ttk.Checkbutton(self.dependent_cb_frame, text=attr.name, variable=dep_var)
            dep_cb.pack(anchor='w', padx=2)
            self.dependent_vars.append((attr, dep_var))
            self.dependent_cb_frame_scrollable.bind_child_for_scrolling(dep_cb)

        self.root.update_idletasks()
        self.determinant_cb_frame_scrollable.canvas.config(
            scrollregion=self.determinant_cb_frame_scrollable.canvas.bbox("all"))
        self.dependent_cb_frame_scrollable.canvas.config(
            scrollregion=self.dependent_cb_frame_scrollable.canvas.bbox("all"))

    def add_attribute(self):
        name = self.attr_name_var.get().strip()
        if not name:
            messagebox.showwarning("Ошибка", "Введите имя атрибута")
            return

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

        self._update_fd_attribute_checkboxes()

        self.attr_name_var.set("")
        self.is_pk_var.set(False)

    def remove_attribute(self):
        selection = self.attr_listbox.curselection()
        if not selection:
            return

        index = selection[0]
        attr_to_remove = self.attributes[index]

        for fd in self.functional_dependencies:
            if attr_to_remove in fd.determinant or attr_to_remove in fd.dependent:
                messagebox.showwarning("Ошибка",
                                       f"Атрибут '{attr_to_remove.name}' используется в функциональных зависимостях. Сначала удалите ФЗ.")
                return

        self.attributes.pop(index)
        self.attr_listbox.delete(index)

        self._update_fd_attribute_checkboxes()

    def clear_attributes(self):
        if self.functional_dependencies:
            messagebox.showwarning("Ошибка",
                                   "Сначала удалите все функциональные зависимости")
            return

        self.attributes.clear()
        self.attr_listbox.delete(0, tk.END)

        self._update_fd_attribute_checkboxes()


    def add_functional_dependency(self):
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

        if determinant_attrs == dependent_attrs:
             messagebox.showwarning("Ошибка", "Детерминант и зависимая часть не могут быть полностью идентичны для нетривиальной ФЗ.")
             return


        fd = FunctionalDependency(determinant_attrs, dependent_attrs)

        if any(fd.determinant == existing_fd.determinant and fd.dependent == existing_fd.dependent
               for existing_fd in self.functional_dependencies):
            messagebox.showwarning("Ошибка", "Такая функциональная зависимость уже существует.")
            return

        self.functional_dependencies.append(fd)
        self.fd_listbox.insert(tk.END, str(fd))

        for _, var in self.determinant_vars:
            var.set(False)
        for _, var in self.dependent_vars:
            var.set(False)

    def remove_fd(self):
        selection = self.fd_listbox.curselection()
        if not selection:
            return

        index = selection[0]
        self.functional_dependencies.pop(index)
        self.fd_listbox.delete(index)

    def clear_fds(self):
        self.functional_dependencies.clear()
        self.fd_listbox.delete(0, tk.END)

    def add_multivalued_dependency(self):
        det_attrs = {attr for attr, var in self.determinant_vars if var.get()}
        dep_attrs = {attr for attr, var in self.dependent_vars if var.get()}

        if not det_attrs or not dep_attrs:
            messagebox.showwarning("Ошибка", "Выберите атрибуты для детерминанта и зависимой части.")
            return

        if not det_attrs.isdisjoint(dep_attrs):
            messagebox.showwarning("Ошибка", "Детерминант и зависимая часть МЗД не должны содержать одинаковых атрибутов.")
            return

        mvd = MultivaluedDependency(det_attrs, dep_attrs)
        self.multivalued_dependencies.append(mvd)
        self.mvd_listbox.insert(tk.END, str(mvd))

        # Очистка выделения
        for _, var in self.determinant_vars:
            var.set(False)
        for _, var in self.dependent_vars:
            var.set(False)

    def remove_mvd(self):
        """Удаление выбранной МЗД"""
        selection = self.mvd_listbox.curselection()
        if not selection:
            return
        index = selection[0]
        self.multivalued_dependencies.pop(index)
        self.mvd_listbox.delete(index)

    def clear_mvds(self):
        """Очистка всех МЗД"""
        self.multivalued_dependencies.clear()
        self.mvd_listbox.delete(0, tk.END)

    def perform_analysis(self):
        """Выполнение анализа отношения"""
        if not self.attributes:
            messagebox.showwarning("Ошибка", "Добавьте атрибуты отношения")
            return

        # Создание отношения
        self.current_relation = Relation(
            name=self.relation_name_var.get(),
            attributes=self.attributes.copy(),
            functional_dependencies=self.functional_dependencies.copy(),
            # ИСПРАВЛЕНИЕ: Добавлена передача многозначных зависимостей
            multivalued_dependencies=self.multivalued_dependencies.copy()
        )

        # Анализ
        analyzer = NormalFormAnalyzer(self.current_relation)
        report = analyzer.get_analysis_report()

        # Дополнительная информация
        report += "\n" + "=" * 50 + "\n"
        report += "Дополнительный анализ:\n\n"

        # Минимальное покрытие
        minimal_cover = FDAlgorithms.minimal_cover(self.functional_dependencies)
        report += f"Минимальное покрытие ({len(minimal_cover)} ФЗ):\n"
        for fd in minimal_cover:
            report += f"  - {fd}\n"

        # Отображение результатов
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
            messagebox.showinfo("Успешно", f"SQL экспортирован в {filename}")\


    def run_decomposition_test(self):
        """Запустить тест на без­потерьность декомпозиции и вывести лог во всплывающем окне."""
        if not self.normalization_result:
            messagebox.showwarning("Ошибка", "Сначала выполните нормализацию.")
            return

        # Считаем, сколько строк нужно сгенерировать
        try:
            num_rows = int(self.test_rows_var.get())
            if num_rows <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Неверный ввод", "Количество строк должно быть положительным целым числом.")
            return

        orig_rel = self.normalization_result.original_relation
        normalized = self.normalization_result.decomposed_relations

        # Перехватим весь stdout, который печатает test_decomposition
        buf = io.StringIO()
        try:
            with redirect_stdout(buf):
                test_decomposition(orig_rel, normalized, num_rows)
        except Exception as e:
            buf.write("\n[ERROR] При выполнении test_decomposition произошла ошибка:\n")
            buf.write(str(e))
        log = buf.getvalue()

        # Показать результат во всплывающем окне с прокруткой
        popup = tk.Toplevel(self.root)
        popup.title("Результат теста декомпозиции")
        popup.geometry("700x500")

        lbl = ttk.Label(popup, text=f"Лог теста (строк: {num_rows}):", font=("Arial", 10, "bold"))
        lbl.pack(anchor='nw', padx=5, pady=(5, 0))

        txt = scrolledtext.ScrolledText(popup, wrap=tk.NONE)
        txt.pack(fill='both', expand=True, padx=5, pady=5)
        txt.insert("1.0", log)
        txt.configure(state="disabled")

        ttk.Button(popup, text="Закрыть", command=popup.destroy).pack(pady=(0,5))

    def run_performance_gui(self):
        """Запустить измерение производительности SELECT-запросов."""
        if not self.normalization_result:
            messagebox.showwarning("Ошибка", "Сначала выполните нормализацию.")
            return

        # Считать, сколько строк генерировать
        try:
            num_rows = int(self.test_rows_var.get())
            if num_rows <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Неверный ввод", "Количество строк должно быть положительным целым числом.")
            return

        orig_rel = self.normalization_result.original_relation

        # Создаем прогресс-диалог
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Выполнение теста производительности")
        progress_window.geometry("400x150")
        progress_window.transient(self.root)
        progress_window.grab_set()

        ttk.Label(progress_window, text="Выполняется тест производительности запросов...",
                  font=("Arial", 10)).pack(pady=20)
        progress_bar = ttk.Progressbar(progress_window, mode='indeterminate', length=300)
        progress_bar.pack(pady=10)
        progress_bar.start(10)

        # Запускаем тест в отдельном потоке
        import threading
        results = {}
        error = None

        def run_test():
            nonlocal results, error
            try:
                # Вызываем обновленный run_performance_test
                results = run_performance_test(orig_rel, num_rows=num_rows, repeats=10)
            except Exception as e:
                error = str(e)
                import traceback
                traceback.print_exc()

        # Запускаем тест
        test_thread = threading.Thread(target=run_test)
        test_thread.start()

        # Ждем завершения
        def check_completion():
            if test_thread.is_alive():
                progress_window.after(100, check_completion)
            else:
                progress_bar.stop()
                progress_window.destroy()

                if error:
                    messagebox.showerror("Ошибка", f"Ошибка при выполнении теста производительности:\n{error}")
                elif results:
                    # Показываем результаты в отдельном окне
                    self.show_performance_results(results)
                    # Строим гистограммы вместо графиков
                    try:
                        from performance_test import plot_performance_histogram
                        plot_performance_histogram(results)
                    except Exception as e:
                        print(f"Ошибка при построении графиков: {e}")
                else:
                    messagebox.showwarning("Ошибка", "Не удалось получить результаты теста")

        progress_window.after(100, check_completion)

    def show_performance_results(self, results: Dict[str, Dict[str, float]]):
        """Показать результаты теста производительности в отдельном окне"""
        result_window = tk.Toplevel(self.root)
        result_window.title("Результаты теста производительности")
        result_window.geometry("900x700")

        # Создаем текстовое поле с прокруткой
        text_frame = ttk.Frame(result_window)
        text_frame.pack(fill='both', expand=True, padx=5, pady=5)

        text_widget = scrolledtext.ScrolledText(text_frame, wrap=tk.WORD, font=("Courier", 9))
        text_widget.pack(fill='both', expand=True)

        # Формируем отчет
        report = "РЕЗУЛЬТАТЫ ТЕСТА ПРОИЗВОДИТЕЛЬНОСТИ ЗАПРОСОВ\n"
        report += "=" * 80 + "\n\n"

        # Описания типов запросов
        query_descriptions = {
            'full_scan': 'Полное сканирование таблицы',
            'pk_lookup': 'Поиск записи по первичному ключу',
            'aggregation': 'Агрегация данных (COUNT)',
            'group_by': 'Группировка данных',
            'filter_non_indexed': 'Фильтрация по неиндексированному полю',
            'full_join': 'Полное соединение всех таблиц',
            'join_with_filter': 'JOIN с условием фильтрации',
            'single_table': 'Запрос к одной таблице',
            'join_aggregation': 'Агрегация после JOIN',
            'subquery': 'Запрос с подзапросом'
        }

        # Сводная таблица
        report += "Сводная таблица времени выполнения (мс):\n"
        report += "-" * 80 + "\n"

        # Собираем все типы запросов
        all_query_types = set()
        for level_queries in results.values():
            all_query_types.update(level_queries.keys())
        all_query_types = sorted(list(all_query_types))

        # Заголовок таблицы
        header = f"{'Тип запроса':<30}"
        for level in results.keys():
            header += f" | {level:>12}"
        report += header + "\n"
        report += "-" * 80 + "\n"

        # Данные таблицы
        for qt in all_query_types:
            desc = query_descriptions.get(qt, qt)
            row = f"{desc:<30}"

            for level in results.keys():
                if qt in results[level]:
                    time_ms = results[level][qt] * 1000
                    row += f" | {time_ms:>10.2f} "
                else:
                    row += f" | {'—':>12}"

            report += row + "\n"

        report += "-" * 80 + "\n\n"

        # Анализ результатов
        report += "АНАЛИЗ РЕЗУЛЬТАТОВ:\n"
        report += "=" * 80 + "\n\n"

        # Находим самые быстрые и медленные запросы для каждого уровня
        for level in results.keys():
            report += f"{level}:\n"

            if results[level]:
                # Сортируем по времени
                sorted_queries = sorted(results[level].items(), key=lambda x: x[1])

                # Самый быстрый
                fastest_query, fastest_time = sorted_queries[0]
                report += f"  ✓ Самый быстрый: {query_descriptions.get(fastest_query, fastest_query)} "
                report += f"({fastest_time * 1000:.2f} мс)\n"

                # Самый медленный
                slowest_query, slowest_time = sorted_queries[-1]
                report += f"  ✗ Самый медленный: {query_descriptions.get(slowest_query, slowest_query)} "
                report += f"({slowest_time * 1000:.2f} мс)\n"

                # Среднее время
                avg_time = sum(results[level].values()) / len(results[level])
                report += f"  • Среднее время: {avg_time * 1000:.2f} мс\n"

            report += "\n"

        # Выводы
        report += "ВЫВОДЫ:\n"
        report += "=" * 80 + "\n"

        # Сравнение JOIN запросов
        join_times = {}
        for level, queries in results.items():
            for qt, time in queries.items():
                if 'join' in qt:
                    if level not in join_times:
                        join_times[level] = []
                    join_times[level].append(time)

        if join_times:
            report += "\n1. Производительность JOIN-запросов:\n"
            for level, times in join_times.items():
                avg_join_time = sum(times) / len(times) if times else 0
                report += f"   - {level}: среднее время JOIN = {avg_join_time * 1000:.2f} мс\n"

        # Влияние нормализации на простые запросы
        simple_query_impact = {}
        for qt in ['full_scan', 'pk_lookup', 'single_table']:
            times_by_level = {}
            for level, queries in results.items():
                if qt in queries:
                    times_by_level[level] = queries[qt]

            if len(times_by_level) > 1:
                simple_query_impact[qt] = times_by_level

        if simple_query_impact:
            report += "\n2. Влияние нормализации на простые запросы:\n"
            for qt, times in simple_query_impact.items():
                report += f"   - {query_descriptions.get(qt, qt)}:\n"
                base_time = list(times.values())[0]
                for level, time in times.items():
                    change = ((time - base_time) / base_time * 100) if base_time > 0 else 0
                    report += f"     • {level}: {time * 1000:.2f} мс"
                    if change != 0:
                        report += f" ({change:+.1f}%)"
                    report += "\n"

        # Вставляем отчет в текстовое поле
        text_widget.insert('1.0', report)
        self.make_text_readonly_but_copyable(text_widget)

        # Кнопки
        button_frame = ttk.Frame(result_window)
        button_frame.pack(fill='x', padx=5, pady=5)

        def copy_all():
            text_widget.tag_add("sel", "1.0", "end")
            text_widget.event_generate("<<Copy>>")
            copy_label = ttk.Label(button_frame, text="Скопировано!", foreground="green")
            copy_label.pack(side='left', padx=10)
            result_window.after(2000, copy_label.destroy)

        ttk.Button(button_frame, text="Копировать отчет", command=copy_all).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Закрыть", command=result_window.destroy).pack(side='right', padx=5)


    def run_memory_test_gui(self):
        """Запустить тест использования памяти"""
        if not self.normalization_result:
            messagebox.showwarning("Ошибка", "Сначала выполните нормализацию.")
            return

        # Считываем количество строк
        try:
            num_rows = int(self.test_rows_var.get())
            if num_rows <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Неверный ввод", "Количество строк должно быть положительным целым числом.")
            return

        # Проверяем параметры подключения к БД
        if not all([self.db_host_var.get(), self.db_port_var.get(),
                    self.db_name_var.get(), self.db_user_var.get()]):
            # Используем дефолтные параметры
            self.db_host_var.set(self.default_db_params['host'])
            self.db_port_var.set(self.default_db_params['port'])
            self.db_name_var.set(self.default_db_params['dbname'])
            self.db_user_var.set(self.default_db_params['user'])
            self.db_password_var.set(self.default_db_params['password'])

        # Обновляем параметры подключения
        import data_test
        data_test.DB_PARAMS = {
            'host': self.db_host_var.get(),
            'port': self.db_port_var.get(),
            'dbname': self.db_name_var.get(),
            'user': self.db_user_var.get(),
            'password': self.db_password_var.get()
        }

        # Также обновляем для memory_test
        import memory_test
        memory_test.connect = data_test.connect

        orig_rel = self.normalization_result.original_relation

        # Создаем прогресс-диалог
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Выполнение теста памяти")
        progress_window.geometry("400x150")
        progress_window.transient(self.root)
        progress_window.grab_set()

        ttk.Label(progress_window, text="Выполняется тест использования памяти...",
                  font=("Arial", 10)).pack(pady=20)
        progress_bar = ttk.Progressbar(progress_window, mode='indeterminate', length=300)
        progress_bar.pack(pady=10)
        progress_bar.start(10)

        status_label = ttk.Label(progress_window, text="Подготовка...")
        status_label.pack(pady=5)

        # Запускаем тест в отдельном потоке
        import threading
        results = {}
        error = None

        def run_test():
            nonlocal results, error
            try:
                import io
                from contextlib import redirect_stdout

                # Перехватываем вывод для отображения в статусе
                buf = io.StringIO()
                with redirect_stdout(buf):
                    results = run_memory_test(orig_rel, num_rows)

                # Обновляем статус периодически
                log_lines = buf.getvalue().split('\n')
                for line in log_lines:
                    if line.strip():
                        progress_window.after(0, lambda l=line: status_label.config(text=l[:50] + "..."))

            except Exception as e:
                error = str(e)
                import traceback
                traceback.print_exc()

        # Запускаем тест
        test_thread = threading.Thread(target=run_test)
        test_thread.start()

        # Ждем завершения
        def check_completion():
            if test_thread.is_alive():
                progress_window.after(100, check_completion)
            else:
                progress_bar.stop()
                progress_window.destroy()

                if error:
                    messagebox.showerror("Ошибка", f"Ошибка при выполнении теста памяти:\n{error}")
                elif results:
                    # Показываем результаты
                    self.show_memory_test_results(results)
                    # Строим графики
                    try:
                        plot_memory_usage(results)
                    except Exception as e:
                        print(f"Ошибка при построении графиков: {e}")
                else:
                    messagebox.showwarning("Ошибка", "Не удалось получить результаты теста")

        progress_window.after(100, check_completion)

    def show_memory_test_results(self, results: Dict[str, Dict[str, Dict[str, int]]]):
        """Показать результаты теста памяти в отдельном окне"""
        result_window = tk.Toplevel(self.root)
        result_window.title("Результаты теста памяти")
        result_window.geometry("800x600")

        # Создаем текстовое поле с прокруткой
        text_frame = ttk.Frame(result_window)
        text_frame.pack(fill='both', expand=True, padx=5, pady=5)

        text_widget = scrolledtext.ScrolledText(text_frame, wrap=tk.WORD, font=("Courier", 9))
        text_widget.pack(fill='both', expand=True)

        # Формируем отчет
        report = "РЕЗУЛЬТАТЫ ТЕСТА ИСПОЛЬЗОВАНИЯ ПАМЯТИ\n"
        report += "=" * 70 + "\n\n"

        # Сводная таблица
        report += "Сводная таблица размеров (в КБ):\n"
        report += "-" * 70 + "\n"
        report += f"{'Уровень':<10} | {'Без индексов':>15} | {'С индексами':>15} | {'Индексы':>12} | {'Изменение':>10}\n"
        report += "-" * 70 + "\n"

        original_size = results.get("Original", {}).get("with_indexes", {}).get("total_size", 1)

        for level in ["Original", "2NF", "3NF", "BCNF", "4NF"]:
            if level in results:
                size_no_idx = results[level]["without_indexes"]["total_size"] / 1024
                size_with_idx = results[level]["with_indexes"]["total_size"] / 1024
                idx_size = results[level]["with_indexes"]["indexes_size"] / 1024

                if level == "Original":
                    change = "базовый"
                else:
                    change_pct = ((size_with_idx * 1024 - original_size) / original_size) * 100
                    change = f"{change_pct:+.1f}%"

                report += f"{level:<10} | {size_no_idx:>15.2f} | {size_with_idx:>15.2f} | {idx_size:>12.2f} | {change:>10}\n"

        report += "-" * 70 + "\n\n"

        # Детальная информация
        report += "Детальная информация:\n"
        report += "=" * 70 + "\n\n"

        for level, level_data in results.items():
            report += f"{level}:\n"
            report += "-" * 30 + "\n"

            without_idx = level_data["without_indexes"]
            with_idx = level_data["with_indexes"]

            report += f"  Без индексов:\n"
            report += f"    - Размер таблиц: {without_idx['table_size'] / 1024:.2f} КБ\n"
            report += f"    - Общий размер: {without_idx['total_size'] / 1024:.2f} КБ\n"
            report += f"    - Количество строк: {without_idx['row_count']}\n"

            report += f"  С индексами:\n"
            report += f"    - Размер таблиц: {with_idx['table_size'] / 1024:.2f} КБ\n"
            report += f"    - Размер индексов: {with_idx['indexes_size'] / 1024:.2f} КБ\n"
            report += f"    - Общий размер: {with_idx['total_size'] / 1024:.2f} КБ\n"

            # Эффективность хранения
            if without_idx['row_count'] > 0:
                bytes_per_row = with_idx['total_size'] / without_idx['row_count']
                report += f"    - Байт на строку: {bytes_per_row:.2f}\n"

            report += "\n"

        # Выводы
        report += "ВЫВОДЫ:\n"
        report += "=" * 70 + "\n"

        # Находим самый экономный уровень
        min_size = float('inf')
        min_level = ""
        max_size = 0
        max_level = ""

        for level, level_data in results.items():
            size = level_data["with_indexes"]["total_size"]
            if size < min_size:
                min_size = size
                min_level = level
            if size > max_size:
                max_size = size
                max_level = level

        if min_level and max_level:
            saving_pct = ((max_size - min_size) / max_size) * 100
            report += f"- Наиболее экономный уровень: {min_level} ({min_size / 1024:.2f} КБ)\n"
            report += f"- Наиболее затратный уровень: {max_level} ({max_size / 1024:.2f} КБ)\n"
            report += f"- Потенциальная экономия: {saving_pct:.1f}%\n"

        # Вставляем отчет в текстовое поле
        text_widget.insert('1.0', report)
        text_widget.configure(state='disabled')

        # Кнопка закрытия
        ttk.Button(result_window, text="Закрыть", command=result_window.destroy).pack(pady=5)


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
            self.clear_attributes()
            self.clear_fds()
            self.clear_mvds()  # очистка МЗД
            self.relation_name_var.set("Отношение1")
            self.analysis_text.delete(1.0, tk.END)
            self.normalization_text.delete(1.0, tk.END)
            self.results_text.delete(1.0, tk.END)
            self.current_relation = None
            self.normalization_result = None
            # Сброс полей подключений
            self.db_host_var.set("")
            self.db_port_var.set("")
            self.db_name_var.set("")
            self.db_user_var.set("")
            self.db_password_var.set("")



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