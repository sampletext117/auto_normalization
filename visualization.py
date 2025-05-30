"""
Модуль визуализации для отображения схем отношений и результатов декомпозиции
"""
import tkinter as tk
from tkinter import ttk, Canvas
import math
from typing import List, Dict, Tuple, Set
from models import Relation, FunctionalDependency, Attribute, NormalizationResult


class RelationDiagram:
    """Класс для визуализации диаграмм отношений"""

    def __init__(self, canvas: Canvas):
        self.canvas = canvas
        self.relations: Dict[str, Dict] = {}  # {name: {x, y, width, height, relation}}
        self.arrows: List[Dict] = []  # Стрелки между отношениями

    def clear(self):
        """Очистка холста"""
        self.canvas.delete("all")
        self.relations.clear()
        self.arrows.clear()

    def draw_relation(self, relation: Relation, x: int, y: int) -> Tuple[int, int]:
        """
        Рисование отношения на холсте

        Returns:
            (width, height) нарисованного отношения
        """
        # Параметры отрисовки
        padding = 10
        line_height = 20
        min_width = 150

        # Вычисление размеров
        title_width = len(relation.name) * 8 + 2 * padding

        # Ширина для атрибутов
        max_attr_width = 0
        for attr in relation.attributes:
            attr_text = f"{'• ' if attr.is_primary_key else '  '}{attr.name}"
            attr_width = len(attr_text) * 7
            max_attr_width = max(max_attr_width, attr_width)

        width = max(min_width, title_width, max_attr_width + 2 * padding)
        height = (len(relation.attributes) + 1) * line_height + 2 * padding

        # Рисование рамки
        rect = self.canvas.create_rectangle(
            x, y, x + width, y + height,
            fill="lightblue", outline="black", width=2
        )

        # Заголовок
        title_y = y + padding
        self.canvas.create_text(
            x + width // 2, title_y,
            text=relation.name, font=("Arial", 10, "bold"),
            anchor="n"
        )

        # Линия под заголовком
        line_y = y + line_height + padding
        self.canvas.create_line(
            x + padding, line_y, x + width - padding, line_y,
            fill="black"
        )

        # Атрибуты
        attr_y = line_y + 5
        for attr in relation.attributes:
            if attr.is_primary_key:
                # Подчеркивание для первичного ключа
                self.canvas.create_text(
                    x + padding, attr_y,
                    text=f"• {attr.name}", font=("Arial", 9, "underline"),
                    anchor="nw"
                )
            else:
                self.canvas.create_text(
                    x + padding + 10, attr_y,
                    text=attr.name, font=("Arial", 9),
                    anchor="nw"
                )
            attr_y += line_height

        # Сохранение информации
        self.relations[relation.name] = {
            'x': x, 'y': y, 'width': width, 'height': height,
            'relation': relation
        }

        return width, height

    def draw_functional_dependencies(self, relation: Relation, x: int, y: int, width: int, height: int):
        """Рисование функциональных зависимостей внутри отношения"""
        if not relation.functional_dependencies:
            return

        # Область для ФЗ
        fd_x = x + width + 20
        fd_y = y

        self.canvas.create_text(
            fd_x, fd_y,
            text="Функциональные зависимости:",
            font=("Arial", 9, "bold"),
            anchor="nw"
        )

        fd_y += 20
        for fd in relation.functional_dependencies[:5]:  # Показываем первые 5
            fd_text = str(fd)
            if len(fd_text) > 30:
                fd_text = fd_text[:27] + "..."
            self.canvas.create_text(
                fd_x, fd_y,
                text=f"• {fd_text}",
                font=("Arial", 8),
                anchor="nw"
            )
            fd_y += 15

        if len(relation.functional_dependencies) > 5:
            self.canvas.create_text(
                fd_x, fd_y,
                text=f"... и еще {len(relation.functional_dependencies) - 5}",
                font=("Arial", 8, "italic"),
                anchor="nw"
            )

    def draw_decomposition_arrow(self, from_rel: str, to_rels: List[str], label: str = ""):
        """Рисование стрелки декомпозиции"""
        if from_rel not in self.relations:
            return

        from_info = self.relations[from_rel]
        from_x = from_info['x'] + from_info['width'] // 2
        from_y = from_info['y'] + from_info['height']

        # Найти центр целевых отношений
        to_coords = []
        for to_rel in to_rels:
            if to_rel in self.relations:
                to_info = self.relations[to_rel]
                to_x = to_info['x'] + to_info['width'] // 2
                to_y = to_info['y']
                to_coords.append((to_x, to_y))

        if not to_coords:
            return

        # Средняя точка
        mid_x = sum(x for x, y in to_coords) // len(to_coords)
        mid_y = from_y + 30

        # Рисование стрелок
        for to_x, to_y in to_coords:
            # Линия от исходного к промежуточной точке
            self.canvas.create_line(
                from_x, from_y, mid_x, mid_y,
                fill="red", width=2
            )
            # Линия от промежуточной к целевой
            self.canvas.create_line(
                mid_x, mid_y, to_x, to_y,
                fill="red", width=2, arrow=tk.LAST
            )

        # Подпись
        if label:
            self.canvas.create_text(
                mid_x, mid_y - 10,
                text=label, font=("Arial", 8),
                fill="red"
            )

    def auto_layout_relations(self, relations: List[Relation]):
        """Автоматическое размещение отношений"""
        self.clear()

        if not relations:
            return

        # Параметры размещения
        margin = 50
        h_spacing = 200
        v_spacing = 150

        # Размещение в сетке
        cols = math.ceil(math.sqrt(len(relations)))

        for i, relation in enumerate(relations):
            row = i // cols
            col = i % cols

            x = margin + col * h_spacing
            y = margin + row * v_spacing

            width, height = self.draw_relation(relation, x, y)

            # ФЗ для небольших отношений
            if len(relation.attributes) <= 5:
                self.draw_functional_dependencies(relation, x, y, width, height)

    def draw_normalization_result(self, result: NormalizationResult):
        """Визуализация результатов нормализации"""
        self.clear()

        # Исходное отношение сверху
        orig_width, orig_height = self.draw_relation(result.original_relation, 300, 50)

        # Результирующие отношения снизу
        self.auto_layout_relations(result.decomposed_relations)

        # Переместить результирующие отношения ниже
        for rel_name in self.relations:
            if rel_name != result.original_relation.name:
                self.relations[rel_name]['y'] += 200

        # Перерисовать с новыми координатами
        temp_relations = self.relations.copy()
        self.clear()

        # Рисуем исходное отношение
        self.draw_relation(result.original_relation, 300, 50)

        # Рисуем результирующие
        for rel_name, info in temp_relations.items():
            if rel_name != result.original_relation.name:
                self.draw_relation(info['relation'], info['x'], info['y'] + 200)

        # Стрелки декомпозиции
        if result.decomposed_relations:
            to_rels = [r.name for r in result.decomposed_relations]
            self.draw_decomposition_arrow(
                result.original_relation.name,
                to_rels,
                f"{result.original_form.value} → {result.target_form.value}"
            )


class VisualizationWindow:
    """Окно для визуализации схем"""

    def __init__(self, parent, normalization_result: NormalizationResult = None):
        self.window = tk.Toplevel(parent)
        self.window.title("Визуализация схемы отношений")
        self.window.geometry("1000x700")

        # Панель управления
        control_frame = ttk.Frame(self.window)
        control_frame.pack(fill='x', padx=5, pady=5)

        ttk.Button(control_frame, text="Увеличить", command=self.zoom_in).pack(side='left', padx=5)
        ttk.Button(control_frame, text="Уменьшить", command=self.zoom_out).pack(side='left', padx=5)
        ttk.Button(control_frame, text="Сбросить масштаб", command=self.reset_zoom).pack(side='left', padx=5)
        ttk.Button(control_frame, text="Экспорт в PNG", command=self.export_to_png).pack(side='left', padx=5)

        # Холст с прокруткой
        canvas_frame = ttk.Frame(self.window)
        canvas_frame.pack(fill='both', expand=True, padx=5, pady=5)

        self.canvas = Canvas(canvas_frame, bg="white", width=900, height=600)
        h_scrollbar = ttk.Scrollbar(canvas_frame, orient='horizontal', command=self.canvas.xview)
        v_scrollbar = ttk.Scrollbar(canvas_frame, orient='vertical', command=self.canvas.yview)

        self.canvas.configure(xscrollcommand=h_scrollbar.set, yscrollcommand=v_scrollbar.set)

        self.canvas.grid(row=0, column=0, sticky='nsew')
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')

        canvas_frame.grid_rowconfigure(0, weight=1)
        canvas_frame.grid_columnconfigure(0, weight=1)

        # Диаграмма
        self.diagram = RelationDiagram(self.canvas)
        self.scale = 1.0

        # Отображение результатов
        if normalization_result:
            self.diagram.draw_normalization_result(normalization_result)
            self.update_scroll_region()

        # Привязка событий мыши для перемещения
        self.canvas.bind("<ButtonPress-1>", self.on_canvas_click)
        self.canvas.bind("<B1-Motion>", self.on_canvas_drag)

    def update_scroll_region(self):
        """Обновление области прокрутки"""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def zoom_in(self):
        """Увеличение масштаба"""
        self.scale *= 1.2
        self.canvas.scale("all", 0, 0, 1.2, 1.2)
        self.update_scroll_region()

    def zoom_out(self):
        """Уменьшение масштаба"""
        self.scale *= 0.8
        self.canvas.scale("all", 0, 0, 0.8, 0.8)
        self.update_scroll_region()

    def reset_zoom(self):
        """Сброс масштаба"""
        self.canvas.scale("all", 0, 0, 1 / self.scale, 1 / self.scale)
        self.scale = 1.0
        self.update_scroll_region()

    def on_canvas_click(self, event):
        """Обработка клика на холсте"""
        self.canvas.scan_mark(event.x, event.y)

    def on_canvas_drag(self, event):
        """Обработка перетаскивания"""
        self.canvas.scan_dragto(event.x, event.y, gain=1)

    def export_to_png(self):
        """Экспорт в PNG (требует дополнительных библиотек)"""
        try:
            from PIL import Image, ImageDraw
            import tkinter.filedialog as filedialog

            # Получение размеров
            bbox = self.canvas.bbox("all")
            if not bbox:
                return

            width = int(bbox[2] - bbox[0])
            height = int(bbox[3] - bbox[1])

            # Создание изображения
            img = Image.new('RGB', (width, height), 'white')

            # Здесь должен быть код для отрисовки содержимого холста в изображение
            # Это требует более сложной реализации

            filename = filedialog.asksaveasfilename(
                defaultextension=".png",
                filetypes=[("PNG files", "*.png"), ("All files", "*.*")]
            )

            if filename:
                img.save(filename)
                messagebox.showinfo("Успешно", f"Изображение сохранено в {filename}")

        except ImportError:
            messagebox.showwarning("Ошибка",
                                   "Для экспорта в PNG требуется установить библиотеку Pillow:\n"
                                   "pip install Pillow")


def add_visualization_to_gui(gui_class):
    """Добавление функций визуализации в основной GUI"""

    def show_visualization(self):
        """Показать окно визуализации"""
        if hasattr(self, 'normalization_result') and self.normalization_result:
            VisualizationWindow(self.root, self.normalization_result)
        else:
            messagebox.showwarning("Ошибка", "Сначала выполните нормализацию")

    # Добавляем метод в класс
    gui_class.show_visualization = show_visualization

    # Добавляем кнопку в интерфейс
    def create_visualization_button(self):
        """Добавление кнопки визуализации"""
        # Находим фрейм с результатами
        for widget in self.results_frame.winfo_children():
            if isinstance(widget, ttk.Frame):
                ttk.Button(widget, text="Визуализация схемы",
                           command=self.show_visualization).pack(side='left', padx=5)
                break

    gui_class.create_visualization_button = create_visualization_button