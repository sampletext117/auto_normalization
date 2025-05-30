"""
Модуль с классами для представления данных реляционной модели
"""
from typing import List, Set, Tuple, Optional
from dataclasses import dataclass, field
from enum import Enum


class NormalForm(Enum):
    """Перечисление нормальных форм"""
    UNNORMALIZED = "Ненормализованная"
    FIRST_NF = "1НФ"
    SECOND_NF = "2НФ"
    THIRD_NF = "3НФ"
    BCNF = "НФБК"
    FOURTH_NF = "4НФ"


@dataclass
class Attribute:
    """Класс для представления атрибута отношения"""
    name: str
    data_type: str = "VARCHAR"
    is_primary_key: bool = False

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, Attribute):
            return self.name == other.name
        return False

    def __repr__(self):
        return f"{self.name}"


@dataclass
class FunctionalDependency:
    """Класс для представления функциональной зависимости"""
    determinant: Set[Attribute]
    dependent: Set[Attribute]

    def __repr__(self):
        det_str = ", ".join([attr.name for attr in self.determinant])
        dep_str = ", ".join([attr.name for attr in self.dependent])
        return f"{{{det_str}}} → {{{dep_str}}}"

    def is_trivial(self) -> bool:
        """Проверка, является ли ФЗ тривиальной"""
        return self.dependent.issubset(self.determinant)

    def is_partial(self, key: Set[Attribute]) -> bool:
        """Проверка, является ли ФЗ частичной относительно ключа"""
        if not self.determinant.issubset(key):
            return False
        # Частичная, если детерминант - собственное подмножество ключа
        return self.determinant != key and len(self.determinant) < len(key)


@dataclass
class MultivaluedDependency:
    """Класс для представления многозначной зависимости (для 4НФ)"""
    determinant: Set[Attribute]
    dependent: Set[Attribute]

    def __repr__(self):
        det_str = ", ".join([attr.name for attr in self.determinant])
        dep_str = ", ".join([attr.name for attr in self.dependent])
        return f"{{{det_str}}} →→ {{{dep_str}}}"


@dataclass
class Relation:
    """Класс для представления отношения"""
    name: str
    attributes: List[Attribute] = field(default_factory=list)
    functional_dependencies: List[FunctionalDependency] = field(default_factory=list)
    multivalued_dependencies: List[MultivaluedDependency] = field(default_factory=list)

    def get_attribute_by_name(self, name: str) -> Optional[Attribute]:
        """Получить атрибут по имени"""
        for attr in self.attributes:
            if attr.name == name:
                return attr
        return None

    def get_primary_key(self) -> Set[Attribute]:
        """Получить первичный ключ"""
        return {attr for attr in self.attributes if attr.is_primary_key}

    def get_all_attributes_set(self) -> Set[Attribute]:
        """Получить все атрибуты как множество"""
        return set(self.attributes)

    def __repr__(self):
        attrs_str = ", ".join([attr.name for attr in self.attributes])
        return f"{self.name}({attrs_str})"


@dataclass
class DecompositionStep:
    """Класс для представления шага декомпозиции"""
    original_relation: Relation
    resulting_relations: List[Relation]
    reason: str
    violated_dependency: Optional[FunctionalDependency] = None

    def __repr__(self):
        result_str = ", ".join([rel.name for rel in self.resulting_relations])
        return f"Декомпозиция {self.original_relation.name} → [{result_str}]: {self.reason}"


@dataclass
class NormalizationResult:
    """Класс для представления результата нормализации"""
    original_form: NormalForm
    target_form: NormalForm
    original_relation: Relation
    decomposed_relations: List[Relation]
    steps: List[DecompositionStep]
    preserved_dependencies: List[FunctionalDependency]
    lost_dependencies: List[FunctionalDependency]

    def is_lossless(self) -> bool:
        """Проверка декомпозиции без потерь"""
        # Упрощенная проверка - должна быть реализована полностью
        return len(self.lost_dependencies) == 0

    def get_summary(self) -> str:
        """Получить краткое описание результата"""
        summary = f"Нормализация из {self.original_form.value} в {self.target_form.value}\n"
        summary += f"Исходное отношение: {self.original_relation}\n"
        summary += f"Результирующие отношения: {len(self.decomposed_relations)}\n"
        for rel in self.decomposed_relations:
            summary += f"  - {rel}\n"
        if self.lost_dependencies:
            summary += f"Потерянные зависимости: {len(self.lost_dependencies)}\n"
        return summary