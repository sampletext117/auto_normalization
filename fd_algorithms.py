"""
Алгоритмы для работы с функциональными зависимостями
"""
from typing import Set, List, Tuple
from models import Attribute, FunctionalDependency, Relation


class FDAlgorithms:
    """Класс с алгоритмами для работы с функциональными зависимостями"""

    @staticmethod
    def closure(attributes: Set[Attribute], fds: List[FunctionalDependency]) -> Set[Attribute]:
        """
        Вычисление замыкания множества атрибутов

        Args:
            attributes: Множество атрибутов
            fds: Список функциональных зависимостей

        Returns:
            Замыкание множества атрибутов
        """
        closure = attributes.copy()
        changed = True

        while changed:
            changed = False
            for fd in fds:
                # Если детерминант ФЗ содержится в замыкании
                if fd.determinant.issubset(closure):
                    # Добавляем зависимые атрибуты
                    new_attrs = fd.dependent - closure
                    if new_attrs:
                        closure.update(new_attrs)
                        changed = True

        return closure

    @staticmethod
    def is_superkey(attributes: Set[Attribute], relation: Relation) -> bool:
        """
        Проверка, является ли множество атрибутов суперключом

        Args:
            attributes: Множество атрибутов
            relation: Отношение

        Returns:
            True, если атрибуты образуют суперключ
        """
        closure = FDAlgorithms.closure(attributes, relation.functional_dependencies)
        return closure == relation.get_all_attributes_set()

    @staticmethod
    def find_all_keys(relation: Relation) -> List[Set[Attribute]]:
        """
        Найти все ключи отношения

        Args:
            relation: Отношение

        Returns:
            Список всех минимальных ключей
        """
        all_attrs = relation.get_all_attributes_set()
        keys = []

        # Проверяем все возможные комбинации атрибутов
        from itertools import combinations

        for r in range(1, len(all_attrs) + 1):
            for combo in combinations(all_attrs, r):
                attr_set = set(combo)
                if FDAlgorithms.is_superkey(attr_set, relation):
                    # Проверяем, что это минимальный ключ
                    is_minimal = True
                    for existing_key in keys:
                        if existing_key.issubset(attr_set):
                            is_minimal = False
                            break
                    if is_minimal:
                        # Удаляем ключи, которые являются надмножествами найденного
                        keys = [k for k in keys if not attr_set.issubset(k)]
                        keys.append(attr_set)

        return keys if keys else [all_attrs]

    @staticmethod
    def find_candidate_keys(relation: Relation) -> List[Set[Attribute]]:
        """
        Найти все потенциальные ключи отношения
        Более эффективный алгоритм
        """
        all_attrs = relation.get_all_attributes_set()
        fds = relation.functional_dependencies

        # Найдем атрибуты, которые появляются только слева, только справа, и с обеих сторон
        left_only = set()
        right_only = set()
        both_sides = set()

        for fd in fds:
            left_only.update(fd.determinant)
            right_only.update(fd.dependent)

        both_sides = left_only & right_only
        left_only = left_only - both_sides
        right_only = right_only - both_sides

        # Атрибуты, которые не участвуют в ФЗ
        not_in_fds = all_attrs - left_only - right_only - both_sides

        # Атрибуты, которые должны быть в каждом ключе
        must_have = left_only | not_in_fds

        # Если must_have уже является ключом
        if FDAlgorithms.is_superkey(must_have, relation):
            return [must_have]

        # Иначе нужно добавлять атрибуты из both_sides
        candidate_keys = []
        from itertools import combinations

        for r in range(len(both_sides) + 1):
            for combo in combinations(both_sides, r):
                candidate = must_have | set(combo)
                if FDAlgorithms.is_superkey(candidate, relation):
                    # Проверяем минимальность
                    is_minimal = True
                    for key in candidate_keys:
                        if key.issubset(candidate):
                            is_minimal = False
                            break
                    if is_minimal:
                        candidate_keys.append(candidate)

        return candidate_keys

    @staticmethod
    def minimal_cover(fds: List[FunctionalDependency]) -> List[FunctionalDependency]:

        # Шаг 1: Разделить правые части
        split_fds = []
        for fd in fds:
            for attr in fd.dependent:
                split_fds.append(FunctionalDependency(
                    fd.determinant.copy(),
                    {attr}
                ))

        # Шаг 2: Удалить избыточные атрибуты из левых частей
        reduced_fds = []
        for fd in split_fds:
            if len(fd.determinant) == 1:
                reduced_fds.append(fd)
                continue

            minimal_det = fd.determinant.copy()
            for attr in fd.determinant:
                test_det = minimal_det - {attr}
                if test_det:
                    closure = FDAlgorithms.closure(test_det, split_fds)
                    if fd.dependent.issubset(closure):
                        minimal_det = test_det

            reduced_fds.append(FunctionalDependency(minimal_det, fd.dependent))

        # Шаг 3: Удалить избыточные ФЗ
        minimal_fds = []
        for i, fd in enumerate(reduced_fds):
            # Проверяем, можно ли вывести эту ФЗ из остальных
            other_fds = reduced_fds[:i] + reduced_fds[i + 1:]
            closure = FDAlgorithms.closure(fd.determinant, other_fds)

            if not fd.dependent.issubset(closure):
                minimal_fds.append(fd)

        return minimal_fds

    @staticmethod
    def decompose_to_bcnf_step(relation: Relation) -> Tuple[bool, List[Relation]]:
        """
        Один шаг декомпозиции в НФБК

        Returns:
            (найдено_нарушение, список_отношений)
        """
        # Находим ФЗ, нарушающую НФБК
        for fd in relation.functional_dependencies:
            if not FDAlgorithms.is_superkey(fd.determinant, relation):
                # Нашли нарушение, декомпозируем

                # R1 содержит детерминант и зависимые атрибуты
                r1_attrs = list(fd.determinant | fd.dependent)
                r1_fds = []

                # R2 содержит детерминант и остальные атрибуты
                r2_attrs = list(fd.determinant | (relation.get_all_attributes_set() - fd.dependent))
                r2_fds = []

                # Проецируем ФЗ на новые отношения
                for orig_fd in relation.functional_dependencies:
                    # Для R1
                    if orig_fd.determinant.issubset(set(r1_attrs)):
                        projected_dependent = orig_fd.dependent & set(r1_attrs)
                        if projected_dependent and projected_dependent != orig_fd.determinant:
                            r1_fds.append(FunctionalDependency(
                                orig_fd.determinant,
                                projected_dependent
                            ))

                    # Для R2
                    if orig_fd.determinant.issubset(set(r2_attrs)):
                        projected_dependent = orig_fd.dependent & set(r2_attrs)
                        if projected_dependent and projected_dependent != orig_fd.determinant:
                            r2_fds.append(FunctionalDependency(
                                orig_fd.determinant,
                                projected_dependent
                            ))

                r1 = Relation(f"{relation.name}_1", r1_attrs, r1_fds)
                r2 = Relation(f"{relation.name}_2", r2_attrs, r2_fds)

                return True, [r1, r2]

        return False, [relation]