"""
Модуль с алгоритмами декомпозиции для различных нормальных форм
"""
from typing import List, Set, Tuple
from models import (
    Relation, FunctionalDependency, NormalForm,
    DecompositionStep, NormalizationResult, Attribute
)
from fd_algorithms import FDAlgorithms
from analyzer import NormalFormAnalyzer


class Decomposer:
    """Класс для выполнения декомпозиции отношений"""

    @staticmethod
    def decompose_to_2nf(relation: Relation) -> NormalizationResult:
        """
        Декомпозиция отношения во вторую нормальную форму
        """
        analyzer = NormalFormAnalyzer(relation)
        original_form, _ = analyzer.determine_normal_form()

        if original_form.value >= NormalForm.SECOND_NF.value:
            return NormalizationResult(
                original_form=original_form,
                target_form=NormalForm.SECOND_NF,
                original_relation=relation,
                decomposed_relations=[relation],
                steps=[],
                preserved_dependencies=relation.functional_dependencies,
                lost_dependencies=[]
            )

        steps = []
        current_relations = [relation]
        all_decomposed = []

        while current_relations:
            rel = current_relations.pop()
            rel_analyzer = NormalFormAnalyzer(rel)

            # Находим частичные зависимости
            partial_deps = []
            for fd in rel.functional_dependencies:
                # Проверяем только зависимости непростых атрибутов
                non_prime_dependent = fd.dependent & rel_analyzer.non_prime_attributes
                if not non_prime_dependent:
                    continue

                # Проверяем частичную зависимость от ключей
                for key in rel_analyzer.candidate_keys:
                    if fd.determinant.issubset(key) and fd.determinant != key:
                        partial_deps.append((fd, key))
                        break

            if not partial_deps:
                # Нет частичных зависимостей, отношение в 2НФ
                all_decomposed.append(rel)
                continue

            # Декомпозируем по первой найденной частичной зависимости
            fd, key = partial_deps[0]

            # R1: детерминант + зависимые атрибуты
            r1_attrs = list(fd.determinant | fd.dependent)
            r1_fds = Decomposer._project_fds(r1_attrs, rel.functional_dependencies)
            r1 = Relation(f"{rel.name}_partial", r1_attrs, r1_fds)

            # R2: ключ + оставшиеся атрибуты
            remaining_attrs = rel.get_all_attributes_set() - fd.dependent
            r2_attrs = list(remaining_attrs)
            r2_fds = Decomposer._project_fds(r2_attrs, rel.functional_dependencies)
            r2 = Relation(f"{rel.name}_main", r2_attrs, r2_fds)

            step = DecompositionStep(
                original_relation=rel,
                resulting_relations=[r1, r2],
                reason=f"Устранение частичной зависимости {fd}",
                violated_dependency=fd
            )
            steps.append(step)

            # Добавляем для дальнейшей обработки
            current_relations.extend([r1, r2])

        # Проверяем сохранение зависимостей
        preserved, lost = Decomposer._check_dependency_preservation(
            relation.functional_dependencies,
            all_decomposed
        )

        return NormalizationResult(
            original_form=original_form,
            target_form=NormalForm.SECOND_NF,
            original_relation=relation,
            decomposed_relations=all_decomposed,
            steps=steps,
            preserved_dependencies=preserved,
            lost_dependencies=lost
        )

    @staticmethod
    def decompose_to_3nf(relation: Relation) -> NormalizationResult:
        """
        Декомпозиция отношения в третью нормальную форму.
        Использует алгоритм синтеза
        """
        analyzer = NormalFormAnalyzer(relation)
        original_form, _ = analyzer.determine_normal_form()

        if original_form.value >= NormalForm.THIRD_NF.value:
            return NormalizationResult(
                original_form=original_form,
                target_form=NormalForm.THIRD_NF,
                original_relation=relation,
                decomposed_relations=[relation],
                steps=[],
                preserved_dependencies=relation.functional_dependencies,
                lost_dependencies=[]
            )

        steps = []

        minimal_fds = FDAlgorithms.minimal_cover(relation.functional_dependencies)

        # Шаг 2: Для каждой ФЗ создать отношение
        decomposed_relations = []
        fd_groups = {}

        # Группируем ФЗ с одинаковыми детерминантами
        for fd in minimal_fds:
            det_key = frozenset(fd.determinant)
            if det_key not in fd_groups:
                fd_groups[det_key] = []
            fd_groups[det_key].append(fd)

        # Создаем отношения для каждой группы
        for det_key, fds in fd_groups.items():
            # Собираем все атрибуты для этого отношения
            attrs = set(det_key)
            for fd in fds:
                attrs.update(fd.dependent)

            # Проецируем все ФЗ на эти атрибуты
            proj_fds = Decomposer._project_fds(list(attrs), minimal_fds)

            rel_name = f"{relation.name}_3nf_{len(decomposed_relations) + 1}"
            new_rel = Relation(rel_name, list(attrs), proj_fds)
            decomposed_relations.append(new_rel)

        # Шаг 3: Проверить, содержит ли хотя бы одно отношение ключ
        has_key = False
        for rel in decomposed_relations:
            for key in analyzer.candidate_keys:
                if key.issubset(rel.get_all_attributes_set()):
                    has_key = True
                    break
            if has_key:
                break

        # Если ни одно отношение не содержит ключ, добавляем отношение с ключом
        if not has_key and analyzer.candidate_keys:
            key = list(analyzer.candidate_keys)[0]
            key_fds = Decomposer._project_fds(list(key), relation.functional_dependencies)
            key_rel = Relation(f"{relation.name}_key", list(key), key_fds)
            decomposed_relations.append(key_rel)

        # Шаг 4: Удалить избыточные отношения
        final_relations = []
        for i, rel in enumerate(decomposed_relations):
            is_redundant = False
            for j, other_rel in enumerate(decomposed_relations):
                if i != j and rel.get_all_attributes_set().issubset(other_rel.get_all_attributes_set()):
                    is_redundant = True
                    break
            if not is_redundant:
                final_relations.append(rel)

        # Создаем шаги декомпозиции
        step = DecompositionStep(
            original_relation=relation,
            resulting_relations=final_relations,
            reason="Декомпозиция в 3НФ методом синтеза"
        )
        steps.append(step)

        # Проверяем сохранение зависимостей
        preserved, lost = Decomposer._check_dependency_preservation(
            relation.functional_dependencies,
            final_relations
        )

        return NormalizationResult(
            original_form=original_form,
            target_form=NormalForm.THIRD_NF,
            original_relation=relation,
            decomposed_relations=final_relations,
            steps=steps,
            preserved_dependencies=preserved,
            lost_dependencies=lost
        )

    @staticmethod
    def decompose_to_bcnf(relation: Relation) -> NormalizationResult:
        """
        Декомпозиция отношения в нормальную форму Бойса-Кодда
        """
        analyzer = NormalFormAnalyzer(relation)
        original_form, _ = analyzer.determine_normal_form()

        if original_form.value >= NormalForm.BCNF.value:
            return NormalizationResult(
                original_form=original_form,
                target_form=NormalForm.BCNF,
                original_relation=relation,
                decomposed_relations=[relation],
                steps=[],
                preserved_dependencies=relation.functional_dependencies,
                lost_dependencies=[]
            )

        steps = []
        to_process = [relation]
        final_relations = []

        while to_process:
            current_rel = to_process.pop()

            # Ищем ФЗ, нарушающую НФБК
            violating_fd = None
            for fd in current_rel.functional_dependencies:
                if not fd.is_trivial() and not FDAlgorithms.is_superkey(fd.determinant, current_rel):
                    violating_fd = fd
                    break

            if not violating_fd:
                # Отношение в НФБК
                final_relations.append(current_rel)
                continue

            # Декомпозируем
            # R1: детерминант + зависимые атрибуты
            r1_attrs = list(violating_fd.determinant | violating_fd.dependent)
            r1_fds = Decomposer._project_fds(r1_attrs, current_rel.functional_dependencies)
            r1 = Relation(
                f"{current_rel.name}_bcnf1_{len(steps)}",
                r1_attrs,
                r1_fds
            )

            # R2: детерминант + остальные атрибуты
            remaining = current_rel.get_all_attributes_set() - violating_fd.dependent
            r2_attrs = list(violating_fd.determinant | remaining)
            r2_fds = Decomposer._project_fds(r2_attrs, current_rel.functional_dependencies)
            r2 = Relation(
                f"{current_rel.name}_bcnf2_{len(steps)}",
                r2_attrs,
                r2_fds
            )

            step = DecompositionStep(
                original_relation=current_rel,
                resulting_relations=[r1, r2],
                reason=f"Устранение нарушения НФБК: {violating_fd}",
                violated_dependency=violating_fd
            )
            steps.append(step)

            # Добавляем для дальнейшей обработки
            to_process.extend([r1, r2])

        # Проверяем сохранение зависимостей
        preserved, lost = Decomposer._check_dependency_preservation(
            relation.functional_dependencies,
            final_relations
        )

        return NormalizationResult(
            original_form=original_form,
            target_form=NormalForm.BCNF,
            original_relation=relation,
            decomposed_relations=final_relations,
            steps=steps,
            preserved_dependencies=preserved,
            lost_dependencies=lost
        )

    @staticmethod
    def decompose_to_4nf(relation: Relation) -> NormalizationResult:
        """
        Декомпозиция отношения в четвертую нормальную форму
        """
        # Сначала приводим к НФБК
        bcnf_result = Decomposer.decompose_to_bcnf(relation)

        analyzer = NormalFormAnalyzer(relation)
        original_form, _ = analyzer.determine_normal_form()

        if not relation.multivalued_dependencies:
            # Если нет многозначных зависимостей, отношение уже в 4НФ после НФБК
            return NormalizationResult(
                original_form=original_form,
                target_form=NormalForm.FOURTH_NF,
                original_relation=relation,
                decomposed_relations=bcnf_result.decomposed_relations,
                steps=bcnf_result.steps,
                preserved_dependencies=bcnf_result.preserved_dependencies,
                lost_dependencies=bcnf_result.lost_dependencies
            )

        # Декомпозируем каждое отношение из НФБК по многозначным зависимостям
        steps = bcnf_result.steps.copy()
        to_process = bcnf_result.decomposed_relations.copy()
        final_relations = []

        while to_process:
            current_rel = to_process.pop(0)

            # Проецируем многозначные зависимости на текущее отношение
            violating_mvd = None
            current_attrs = current_rel.get_all_attributes_set()

            for mvd in relation.multivalued_dependencies:
                # МЗД применима, если все атрибуты детерминанта есть в текущем отношении
                if not mvd.determinant.issubset(current_attrs):
                    continue

                # Вычисляем зависимую и независимую части в контексте текущего отношения
                dependent_in_rel = mvd.dependent & current_attrs
                independent_in_rel = current_attrs - mvd.determinant - dependent_in_rel

                # Проверяем, что МЗД нетривиальна в контексте текущего отношения
                if dependent_in_rel and independent_in_rel:
                    # Проверяем, является ли детерминант суперключом
                    if not FDAlgorithms.is_superkey(mvd.determinant, current_rel):
                        violating_mvd = mvd
                        break

            if not violating_mvd:
                # Отношение в 4НФ
                final_relations.append(current_rel)
                continue

            # Декомпозируем по нарушающей МЗД
            # R1: детерминант + зависимые атрибуты
            r1_attrs = list(violating_mvd.determinant | (violating_mvd.dependent & current_attrs))
            r1_fds = Decomposer._project_fds(r1_attrs, current_rel.functional_dependencies)
            r1 = Relation(
                f"{current_rel.name}_4nf_{len(final_relations)}",
                r1_attrs,
                r1_fds
            )

            # R2: детерминант + независимые атрибуты
            independent_attrs = current_attrs - violating_mvd.dependent
            r2_attrs = list(violating_mvd.determinant | independent_attrs)
            r2_fds = Decomposer._project_fds(r2_attrs, current_rel.functional_dependencies)
            r2 = Relation(
                f"{current_rel.name}_4nf_{len(final_relations) + 1}",
                r2_attrs,
                r2_fds
            )

            step = DecompositionStep(
                original_relation=current_rel,
                resulting_relations=[r1, r2],
                reason=f"Устранение многозначной зависимости: {violating_mvd}"
            )
            steps.append(step)

            # Добавляем для дальнейшей обработки
            to_process.extend([r1, r2])

        # Проверяем сохранение зависимостей
        preserved, lost = Decomposer._check_dependency_preservation(
            relation.functional_dependencies,
            final_relations
        )

        return NormalizationResult(
            original_form=original_form,
            target_form=NormalForm.FOURTH_NF,
            original_relation=relation,
            decomposed_relations=final_relations,
            steps=steps,
            preserved_dependencies=preserved,
            lost_dependencies=lost
        )

    @staticmethod
    def _project_fds(attributes: List[Attribute], fds: List[FunctionalDependency]) -> List[FunctionalDependency]:
        """
        Проецировать функциональные зависимости на подмножество атрибутов
        """
        attr_set = set(attributes)
        projected_fds = []

        # Для каждого подмножества атрибутов проверяем, что оно определяет
        from itertools import combinations
        for r in range(1, len(attributes)):
            for det_combo in combinations(attributes, r):
                det_set = set(det_combo)
                # Вычисляем замыкание в исходном множестве ФЗ
                closure = FDAlgorithms.closure(det_set, fds)
                # Пересекаем с проецируемыми атрибутами
                dependent = closure & attr_set - det_set

                if dependent:
                    # Проверяем минимальность
                    is_minimal = True
                    for existing_fd in projected_fds:
                        if (existing_fd.determinant == det_set and
                                existing_fd.dependent.issubset(dependent)):
                            # Расширяем существующую ФЗ
                            existing_fd.dependent.update(dependent)
                            is_minimal = False
                            break

                    if is_minimal:
                        projected_fds.append(FunctionalDependency(det_set, dependent))

        return projected_fds

    @staticmethod
    def _check_dependency_preservation(
            original_fds: List[FunctionalDependency],
            decomposed_relations: List[Relation]
    ) -> Tuple[List[FunctionalDependency], List[FunctionalDependency]]:
        """
        Проверить сохранение функциональных зависимостей после декомпозиции
        """
        preserved = []
        lost = []

        # Собираем все ФЗ из декомпозированных отношений
        all_decomposed_fds = []
        for rel in decomposed_relations:
            all_decomposed_fds.extend(rel.functional_dependencies)

        # Проверяем каждую исходную ФЗ
        for fd in original_fds:
            # Проверяем, можно ли вывести эту ФЗ из декомпозированных
            closure = FDAlgorithms.closure(fd.determinant, all_decomposed_fds)

            if fd.dependent.issubset(closure):
                preserved.append(fd)
            else:
                lost.append(fd)

        return preserved, lost