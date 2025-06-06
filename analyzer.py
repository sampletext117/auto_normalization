"""
Модуль для анализа нормальных форм отношений
"""
from typing import List, Set, Tuple, Optional
from models import Relation, NormalForm, FunctionalDependency, Attribute
from fd_algorithms import FDAlgorithms


class NormalFormAnalyzer:
    """Класс для анализа нормальных форм"""

    def __init__(self, relation: Relation):
        self.relation = relation
        self.candidate_keys = FDAlgorithms.find_candidate_keys(relation)
        self.prime_attributes = self._find_prime_attributes()
        self.non_prime_attributes = self.relation.get_all_attributes_set() - self.prime_attributes

    def _find_prime_attributes(self) -> Set[Attribute]:
        """Найти простые атрибуты (входящие хотя бы в один ключ)"""
        prime_attrs = set()
        for key in self.candidate_keys:
            prime_attrs.update(key)
        return prime_attrs

    def check_1nf(self) -> Tuple[bool, List[str]]:
        """
        Проверка первой нормальной формы

        Returns:
            (соответствует_1НФ, список_нарушений)
        """
        violations = []

        # В нашей модели предполагаем, что все значения атомарны
        # В реальности здесь была бы проверка на составные/множественные значения

        if not self.relation.attributes:
            violations.append("Отношение не содержит атрибутов")

        # Проверяем наличие первичного ключа
        has_key = any(self.candidate_keys)
        if not has_key:
            violations.append("Отношение не имеет первичного ключа")

        return len(violations) == 0, violations

    def check_2nf(self) -> Tuple[bool, List[str]]:
        """
        Проверка второй нормальной формы

        Returns:
            (соответствует_2НФ, список_нарушений)
        """
        violations = []

        # Сначала проверяем 1НФ
        is_1nf, nf1_violations = self.check_1nf()
        if not is_1nf:
            violations.extend(nf1_violations)
            return False, violations

        # Если все ключи состоят из одного атрибута, отношение автоматически в 2НФ
        if all(len(key) == 1 for key in self.candidate_keys):
            return True, []

        # Проверяем частичные зависимости от составных ключей
        for fd in self.relation.functional_dependencies:
            # Пропускаем тривиальные зависимости
            if fd.is_trivial():
                continue

            # Проверяем только зависимости, где зависимые атрибуты непростые
            dependent_non_prime = fd.dependent & self.non_prime_attributes
            if not dependent_non_prime:
                continue

            # Проверяем, является ли детерминант частью какого-либо составного ключа
            for key in self.candidate_keys:
                if len(key) > 1 and fd.determinant.issubset(key) and fd.determinant != key:
                    # Это частичная зависимость
                    det_str = ", ".join([a.name for a in fd.determinant])
                    dep_str = ", ".join([a.name for a in dependent_non_prime])
                    key_str = ", ".join([a.name for a in key])
                    violations.append(
                        f"Частичная зависимость: {{{det_str}}} → {{{dep_str}}} "
                        f"(детерминант - часть ключа {{{key_str}}})"
                    )
                    break

        return len(violations) == 0, violations

    def check_3nf(self) -> Tuple[bool, List[str]]:
        """
        Проверка третьей нормальной формы

        Returns:
            (соответствует_3НФ, список_нарушений)
        """
        violations = []

        # Сначала проверяем 2НФ
        is_2nf, nf2_violations = self.check_2nf()
        if not is_2nf:
            violations.extend(nf2_violations)
            return False, violations

        # Проверяем отсутствие транзитивных зависимостей
        for fd in self.relation.functional_dependencies:
            # Пропускаем тривиальные зависимости
            if fd.is_trivial():
                continue

            # Проверяем условия 3НФ:
            # ФЗ X→Y нарушает 3НФ, если:
            # 1. X не является суперключом, И
            # 2. Y-X содержит непростые атрибуты

            is_superkey = FDAlgorithms.is_superkey(fd.determinant, self.relation)

            # Вычисляем Y-X (атрибуты, которые есть в Y, но не в X)
            dependent_minus_determinant = fd.dependent - fd.determinant
            non_prime_in_dependent = dependent_minus_determinant & self.non_prime_attributes

            if not is_superkey and non_prime_in_dependent:
                det_str = ", ".join([a.name for a in fd.determinant])
                dep_str = ", ".join([a.name for a in non_prime_in_dependent])
                violations.append(
                    f"Нарушение 3НФ: {{{det_str}}} → {{{dep_str}}} "
                    f"(детерминант не является суперключом, зависимые непростые атрибуты)"
                )

        return len(violations) == 0, violations

    def check_bcnf(self) -> Tuple[bool, List[str]]:
        """
        Проверка нормальной формы Бойса-Кодда

        Returns:
            (соответствует_НФБК, список_нарушений)
        """
        violations = []

        # Сначала проверяем 1НФ
        is_1nf, nf1_violations = self.check_1nf()
        if not is_1nf:
            violations.extend(nf1_violations)
            return False, violations

        # Проверяем, что все нетривиальные детерминанты являются суперключами
        for fd in self.relation.functional_dependencies:
            # Пропускаем тривиальные зависимости
            if fd.is_trivial():
                continue

            if not FDAlgorithms.is_superkey(fd.determinant, self.relation):
                det_str = ", ".join([a.name for a in fd.determinant])
                dep_str = ", ".join([a.name for a in fd.dependent])
                violations.append(
                    f"Нарушение НФБК: {{{det_str}}} → {{{dep_str}}} "
                    f"(детерминант не является суперключом)"
                )

        return len(violations) == 0, violations

    def check_4nf(self) -> Tuple[bool, List[str]]:
        """
        Проверка четвертой нормальной формы

        Returns:
            (соответствует_4НФ, список_нарушений)
        """
        violations = []

        # Сначала проверяем НФБК
        is_bcnf, bcnf_violations = self.check_bcnf()
        if not is_bcnf:
            violations.extend(bcnf_violations)
            return False, violations

        # Проверяем многозначные зависимости
        for mvd in self.relation.multivalued_dependencies:
            # Проверяем, что детерминант является суперключом
            # или МЗД тривиальна (Y ∪ Z = R - X)
            if not FDAlgorithms.is_superkey(mvd.determinant, self.relation):
                # Проверяем тривиальность
                all_attrs = self.relation.get_all_attributes_set()
                remaining = all_attrs - mvd.determinant - mvd.dependent

                # МЗД X →→ Y тривиальна, если Y ∪ Z = R - X (где Z = R - X - Y)
                if remaining:  # Если есть другие атрибуты, МЗД нетривиальна
                    det_str = ", ".join([a.name for a in mvd.determinant])
                    dep_str = ", ".join([a.name for a in mvd.dependent])
                    violations.append(
                        f"Нарушение 4НФ: {{{det_str}}} →→ {{{dep_str}}} "
                        f"(нетривиальная многозначная зависимость, детерминант не суперключ)"
                    )

        return len(violations) == 0, violations

    def determine_normal_form(self) -> Tuple[NormalForm, List[str]]:
        """
        Определить текущую нормальную форму отношения

        Returns:
            (нормальная_форма, список_всех_нарушений)
        """
        all_violations = []

        # Проверяем последовательно все нормальные формы
        is_1nf, violations_1nf = self.check_1nf()
        if not is_1nf:
            all_violations.extend(violations_1nf)
            return NormalForm.UNNORMALIZED, all_violations

        is_2nf, violations_2nf = self.check_2nf()
        if not is_2nf:
            # Убираем дублирование нарушений 1НФ
            for v in violations_2nf:
                if v not in violations_1nf:
                    all_violations.append(v)
            return NormalForm.FIRST_NF, all_violations

        is_3nf, violations_3nf = self.check_3nf()
        if not is_3nf:
            # Убираем дублирование нарушений 2НФ
            for v in violations_3nf:
                if v not in violations_2nf:
                    all_violations.append(v)
            return NormalForm.SECOND_NF, all_violations

        is_bcnf, violations_bcnf = self.check_bcnf()
        if not is_bcnf:
            all_violations.extend(violations_bcnf)
            return NormalForm.THIRD_NF, all_violations

        is_4nf, violations_4nf = self.check_4nf()
        if not is_4nf:
            all_violations.extend(violations_4nf)
            return NormalForm.BCNF, all_violations

        return NormalForm.FOURTH_NF, all_violations

    def get_analysis_report(self) -> str:
        """Получить подробный отчет об анализе"""
        report = f"Анализ отношения: {self.relation.name}\n"
        report += "=" * 50 + "\n\n"

        # Атрибуты
        report += "Атрибуты:\n"
        for attr in self.relation.attributes:
            report += f"  - {attr.name} ({attr.data_type})"
            if attr.is_primary_key:
                report += " [PK]"
            report += "\n"

        # Функциональные зависимости
        report += f"\nФункциональные зависимости ({len(self.relation.functional_dependencies)}):\n"
        for fd in self.relation.functional_dependencies:
            report += f"  - {fd}\n"

        # Многозначные зависимости
        if self.relation.multivalued_dependencies:
            report += f"\nМногозначные зависимости ({len(self.relation.multivalued_dependencies)}):\n"
            for mvd in self.relation.multivalued_dependencies:
                report += f"  - {mvd}\n"

        # Ключи
        report += f"\nКандидатные ключи ({len(self.candidate_keys)}):\n"
        for key in self.candidate_keys:
            key_str = ", ".join([a.name for a in key])
            report += f"  - {{{key_str}}}\n"

        # Простые и непростые атрибуты
        report += f"\nПростые атрибуты: {{{', '.join([a.name for a in self.prime_attributes])}}}\n"
        report += f"Непростые атрибуты: {{{', '.join([a.name for a in self.non_prime_attributes])}}}\n"

        # Определение нормальной формы
        nf, violations = self.determine_normal_form()
        report += f"\nТекущая нормальная форма: {nf.value}\n"

        if violations:
            report += "\nНарушения:\n"
            for v in violations:
                report += f"  - {v}\n"

        return report