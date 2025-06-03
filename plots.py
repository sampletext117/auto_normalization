import matplotlib.pyplot as plt
import numpy as np

# --- Гипотетические данные для исследования ---

# Сценарий 1: Зависимость времени от количества атрибутов (N)
# Предположим, количество ФЗ (M) фиксировано, например, M = 10
n_values = np.array([5, 7, 10, 12, 15, 17, 20]) # Количество атрибутов

# Время анализа (Мой алгоритм) - остается тем же
analysis_time_vs_n_my_algo = np.array([0.01, 0.05, 0.3, 1.0, 5.0, 15.0, 40.0])

# Время декомпозиции (Мой алгоритм) - остается тем же
decomposition_time_vs_n_my_algo = np.array([0.02, 0.1, 0.8, 3.0, 15.0, 45.0, 120.0])

# Время декомпозиции (JMathNorm) - гипотетические данные
# Начинается чуть медленнее, но растет медленнее с увеличением N
jmath_time_vs_n = np.array([0.1, 0.4, 0.9, 2.0, 6.0, 12.0, 25.0])


# Сценарий 2: Зависимость времени от количества функциональных зависимостей (M)
# Предположим, количество атрибутов (N) фиксировано, например, N = 10
m_values = np.array([5, 10, 15, 20, 25, 30, 35, 40]) # Количество ФЗ

# Время анализа (Мой алгоритм) - остается тем же
analysis_time_vs_m_my_algo = 0.3 * (m_values / 10)**2

# Время декомпозиции (Мой алгоритм) - остается тем же
decomposition_time_vs_m_my_algo = 0.8 * (m_values / 10)**2.2

# Время декомпозиции (JMathNorm) - гипотетические данные
# Начинается чуть медленнее, но растет медленнее с увеличением M
jmath_time_vs_m = np.array([0.5, 1.2, 2.2, 3.5, 4.2, 6.0, 8.0, 10.5])


# --- Построение графиков ---

plt.style.use('seaborn-v0_8-whitegrid') # Используем стиль для лучшего вида

# График 1: Время выполнения от количества атрибутов (N)
plt.figure(figsize=(12, 7)) # Немного увеличим размер для лучшей читаемости
plt.plot(n_values, analysis_time_vs_n_my_algo, marker='o', linestyle='-', color='dodgerblue', label='Время анализа (Мой алгоритм)')
plt.plot(n_values, decomposition_time_vs_n_my_algo, marker='s', linestyle='--', color='orangered', label='Время декомпозиции (Мой алгоритм)')
plt.plot(n_values, jmath_time_vs_n, marker='^', linestyle=':', color='green', label='Время декомпозиции (JMathNorm)')

plt.title('Зависимость времени выполнения от количества атрибутов (N)', fontsize=16)
plt.xlabel('Количество атрибутов (N)', fontsize=12)
plt.ylabel('Время выполнения (секунды)', fontsize=12)
plt.xticks(n_values)
plt.legend()
plt.grid(True, which="both", ls="--", linewidth=0.5)
plt.tight_layout()
plt.show()

# График 2: Время выполнения от количества функциональных зависимостей (M)
plt.figure(figsize=(12, 7)) # Немного увеличим размер
plt.plot(m_values, analysis_time_vs_m_my_algo, marker='o', linestyle='-', color='forestgreen', label='Время анализа (Мой алгоритм)')
plt.plot(m_values, decomposition_time_vs_m_my_algo, marker='s', linestyle='--', color='purple', label='Время декомпозиции (Мой алгоритм)')
plt.plot(m_values, jmath_time_vs_m, marker='^', linestyle=':', color='brown', label='Время декомпозиции (JMathNorm)')

plt.title('Зависимость времени выполнения от количества ФЗ (M)', fontsize=16)
plt.xlabel('Количество функциональных зависимостей (M)', fontsize=12)
plt.ylabel('Время выполнения (секунды)', fontsize=12)
plt.xticks(m_values)
plt.legend()
plt.grid(True, which="both", ls="--", linewidth=0.5)
plt.tight_layout()
plt.show()