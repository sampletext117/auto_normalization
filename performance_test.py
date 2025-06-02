# файл: performance_test.py

import psycopg2
import time
from typing import List, Dict, Tuple, Optional
import matplotlib.pyplot as plt

from models import Relation, Attribute
from data_test import *
from decomposition import Decomposer


def _build_select_queries(
    orig_rel: Relation,
    normalized: List[Relation]
) -> Dict[str, Dict[str, str]]:
    """
    Построить для каждого уровня нормализации (ключ словаря) набор SQL SELECT-запросов.
    Ключи внутри: "all", "pk", "non_pk", "join_norm" (для нормализованных).
    Возвращает:
    {
      "original": {
          "all":    "SELECT a,b,c,... FROM Orig;",
          "pk":     "SELECT <PK_COLUMNS> FROM Orig;",
          "non_pk": "SELECT <NON_PK_COLUMNS> FROM Orig;"
      },
      "2NF":  { ... },
      "3NF":  { ... },
      "BCNF": { ... },
      "4NF":  { ... }
    }
    """
    queries: Dict[str, Dict[str, str]] = {}

    # ---- Исходная схема ----
    orig_name = orig_rel.name
    all_cols  = ", ".join(attr.name for attr in orig_rel.attributes)
    pk_cols   = ", ".join(attr.name for attr in orig_rel.attributes if attr.is_primary_key)
    non_pk_cols = ", ".join(attr.name for attr in orig_rel.attributes if not attr.is_primary_key)

    queries["Original"] = {
        "all":    f"SELECT {all_cols} FROM {orig_name};",
        "pk":     f"SELECT {pk_cols} FROM {orig_name};"   if pk_cols   else "",
        "non_pk": f"SELECT {non_pk_cols} FROM {orig_name};" if non_pk_cols else ""
    }

    # ---- Перебираем нормализованные отношения ----
    # Определяем уровень для каждого нормализованного Relation по имени:
    # предположим, что именем содержит _2nf, _3nf, _bcnf, _4nf (в зависимости от метода).
    # Если не нашли, складываем в «Unknown».
    def classify_level(rels: List[Relation]) -> str:
        for r in rels:
            name = r.name.lower()
            if "_2nf" in name:
                return "2NF"
            if "_3nf" in name:
                return "3NF"
            if "_bcnf" in name:
                return "BCNF"
            if "_4nf" in name:
                return "4NF"
        return "Unknown"

    level = classify_level(normalized)
    if level == "Unknown":
        # По умолчанию, если везде сходится 3NF‐декомпозиция
        level = "3NF"

    # Собираем все атрибуты по-уровневые
    # Для каждого нормализованного relation на этом уровне делаем:
    # - "all"   = SELECT <all_attrs>   FROM R1 JOIN R2 JOIN ... (склейка обратно)
    # - "pk"    = SELECT <PK всех rel>  FROM R1 JOIN R2 JOIN ...
    # - "non_pk"= SELECT <nonPK всех rel> FROM R1 JOIN R2 JOIN ...
    #
    # Сначала определения JOIN-клаузы:
    join_clause = ""
    aliases = []
    for i, r in enumerate(normalized):
        alias = f"T{i}"
        aliases.append( (alias, r) )
        if i == 0:
            join_clause = f"{r.name} AS {alias}"
        else:
            # ищем первое общее поле между текущей таблицей и предыдущими
            prev_attrs = set().union(*(set(x.attributes) for _, x in aliases[:-1]))
            cur_attrs  = set(r.attributes)
            common = prev_attrs.intersection(cur_attrs)
            if common:
                # возьмём одно общее по имени
                col = next(iter(common)).name
                join_clause = f"{join_clause} JOIN {r.name} AS {alias} ON {alias}.{col} = {aliases[0][0]}.{col}"
            else:
                # если нет пересечений, делаем CROSS JOIN
                join_clause = f"{join_clause} CROSS JOIN {r.name} AS {alias}"

    # Все столбцы «all» для normalized
    all_cols_norm = []
    pk_cols_norm  = []
    non_pk_cols_norm = []
    for alias, r in aliases:
        for attr in r.attributes:
            all_cols_norm.append(f"{alias}.{attr.name}")
            if attr.is_primary_key:
                pk_cols_norm.append(f"{alias}.{attr.name}")
            else:
                non_pk_cols_norm.append(f"{alias}.{attr.name}")

    queries[level] = {
        "all":    f"SELECT {', '.join(all_cols_norm)} FROM {join_clause};",
        "pk":     f"SELECT {', '.join(pk_cols_norm)} FROM {join_clause};"    if pk_cols_norm    else "",
        "non_pk": f"SELECT {', '.join(non_pk_cols_norm)} FROM {join_clause};" if non_pk_cols_norm else ""
    }

    return queries


def _time_query(conn: psycopg2.extensions.connection, sql: str, repeats: int = 5) -> float:
    """
    Запустить запрос `sql` `repeats` раз и вернуть среднее время в секундах.
    """
    total = 0.0
    with conn.cursor() as cur:
        for _ in range(repeats):
            start = time.perf_counter()
            cur.execute(sql)
            # ждем фактического считывания всех строк
            cur.fetchall()
            end = time.perf_counter()
            total += (end - start)
    return total / repeats


# В файле performance_test.py замените функцию run_performance_test на следующую версию:

def run_performance_test(
    orig_rel: Relation,
    num_rows: int = 1000,
    repeats: int = 5
) -> Dict[str, Dict[str, float]]:
    """
    1) Создаёт таблицы: Orig и для каждого уровня нормализации (2NF, 3NF, BCNF, 4NF).
    2) Заполняет Orig случайными данными.
    3) Для каждого уровня вызывает Decomposer.decompose_to_X(), создаёт соответствующие таблицы
       и заполняет их через INSERT SELECT DISTINCT.
    4) Замеряет время выполнения трёх видов SELECT-запросов: all, pk, non_pk
       по исходной и по каждой нормализованной схеме.
    Возвращает словарь:
      {
        "Original": {"all": t1, "pk": t2, "non_pk": t3},
        "2NF":      {"all": t4, "pk": t5, "non_pk": t6},
        "3NF":      {...},
        "BCNF":     {...},
        "4NF":      {...}
      }
    """
    conn = connect()
    results: Dict[str, Dict[str, float]] = {}
    try:
        # ----------------- 1) Создаём и заполняем исходную таблицу -----------------
        drop_table_if_exists(conn, orig_rel.name)
        create_table(conn, orig_rel)
        insert_random_data(conn, orig_rel, num_rows)

        # Подготовим словарь, куда запишем разбиения по уровням
        decomposed_by_level: Dict[str, List[Relation]] = {}

        # ----------------- 2) Декомпозиция для каждого уровня -----------------
        # 2NF
        res_2nf = Decomposer.decompose_to_2nf(orig_rel)
        decomposed_by_level["2NF"] = res_2nf.decomposed_relations

        # 3NF
        res_3nf = Decomposer.decompose_to_3nf(orig_rel)
        decomposed_by_level["3NF"] = res_3nf.decomposed_relations

        # BCNF
        res_bcnf = Decomposer.decompose_to_bcnf(orig_rel)
        decomposed_by_level["BCNF"] = res_bcnf.decomposed_relations

        # 4NF
        res_4nf = Decomposer.decompose_to_4nf(orig_rel)
        decomposed_by_level["4NF"] = res_4nf.decomposed_relations

        # ----------------- 3) Для каждого нормализованного уровня создаём и заполняем таблицы -----------------
        for level, rels in decomposed_by_level.items():
            for r in rels:
                drop_table_if_exists(conn, r.name)
                # Создаём таблицу без ограничения PK
                cols_sql = [f"{attr.name} {sql_type_for(attr)}" for attr in r.attributes]
                create_sql = f"CREATE TABLE {r.name} (\n    " + ",\n    ".join(cols_sql) + "\n);"
                with conn.cursor() as cur:
                    cur.execute(create_sql)
                conn.commit()

                # Заполняем через distinct из исходной
                cols_list = ", ".join(attr.name for attr in r.attributes)
                proj_sql = f"INSERT INTO {r.name} ({cols_list}) SELECT DISTINCT {cols_list} FROM {orig_rel.name};"
                with conn.cursor() as cur:
                    cur.execute(proj_sql)
                conn.commit()

        # ----------------- 4) Собираем запросы для всех уровней -----------------
        all_levels_queries: Dict[str, Dict[str, str]] = {}

        # Original
        all_levels_queries["Original"] = _build_select_queries(orig_rel, [])

        # 2NF
        all_levels_queries["2NF"] = _build_select_queries(orig_rel, decomposed_by_level["2NF"])["2NF"]

        # 3NF
        all_levels_queries["3NF"] = _build_select_queries(orig_rel, decomposed_by_level["3NF"])["3NF"]

        # BCNF
        all_levels_queries["BCNF"] = _build_select_queries(orig_rel, decomposed_by_level["BCNF"])["BCNF"]

        # 4NF
        all_levels_queries["4NF"] = _build_select_queries(orig_rel, decomposed_by_level["4NF"])["4NF"]

        # ----------------- 5) Замеряем время выполнения -----------------
        for level_name, qdict in all_levels_queries.items():
            results[level_name] = {}
            for qname, sql in qdict.items():
                if not sql:
                    results[level_name][qname] = float("nan")
                else:
                    print(f"[TIME-TEST] Level={level_name}, Query={qname}, SQL={sql.strip()}")
                    t = _time_query(conn, sql, repeats=repeats)
                    results[level_name][qname] = t

    finally:
        conn.close()

    return results



def plot_performance(results: Dict[str, Dict[str, float]]):
    """
    По словарю results строит графики (matplotlib), где по оси X — уровни нормализации,
    а по оси Y — время выполнения (секунды) для каждого типа запроса (all, pk, non_pk).
    """
    # Упорядочим уровни:
    levels_order = ["Original", "2NF", "3NF", "BCNF", "4NF"]
    query_types = ["all", "pk", "non_pk"]

    # Подготовим данные
    x = []
    data = {q: [] for q in query_types}
    for lvl in levels_order:
        if lvl in results:
            x.append(lvl)
            for qt in query_types:
                data[qt].append(results[lvl].get(qt, float("nan")))
    # Сделаем отдельный график для каждого типа запроса
    for qt in query_types:
        plt.figure(figsize=(8,4))
        y = data[qt]
        plt.plot(x, y, marker='o')
        plt.title(f"Время выполнения SELECT-{qt}")
        plt.xlabel("Уровень нормализации")
        plt.ylabel("Время (сек)")
        plt.grid(True)
        plt.tight_layout()
        plt.show()


