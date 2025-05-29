import sqlglot
from sqlglot import parse_one, exp
import pandas as pd

sql = """
WITH cte1 AS (
    SELECT h.hi, h.hj FROM mg.hope h
),
cte2 AS (
    SELECT p.pr, p.pz, cte1.hi FROM mg.pro p JOIN cte1 ON cte1.hi = p.hi
),
final_cte AS (
    SELECT cte2.*, ROW_NUMBER() OVER (PARTITION BY cte2.hi ORDER BY cte2.pr) AS rn FROM cte2
)
SELECT f.hi, f.pr 
FROM final_cte f
WHERE f.rn = 1
"""

df_columns = pd.DataFrame({
    "Database Name": ["mg"] * 5 + ["mg"] * 3,
    "Schema Name": ["mg"] * 8,
    "Table Name": ["hope"] * 3 + ["pro"] * 3 + ["hope", "pro"],
    "Column Name": ["hi", "hj", "hk", "pr", "pz", "hi", "hj", "pr"]
}).astype(str).apply(lambda col: col.str.strip().str.lower())

parsed = parse_one(sql)
alias_map = {}
final_records = set()

def resolve_column(col_name, alias):
    col_name = col_name.lower()
    if alias:
        alias = alias.lower()
        if alias in alias_map:
            for c, db, schema, table in alias_map[alias]:
                if c == col_name:
                    return (db, schema, table, col_name)
    else:
        matches = df_columns[df_columns["Column Name"] == col_name]
        if len(matches["Table Name"].unique()) == 1:
            row = matches.iloc[0]
            return (row["Database Name"], row["Schema Name"], row["Table Name"], col_name)
    return None

def process_projection(projection):
    cols = []
    for proj in projection:
        if isinstance(proj, exp.Star):
            continue
        if isinstance(proj, exp.Alias):
            alias_name = proj.alias
            col_expr = proj.this
        else:
            alias_name = None
            col_expr = proj
        for col in col_expr.find_all(exp.Column):
            resolved = resolve_column(col.name, col.table)
            if resolved:
                cols.append(resolved)
                if alias_name:
                    cols.append((resolved[0], resolved[1], resolved[2], alias_name.lower()))
    return cols

def process_query(query):
    if not isinstance(query, exp.Subqueryable):
        return []
    cols = process_projection(query.expressions)
    from_exprs = query.args.get("from")
    if from_exprs:
        for t in from_exprs.find_all(exp.Table):
            alias = t.alias_or_name
            table = t.name.lower()
            db = t.args.get("catalog")
            schema = t.args.get("db")
            db = db.name.lower() if db else None
            schema = schema.name.lower() if schema else None
            matches = df_columns[df_columns["Table Name"] == table]
            entries = [(row["Column Name"], row["Database Name"], row["Schema Name"], row["Table Name"])
                       for _, row in matches.iterrows()]
            alias_map[alias.lower()] = [(c, db, schema, table) for c, db, schema, table in entries]
    return cols

def build_alias_map(node):
    for cte in node.find_all(exp.CTE):
        alias = cte.alias_or_name.lower()
        cte_body = cte.this
        alias_map[alias] = process_query(cte_body)

build_alias_map(parsed)

for col in parsed.find_all(exp.Column):
    col_name = col.name
    table_alias = col.table
    resolved = resolve_column(col_name, table_alias)
    if resolved:
        final_records.add(resolved)

df_result = pd.DataFrame(sorted(final_records), columns=["Database", "Schema", "Table", "Column"])
print(df_result)
