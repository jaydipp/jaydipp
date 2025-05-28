import sqlglot
from sqlglot import parse_one, expressions as exp
import pandas as pd

# Sample SQL query with CTEs and subqueries
sql = """
WITH cte1 AS (
    SELECT loan_number, process_dt FROM a.loan
),
cte2 AS (
    SELECT * FROM cte1 WHERE process_dt > '2023-01-01'
)
SELECT x.loan_number, x.process_dt
FROM (
    SELECT * FROM cte2
) x
"""

# Reference column metadata
column_df = pd.DataFrame({
    "Database Name": ["ip", "ip", "ip", "ip"],
    "Schema Name":   ["a", "a", "a", "b"],
    "Table Name":    ["loan", "loan", "loan", "another"],
    "Column Name":   ["loan_number", "process_dt", "abc", "process_dt"]
})

column_df = column_df.astype(str).apply(lambda col: col.str.strip().str.lower())

parsed = parse_one(sql)

alias_map = {}  # alias -> (db, schema, table)
cte_map = {}    # cte name -> parsed expression

# Collect all CTEs
for cte in parsed.find_all(exp.CTE):
    cte_name = cte.alias_or_name
    cte_map[cte_name] = cte.this

# Track alias from tables and subqueries

def process_table_expr(expr, alias_override=None):
    if isinstance(expr, exp.Subquery):
        alias = alias_override or expr.alias_or_name
        for table_expr in expr.find_all(exp.Table):
            db_expr = table_expr.args.get("db")
            schema_expr = table_expr.args.get("catalog")
            db = db_expr.name.lower() if db_expr else None
            schema = schema_expr.name.lower() if schema_expr else None
            table = table_expr.name.lower()
            if alias:
                alias_map[alias] = (db, schema, table)
    elif isinstance(expr, exp.Table):
        alias = alias_override or expr.alias_or_name
        db_expr = expr.args.get("db")
        schema_expr = expr.args.get("catalog")
        db = db_expr.name.lower() if db_expr else None
        schema = schema_expr.name.lower() if schema_expr else None
        table = expr.name.lower()

        # Try to infer DB/schema from column_df if missing
        if not db or not schema:
            matches = column_df[column_df["Table Name"] == table]
            if not db and not matches.empty and matches["Database Name"].nunique() == 1:
                db = matches.iloc[0]["Database Name"]
            if not schema and not matches.empty and matches["Schema Name"].nunique() == 1:
                schema = matches.iloc[0]["Schema Name"]

        if alias:
            alias_map[alias] = (db, schema, table)

# Handle FROM clauses, subqueries, and main tables
for node in parsed.find_all(exp.From):
    for expr in node.args.get("expressions", []):
        process_table_expr(expr)

# Also process CTE subqueries
for name, cte_expr in cte_map.items():
    for node in cte_expr.find_all(exp.From):
        for expr in node.args.get("expressions", []):
            process_table_expr(expr)

# Extract columns
records = set()

for node in parsed.walk():
    if isinstance(node, exp.Column):
        col = node.name.strip().lower()
        alias = node.table

        if alias and alias in alias_map:
            db, schema, table = alias_map[alias]
            records.add((db, schema, table, col))
        else:
            # Try to resolve unqualified columns
            matches = column_df[column_df["Column Name"] == col]
            if not matches.empty:
                for _, row in matches.iterrows():
                    records.add((
                        row["Database Name"],
                        row["Schema Name"],
                        row["Table Name"],
                        row["Column Name"]
                    ))

# Final DataFrame
df_result = pd.DataFrame(sorted(records), columns=["Database", "Schema", "Table", "Column"])
df_result = df_result.drop_duplicates()
print(df_result)
