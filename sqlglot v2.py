import sqlglot
from sqlglot import parse_one, expressions as exp
import pandas as pd

# Reference column metadata (lowercased and stripped)
df_columns = pd.DataFrame({
    "Database Name": ["ip", "ip", "ip", "ip", "ip", "ip", "ip", "ip"],
    "Schema Name": ["mg", "mg", "mg", "mg", "mg", "delq", "delq", "mg"],
    "Table Name": ["loan", "loan", "loan", "letter", "letter", "delq", "delq", "letter"],
    "Column Name": ["loan_number", "fpb", "npdd", "loan_number", "letter_date", "loan_number", "dpd", "letter_id"]
})
df_columns = df_columns.astype(str).apply(lambda col: col.str.strip().str.lower())

# Sample SQL with CTEs and subquery
sql = """
WITH cte1 AS (
    SELECT loan_number, dpd FROM delq
), 
cte2 AS (
    SELECT loan_number, letter_date FROM mg.letter
)
SELECT 
    a.loan_number, 
    b.letter_date, 
    c.dpd 
FROM (SELECT * FROM mg.loan) a
LEFT JOIN cte2 b ON a.loan_number = b.loan_number
LEFT JOIN cte1 c ON a.loan_number = c.loan_number
"""

parsed = parse_one(sql)

# Extract CTE names
cte_names = set()
with_expr = parsed.args.get("with")
if with_expr:
    for cte in with_expr.expressions:
        cte_names.add(cte.alias)

# Helper to collect alias mappings
alias_map = {}

def process_tables(expr):
    for table_expr in expr.find_all(exp.Table):
        alias = table_expr.alias_or_name
        table = table_expr.name.lower()

        if table in cte_names:
            continue  # skip CTEs

        db_expr = table_expr.args.get("db")
        schema_expr = table_expr.args.get("catalog")

        db = db_expr.name.lower() if db_expr else None
        schema = schema_expr.name.lower() if schema_expr else None

        matches = df_columns[df_columns["Table Name"] == table]
        if not db and len(matches["Database Name"].unique()) == 1:
            db = matches.iloc[0]["Database Name"]
        if not schema and len(matches["Schema Name"].unique()) == 1:
            schema = matches.iloc[0]["Schema Name"]

        alias_map[alias] = (db, schema, table)

process_tables(parsed)

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
            matches = df_columns[df_columns["Column Name"] == col]
            if len(matches) == 1:
                row = matches.iloc[0]
                records.add((row["Database Name"], row["Schema Name"], row["Table Name"], row["Column Name"]))
            else:
                for _, row in matches.iterrows():
                    records.add((row["Database Name"], row["Schema Name"], row["Table Name"], row["Column Name"]))

# Result
df_result = pd.DataFrame(sorted(records), columns=["Database", "Schema", "Table", "Column"])
print(df_result)
