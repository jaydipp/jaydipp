import sqlglot
from sqlglot import parse_one, expressions as exp
import pandas as pd

# Sample SQL query
sql = """
WITH recent_users AS (
    SELECT user_id, user_name FROM mg.users
),
extended_users AS (
    SELECT * FROM recent_users
)
SELECT ru.*, h.user_name 
FROM extended_users ru
JOIN mg.user_info h ON h.unumber = ru.user_id
UNION
SELECT * FROM mg.archive_users
"""

# Reference column metadata
df_columns = pd.DataFrame({
    "Database Name": ["ip"] * 9,
    "Schema Name": ["mg"] * 9,
    "Table Name": ["users", "users", "user_info", "user_info", "user_info", "archive_users", "archive_users", "users", "archive_users"],
    "Column Name": [
        "user_id", "user_name", "unumber",
        "user_name", "user_id", "user_id",
        "user_name", "user_name", "user_id"
    ]
})

# Normalize metadata for matching
df_columns = df_columns.astype(str).apply(lambda col: col.str.strip().str.lower())

# Parse SQL
parsed = parse_one(sql)

# Track aliases and sources
alias_map = {}
excluded_aliases = set()
source_records = set()

def normalize_identifier(identifier):
    return identifier.replace('"', '').replace('[', '').replace(']', '').lower()

# Track all CTEs
for cte in parsed.find_all(exp.CTE):
    excluded_aliases.add(normalize_identifier(cte.alias_or_name))

# Recursively process all FROM/JOIN expressions
def process_from_clause(expr):
    if isinstance(expr, exp.Subquery):
        excluded_aliases.add(normalize_identifier(expr.alias_or_name))
        # Also explore nested subqueries
        for nested_from in expr.find_all(exp.From):
            for source in nested_from.expressions:
                process_from_clause(source)

    elif isinstance(expr, exp.Table):
        alias = normalize_identifier(expr.alias_or_name)
        table = normalize_identifier(expr.name)
        if alias in excluded_aliases or table in excluded_aliases:
            return
        if isinstance(expr.parent, exp.Func):  # Skip table functions
            return

        db_expr = expr.args.get("catalog")
        schema_expr = expr.args.get("db")
        db = normalize_identifier(db_expr.name) if db_expr else None
        schema = normalize_identifier(schema_expr.name) if schema_expr else None

        # Infer from metadata
        if db is None or schema is None:
            matches = df_columns[df_columns["Table Name"] == table]
            if not matches.empty:
                if db is None and matches["Database Name"].nunique() == 1:
                    db = matches.iloc[0]["Database Name"]
                if schema is None and matches["Schema Name"].nunique() == 1:
                    schema = matches.iloc[0]["Schema Name"]

        alias_map[alias] = (db, schema, table)

# Collect all FROM and JOIN expressions
for node in parsed.walk():
    if isinstance(node, (exp.From, exp.Join)):
        for expr in node.args.get("expressions", []):
            process_from_clause(expr)
        if isinstance(node, exp.Join):
            process_from_clause(node.this)

# Walk all columns, including wildcards
for node in parsed.walk():
    if isinstance(node, exp.Column):
        col = normalize_identifier(node.name)
        alias = normalize_identifier(node.table) if node.table else None

        if alias in excluded_aliases:
            continue

        if alias and alias in alias_map:
            db, schema, table = alias_map[alias]
            source_records.add((db, schema, table, col))
        elif not alias:
            matches = df_columns[df_columns["Column Name"] == col]
            for _, row in matches.iterrows():
                source_records.add((
                    row["Database Name"],
                    row["Schema Name"],
                    row["Table Name"],
                    row["Column Name"]
                ))

    elif isinstance(node, exp.Star):  # Expand wildcard (*)
        alias = normalize_identifier(node.table) if node.table else None
        if alias and alias in alias_map:
            db, schema, table = alias_map[alias]
            matches = df_columns[
                (df_columns["Database Name"] == db) &
                (df_columns["Schema Name"] == schema) &
                (df_columns["Table Name"] == table)
            ]
            for _, row in matches.iterrows():
                source_records.add((db, schema, table, row["Column Name"]))
        elif not alias:  # Global wildcard
            for _, row in df_columns.iterrows():
                source_records.add((
                    row["Database Name"],
                    row["Schema Name"],
                    row["Table Name"],
                    row["Column Name"]
                ))

# Deduplicated result
df_result = pd.DataFrame(sorted(source_records), columns=["Database", "Schema", "Table", "Column"])
print(df_result)
