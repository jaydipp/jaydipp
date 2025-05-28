import sqlglot
from sqlglot import parse_one, expressions as exp
import pandas as pd

# Sample SQL query
sql = """
SELECT user_id, user_name 
FROM (
    SELECT p_date, user_id 
    FROM mg.users
) uid 
LEFT JOIN mg.user_info h 
    ON h.unumber = uid.user_id
"""

# Reference column metadata
df_columns = pd.DataFrame({
    "Database Name": ["ip"] * 8,
    "Schema Name": ["mg"] * 5 + ["mg"] * 3,
    "Table Name": ["users"] * 3 + ["user_info"] * 3 + ["users", "user_info"],
    "Column Name": [
        "p_date", "user_id", "user_name",
        "unumber", "user_name", "user_id",
        "p_date", "user_name"
    ]
})

# Normalize for matching
df_columns = df_columns.astype(str).apply(lambda col: col.str.strip().str.lower())

# Parse SQL
parsed = parse_one(sql)

# Helper: Build alias map and handle subqueries
alias_map = {}
cte_map = {}


def process_from_expression(expr, parent_alias=None):
    if isinstance(expr, exp.Subquery):
        alias = expr.alias_or_name
        sub_select = expr.unnest()
        visible_cols = []
        for proj in sub_select.expressions:
            if isinstance(proj, exp.Alias):
                visible_cols.append((proj.alias, proj.this))
            elif isinstance(proj, exp.Column):
                visible_cols.append((proj.name, proj))
        for col_name, col_expr in visible_cols:
            source_table = col_expr.table
            if source_table in alias_map:
                db, schema, table = alias_map[source_table]
                alias_map.setdefault(alias, []).append((col_name, db, schema, table))
        return

    if isinstance(expr, exp.Table):
        alias = expr.alias_or_name
        db_expr = expr.args.get("catalog")
        schema_expr = expr.args.get("db")
        db = db_expr.name.lower() if db_expr else None
        schema = schema_expr.name.lower() if schema_expr else None
        table = expr.name.lower()

        # Infer DB and Schema from df_columns if missing
        if db is None or schema is None:
            matches = df_columns[df_columns["Table Name"] == table]
            if not matches.empty:
                if db is None and matches["Database Name"].nunique() == 1:
                    db = matches.iloc[0]["Database Name"]
                if schema is None and matches["Schema Name"].nunique() == 1:
                    schema = matches.iloc[0]["Schema Name"]

        alias_map[alias] = (db, schema, table)


# Process FROM and JOIN clauses
for from_expr in parsed.find_all(exp.From):
    for source in from_expr.args.get("expressions", []):
        process_from_expression(source)

for join_expr in parsed.find_all(exp.Join):
    process_from_expression(join_expr.this)

# Extract column references
records = set()

for node in parsed.walk():
    if isinstance(node, exp.Column):
        col = node.name.strip().lower()
        alias = node.table

        if alias and alias in alias_map:
            entry = alias_map[alias]
            if isinstance(entry, list):  # Subquery
                for c, db, schema, table in entry:
                    if c == col:
                        records.add((db, schema, table, col))
                        break
            else:
                db, schema, table = entry
                records.add((db, schema, table, col))
        elif not alias:  # Unqualified column
            matches = df_columns[df_columns["Column Name"] == col]
            if not matches.empty:
                for _, row in matches.iterrows():
                    records.add((
                        row["Database Name"],
                        row["Schema Name"],
                        row["Table Name"],
                        row["Column Name"]
                    ))

# Final deduplicated DataFrame
df_result = pd.DataFrame(sorted(records), columns=["Database", "Schema", "Table", "Column"])
print(df_result)
