import sqlglot
from sqlglot import parse_one, exp
import pandas as pd

sql = """
WITH a AS (
    SELECT h.hi, h.hj, h.hk FROM mg.hope h
),
b AS (
    SELECT p.pr, p.pz, a.hi FROM mg.pro p JOIN a ON a.hi = p.hi
)
SELECT * 
FROM (
    SELECT b.*, ROW_NUMBER() OVER (PARTITION BY b.hi ORDER BY b.pr) as rn 
    FROM b
) final
"""

# Reference metadata (used for disambiguating unqualified columns)
df_columns = pd.DataFrame({
    "Database Name": ["mg"] * 5 + ["mg"] * 3,
    "Schema Name": ["mg"] * 8,
    "Table Name": ["hope"] * 3 + ["pro"] * 3 + ["hope", "pro"],
    "Column Name": ["hi", "hj", "hk", "pr", "pz", "hi", "hj", "pr"]
}).astype(str).apply(lambda col: col.str.strip().str.lower())

parsed = parse_one(sql)
alias_map = {}
cte_names = set()


def process_ctes(expression):
    """Extract CTEs recursively and register their names to skip them as real tables"""
    for cte in expression.find_all(exp.CTE):
        cte_name = cte.alias_or_name.lower()
        cte_names.add(cte_name)
        process_from_expression(cte.this, cte_name)  # Process body of the CTE


def process_from_expression(expr, parent_alias=None):
    """Process subqueries, joins, and tables to build alias_map"""
    if isinstance(expr, exp.Subquery):
        alias = expr.alias_or_name
        sub_select = expr.unnest()
        visible_cols = []

        for proj in sub_select.expressions:
            if isinstance(proj, exp.Alias):
                col_name = proj.alias
                col_expr = proj.this
            elif isinstance(proj, exp.Column):
                col_name = proj.name
                col_expr = proj
            else:
                continue

            source_table = col_expr.table
            if source_table in alias_map:
                entries = alias_map[source_table]
                if isinstance(entries, list):
                    for c, db, schema, table in entries:
                        if c == col_name.lower():
                            visible_cols.append((col_name.lower(), db, schema, table))
                            break
                else:
                    db, schema, table = entries
                    visible_cols.append((col_name.lower(), db, schema, table))

        alias_map[alias] = visible_cols
        process_from_expression(sub_select)

    elif isinstance(expr, exp.Table):
        alias = expr.alias_or_name
        table = expr.name.lower()
        schema = expr.args.get("db")
        db = expr.args.get("catalog")
        db = db.name.lower() if db else None
        schema = schema.name.lower() if schema else None

        if table in cte_names:
            return  # Skip CTEs

        # Try resolving from column reference if missing db/schema
        matches = df_columns[df_columns["Table Name"] == table]
        if not matches.empty:
            if db is None and matches["Database Name"].nunique() == 1:
                db = matches.iloc[0]["Database Name"]
            if schema is None and matches["Schema Name"].nunique() == 1:
                schema = matches.iloc[0]["Schema Name"]

        alias_map[alias] = (db, schema, table)

    # Process nested FROMs and JOINs
    for child in expr.find_all(exp.From) + list(expr.find_all(exp.Join)):
        for source in child.args.get("expressions", [child.this]):
            process_from_expression(source)


# Process CTEs first
process_ctes(parsed)

# Process main FROM and JOINs
for from_expr in parsed.find_all(exp.From):
    for source in from_expr.args.get("expressions", []):
        process_from_expression(source)

for join_expr in parsed.find_all(exp.Join):
    process_from_expression(join_expr.this)

# Extract all column references
records = set()

for node in parsed.walk():
    if isinstance(node, exp.Column):
        col = node.name.strip().lower()
        alias = node.table

        if alias and alias in alias_map:
            entry = alias_map[alias]
            if isinstance(entry, list):  # Subquery alias
                for c, db, schema, table in entry:
                    if c == col:
                        records.add((db, schema, table, col))
                        break
            else:
                db, schema, table = entry
                records.add((db, schema, table, col))
        elif not alias:
            # Unqualified column: check against df_columns
            matches = df_columns[df_columns["Column Name"] == col]
            for _, row in matches.iterrows():
                records.add((
                    row["Database Name"],
                    row["Schema Name"],
                    row["Table Name"],
                    row["Column Name"]
                ))

# Output
df_result = pd.DataFrame(sorted(records), columns=["Database", "Schema", "Table", "Column"])
print(df_result)
