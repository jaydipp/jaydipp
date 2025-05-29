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
    """
    Process subqueries, joins, and tables to build alias_map.
    Handles expansion of alias.* wildcards by copying columns from underlying alias.
    """
    if isinstance(expr, exp.Subquery):
        alias = expr.alias_or_name
        sub_select = expr.unnest()

        # Local alias map for subquery columns, to resolve qualified * inside projections
        local_alias_map = {}

        # Process FROM/JOIN inside this subquery first (build local alias map)
        for from_expr in sub_select.find_all(exp.From):
            for source in from_expr.args.get("expressions", []):
                process_from_expression(source)
        for join_expr in sub_select.find_all(exp.Join):
            process_from_expression(join_expr.this)

        # Copy global alias_map into local_alias_map for subquery column resolution
        local_alias_map.update(alias_map)

        visible_cols = []

        for proj in sub_select.expressions:
            # Handle unqualified * — skip (do not expand)
            if isinstance(proj, exp.Star):
                continue

            # Handle qualified star like alias.*
            if isinstance(proj, exp.Column) and proj.name == "*" and proj.table:
                star_alias = proj.table.lower()
                if star_alias in local_alias_map:
                    star_entries = local_alias_map[star_alias]
                    if isinstance(star_entries, list):
                        visible_cols.extend(star_entries)
                    else:
                        # base table - optionally expand columns from df_columns if needed
                        # but here we just skip
                        pass
                continue

            if isinstance(proj, exp.Alias):
                col_alias = proj.alias.lower()
                col_expr = proj.this
            elif isinstance(proj, exp.Column):
                col_alias = proj.name.lower()
                col_expr = proj
            else:
                # ignore complex expressions (functions, literals)
                continue

            source_table_alias = None
            if isinstance(col_expr, exp.Column):
                source_table_alias = col_expr.table.lower() if col_expr.table else None

            if source_table_alias and source_table_alias in local_alias_map:
                entry = local_alias_map[source_table_alias]
                if isinstance(entry, list):
                    for c, db, schema, table in entry:
                        if c == col_alias:
                            visible_cols.append((col_alias, db, schema, table))
                            break
                else:
                    db, schema, table = entry
                    visible_cols.append((col_alias, db, schema, table))

        alias_map[alias] = visible_cols

    elif isinstance(expr, exp.Table):
        alias = expr.alias_or_name
        table = expr.name.lower()
        schema_expr = expr.args.get("db")
        db_expr = expr.args.get("catalog")
        db = db_expr.name.lower() if db_expr else None
        schema = schema_expr.name.lower() if schema_expr else None

        # Skip CTEs as they are not source tables
        if table in cte_names:
            return

        # Infer missing db/schema from df_columns
        matches = df_columns[df_columns["Table Name"] == table]
        if not matches.empty:
            if db is None and matches["Database Name"].nunique() == 1:
                db = matches.iloc[0]["Database Name"]
            if schema is None and matches["Schema Name"].nunique() == 1:
                schema = matches.iloc[0]["Schema Name"]

        alias_map[alias] = (db, schema, table)

    # Recurse for nested FROM and JOIN clauses
    for child in list(expr.find_all(exp.From)) + list(expr.find_all(exp.Join)):
        # child.args.get("expressions") is list for FROM, not for JOIN, so handle carefully
        exprs = child.args.get("expressions")
        if exprs:
            for source in exprs:
                process_from_expression(source)
        else:
            # For JOIN, process child.this
            if hasattr(child, "this") and child.this:
                process_from_expression(child.this)

# Process CTEs first to register their names and build alias map
process_ctes(parsed)

# Process main FROM and JOIN clauses
for from_expr in parsed.find_all(exp.From):
    for source in from_expr.args.get("expressions", []):
        process_from_expression(source)
for join_expr in parsed.find_all(exp.Join):
    process_from_expression(join_expr.this)

# Extract column references from the full query
records = set()

for node in parsed.walk():
    if isinstance(node, exp.Column):
        col = node.name.strip().lower()
        alias = node.table

        if alias and alias in alias_map:
            entry = alias_map[alias]
            if isinstance(entry, list):  # Subquery or CTE alias
                for c, db, schema, table in entry:
                    if c == col:
                        records.add((db, schema, table, col))
                        break
            else:
                db, schema, table = entry
                records.add((db, schema, table, col))

        elif not alias:
            # Unqualified column: lookup in df_columns (can add logic to disambiguate if needed)
            matches = df_columns[df_columns["Column Name"] == col]
            for _, row in matches.iterrows():
                records.add((
                    row["Database Name"],
                    row["Schema Name"],
                    row["Table Name"],
                    row["Column Name"]
                ))

# Final DataFrame output
df_result = pd.DataFrame(sorted(records), columns=["Database", "Schema", "Table", "Column"])
print(df_result)
