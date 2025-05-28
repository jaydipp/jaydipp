import sqlglot
from sqlglot import parse_one, expressions as exp
import pandas as pd

# Sample SQL with CTE and subquery
sql = """
WITH recent_loans AS (
    SELECT loan_number, process_dt 
    FROM ip.mg_loan_table 
    WHERE process_dt > '2023-01-01'
),
latest_letters AS (
    SELECT loan_number, MAX(letter_date) AS last_letter_date
    FROM ip.mg_letter_table
    GROUP BY loan_number
)
SELECT 
    a.loan_number, 
    a.process_dt,
    b.last_letter_date,
    CASE 
        WHEN b.last_letter_date > a.process_dt THEN 'Y' 
        ELSE 'N' 
    END AS is_recent
FROM recent_loans a
LEFT JOIN latest_letters b ON a.loan_number = b.loan_number
WHERE a.process_dt IS NOT NULL
"""

# Reference column metadata
df_columns = pd.DataFrame({
    "Database Name": ["ip"] * 6,
    "Schema Name":   ["mg"] * 6,
    "Table Name":    ["loan_table", "loan_table", "loan_table", "letter_table", "letter_table", "letter_table"],
    "Column Name":   ["loan_number", "process_dt", "amount", "loan_number", "letter_date", "letter_id"]
}).astype(str).apply(lambda col: col.str.strip().str.lower())

# Helper: normalize column lookup
col_lookup = df_columns.groupby("Column Name")[["Database Name", "Schema Name", "Table Name"]].apply(lambda x: x.to_records(index=False).tolist()).to_dict()

# Parse SQL
parsed = parse_one(sql)

# Recursive function to walk through expressions and extract columns
def extract_columns(expr, alias_map=None, cte_map=None):
    if alias_map is None:
        alias_map = {}
    if cte_map is None:
        cte_map = {}

    records = set()

    def update_alias_map(expr):
        local_map = {}
        for table_expr in expr.find_all(exp.Table):
            alias = table_expr.alias_or_name or table_expr.name
            db_expr = table_expr.args.get("db")
            schema_expr = table_expr.args.get("catalog")
            db = db_expr.name.lower() if db_expr else None
            schema = schema_expr.name.lower() if schema_expr else None
            table = table_expr.name.lower()

            local_map[alias] = (db, schema, table)
        return local_map

    if isinstance(expr, exp.With):
        for cte in expr.ctes:
            cte_map[cte.alias_or_name] = cte.this
        expr = expr.this

    alias_map.update(update_alias_map(expr))

    for node in expr.walk():
        if isinstance(node, exp.Subquery):
            # Recurse into subquery
            records |= extract_columns(node.unnest(), alias_map.copy(), cte_map)
        elif isinstance(node, exp.CTE):
            continue
        elif isinstance(node, exp.Column):
            col = node.name.lower()
            tbl_alias = node.table

            if tbl_alias:
                if tbl_alias in alias_map:
                    db, schema, table = alias_map[tbl_alias]
                    if db and table:
                        records.add((db, schema, table, col))
            else:
                # Unqualified column
                matches = col_lookup.get(col)
                if matches and len(matches) == 1:
                    db, schema, table = matches[0]
                    records.add((db, schema, table, col))

    # Handle CTEs
    for alias, cte_expr in cte_map.items():
        records |= extract_columns(cte_expr, alias_map.copy(), cte_map)

    return records

# Run extraction
columns = extract_columns(parsed)

# Final DataFrame
df_result = pd.DataFrame(columns, columns=["Database", "Schema", "Table", "Column"])
df_result = df_result.drop_duplicates().sort_values(by=["Database", "Schema", "Table", "Column"]).reset_index(drop=True)

print(df_result)
