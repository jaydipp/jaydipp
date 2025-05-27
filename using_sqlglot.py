import sqlglot
from sqlglot import parse_one, expressions as exp
import pandas as pd

# Sample SQL query
sql = """
SELECT 
    a.loan_number as ln, 
    b.letter_date, 
    c.dpd, 
    case when a.fpb > 0 then 'Y' else N end as zerofpb, 
    case when letter_date > npdd then 'Y' else 'N' end as letter_sent,
    max(npdd) as last_npdd
from mg.loan a 
left join mg.letter b on a.loan_number = b.loan_number
left join (select loan_number, dpd from delq) c on a.loan_number = c.loan_number
where letter_id = '12345'
group by a.loan_number, 
    b.letter_date, 
    c.dpd, 
    a.fpb
"""

# Reference column metadata
df_columns = pd.DataFrame({
    "Database Name": ["mg", "mg", "mg", "mg", "mg", "mg", "mg","mg"],
    "Table Name":    ["loan", "loan", "loan", "letter", "letter", "delq", "delq","letter"],
    "Column Name":   ["loan_number", "fpb", "npdd", "loan_number", "letter_date", "loan_number", "dpd","letter_id"]
})

# Normalize for matching
df_columns = df_columns.astype(str).apply(lambda col: col.str.strip().str.lower())

# Parse SQL
parsed = parse_one(sql)

# Build alias map: alias -> (database, table)
alias_map = {}
for table_expr in parsed.find_all(exp.Table):
    alias = table_expr.alias_or_name
    db_expr = table_expr.args.get("db")
    db = db_expr.name.lower() if db_expr else None
    table = table_expr.name.lower()

    # Try to infer DB from df_columns if missing
    if db is None:
        matches = df_columns[df_columns["Table Name"] == table]
        if not matches.empty and matches["Database Name"].nunique() == 1:
            db = matches.iloc[0]["Database Name"]

    alias_map[alias] = (db, table)

# Extract column references
records = set()

for node in parsed.walk():
    if isinstance(node, exp.Column):
        col = node.name.strip().lower()
        alias = node.table

        if alias:  # Qualified column
            if alias in alias_map:
                db, table = alias_map[alias]
                records.add((db, table, col))
        else:  # Unqualified column — use df_columns lookup
            matches = df_columns[df_columns["Column Name"] == col]
            if not matches.empty:
                for _, row in matches.iterrows():
                    records.add((
                        row["Database Name"],
                        row["Table Name"],
                        row["Column Name"]
                    ))

# Final deduplicated DataFrame
df_result = pd.DataFrame(sorted(records), columns=["Database", "Table", "Column"])
print(df_result)
