import re
from collections import defaultdict, deque

def convert_temp_tables_to_ctes(tsql_script: str) -> str:
    warnings = []
    variables = {}

    # --- Step 0: Extract and replace scalar variables ---
    # DECLARE @var TYPE = value;
    declare_assignments = re.findall(
        r"DECLARE\s+@(\w+)\s+\w+(?:\s*=\s*('[^']*'|\d+))?",
        tsql_script,
        flags=re.IGNORECASE
    )
    for var_name, value in declare_assignments:
        if value:
            variables[f"@{var_name}"] = value

    # SET @var = value;
    set_assignments = re.findall(
        r"SET\s+@(\w+)\s*=\s*('[^']*'|\d+)",
        tsql_script,
        flags=re.IGNORECASE
    )
    for var_name, value in set_assignments:
        variables[f"@{var_name}"] = value

    # Remove DECLARE and SET statements
    tsql_script = re.sub(
        r"DECLARE\s+@(\w+)\s+\w+(\s*=\s*('[^']*'|\d+))?;",
        '',
        tsql_script,
        flags=re.IGNORECASE
    )
    tsql_script = re.sub(
        r"SET\s+@(\w+)\s*=\s*('[^']*'|\d+);",
        '',
        tsql_script,
        flags=re.IGNORECASE
    )

    # Replace scalar variables throughout script
    for var, value in variables.items():
        tsql_script = re.sub(r'\b' + re.escape(var) + r'\b', value, tsql_script, flags=re.IGNORECASE)

    # --- Step 1: Remove DROP TABLE statements with or without IF OBJECT_ID ---
    tsql_script = re.sub(
        r"IF\s+OBJECT_ID\s*\(\s*'[^']*?[#]{1,2}\w+'\s*\)\s+IS\s+NOT\s+NULL\s+DROP\s+TABLE\s+[#]{1,2}\w+;\s*",
        '',
        tsql_script,
        flags=re.IGNORECASE
    )
    tsql_script = re.sub(
        r"DROP\s+TABLE\s+[#]{1,2}\w+;\s*",
        '',
        tsql_script,
        flags=re.IGNORECASE
    )

    # --- Step 2: Find all temp table definitions ---

    # Pattern for SELECT INTO #Temp FROM ...
    select_into_pattern = re.compile(
        r"SELECT\s+(.*?)\s+INTO\s+(#|##)(\w+)\s+FROM\s+(.*?)(?=;\s*|\Z)",
        re.IGNORECASE | re.DOTALL
    )
    # Pattern for CREATE TABLE #Temp (...) + INSERT INTO #Temp (...) SELECT ...
    create_table_pattern = re.compile(
        r"CREATE\s+TABLE\s+(#|##)(\w+)\s*\((.*?)\);\s*",
        re.IGNORECASE | re.DOTALL
    )
    insert_into_pattern = re.compile(
        r"INSERT\s+INTO\s+(#|##)(\w+)\s*\((.*?)\)\s*SELECT\s+(.*?)\s+FROM\s+(.*?)(?=;\s*|\Z)",
        re.IGNORECASE | re.DOTALL
    )

    # Dict to store temp table definitions:
    # temp_name -> {'type': '#'/ '##', 'query': SQL string, 'depends': set()}
    temp_tables = {}

    # Find all SELECT INTO temp tables
    for match in select_into_pattern.finditer(tsql_script):
        columns = match.group(1).strip()
        temp_type = match.group(2)
        temp_name = match.group(3).strip()
        source = match.group(4).strip()

        full_name = temp_type + temp_name
        temp_tables[temp_name] = {
            'type': temp_type,
            'query': f"SELECT {columns} FROM {source}",
            'depends': set()
        }

    # Find all CREATE TABLE + INSERT INTO temp tables
    create_tables = {}
    for match in create_table_pattern.finditer(tsql_script):
        temp_type = match.group(1)
        temp_name = match.group(2).strip()
        create_tables[temp_name] = {
            'type': temp_type,
            'definition': match.group(3)
        }

    for match in insert_into_pattern.finditer(tsql_script):
        temp_type = match.group(1)
        temp_name = match.group(2).strip()
        insert_columns = match.group(3).strip()
        select_columns = match.group(4).strip()
        source = match.group(5).strip()

        if temp_name in create_tables:
            temp_tables[temp_name] = {
                'type': temp_type,
                'query': f"SELECT {select_columns} FROM {source}",
                'depends': set()
            }

    # --- Step 3: Analyze dependencies between temp tables ---
    temp_names = set(temp_tables.keys())

    for temp_name, info in temp_tables.items():
        query = info['query'].lower()
        dependencies = set()
        for other_temp in temp_names:
            if other_temp == temp_name:
                continue
            # Look for references to #other_temp or ##other_temp inside the query (including joins/subqueries)
            pattern = re.compile(rf"([#]{{1,2}}){re.escape(other_temp)}\b", re.IGNORECASE)
            if pattern.search(query):
                dependencies.add(other_temp)
        temp_tables[temp_name]['depends'] = dependencies

    # --- Step 4: Topological sort of temp tables based on dependencies ---

    def topo_sort(dep_dict):
        sorted_list = []
        visited = {}
        def visit(node):
            if node in visited:
                if visited[node] == 1:
                    raise ValueError(f"Circular dependency detected at {node}")
                return
            visited[node] = 1
            for dep in dep_dict[node]:
                visit(dep)
            visited[node] = 2
            sorted_list.append(node)
        for node in dep_dict:
            if node not in visited:
                visit(node)
        return sorted_list[::-1]  # reverse for correct order

    dependency_map = {k: v['depends'] for k,v in temp_tables.items()}
    try:
        sorted_temps = topo_sort(dependency_map)
    except ValueError as e:
        warnings.append(f"⚠️ {str(e)}. Cannot convert due to circular temp table dependencies.")
        sorted_temps = list(temp_tables.keys())  # fallback, no ordering

    # --- Step 5: Build CTEs in dependency order ---
    cte_list = []
    for temp_name in sorted_temps:
        cte_list.append(f"{temp_name} AS (\n    {temp_tables[temp_name]['query']}\n)")

    ctes_sql = ""
    if cte_list:
        ctes_sql = "WITH " + ",\n".join(cte_list) + "\n"

    # --- Step 6: Remove temp table creation and insertion and SELECT INTO from original script ---

    # Remove SELECT INTO statements for all temp tables
    tsql_script = select_into_pattern.sub('', tsql_script)

    # Remove CREATE TABLE statements for all temp tables
    tsql_script = create_table_pattern.sub('', tsql_script)

    # Remove INSERT INTO statements for all temp tables
    tsql_script = insert_into_pattern.sub('', tsql_script)

    # --- Step 7: Replace all references to temp tables (#Temp, ##Temp) with just CTE names ---
    for temp_name in temp_tables.keys():
        tsql_script = re.sub(
            rf"([#]{{1,2}}){re.escape(temp_name)}\b",
            temp_name,
            tsql_script,
            flags=re.IGNORECASE
        )

    # --- Step 8: Remove leftover DROP TABLE statements again ---
    tsql_script = re.sub(
        r"IF\s+OBJECT_ID\s*\(\s*'[^']*?[#]{1,2}\w+'\s*\)\s+IS\s+NOT\s+NULL\s+DROP\s+TABLE\s+[#]{1,2}\w+;\s*",
        '',
        tsql_script,
        flags=re.IGNORECASE
    )
    tsql_script = re.sub(
        r"DROP\s+TABLE\s+[#]{1,2}\w+;\s*",
        '',
        tsql_script,
        flags=re.IGNORECASE
    )

    # --- Step 9: Warn on table variables (skip converting) ---
    if re.search(r"DECLARE\s+@\w+\s+TABLE", tsql_script, re.IGNORECASE):
        warnings.append("⚠️ Table variables (@Table) detected — not converted to CTEs due to scope and mutability.")

    # --- Step 10: Assemble final script ---
    final_script = ctes_sql + tsql_script.strip()

    if warnings:
        final_script = "-- " + "\n-- ".join(warnings) + "\n\n" + final_script

    # Clean multiple blank lines
    final_script = re.sub(r'\n\s*\n', '\n\n', final_script)

    return final_script.strip()
