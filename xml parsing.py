from lxml import etree

def is_disabled(node):
    gui_settings = node.find(".//GuiSettings")
    return gui_settings is not None and gui_settings.attrib.get("Enabled") == "False"

def traverse_and_collect_sql(node, is_parent_disabled=False):
    sql_results = []

    # Determine if this node or any ancestor is disabled
    this_disabled = is_disabled(node)
    disabled = is_parent_disabled or this_disabled

    # If this is a tool node, extract SQL if not disabled
    if node.tag == "Node" and not disabled:
        tool_id = node.attrib.get("ToolID")
        config = node.find(".//Properties/Configuration")

        if config is not None:
            for sql_tag in ['Sql', 'InitialSQL', 'Query', 'PreSQL', 'PostSQL']:
                sql_element = config.find(f".//{sql_tag}")
                if sql_element is not None and sql_element.text and sql_element.text.strip():
                    sql_text = sql_element.text.strip()
                    sql_results.append((tool_id, sql_tag, sql_text))

    # Recursively process child nodes
    for child in node:
        sql_results.extend(traverse_and_collect_sql(child, disabled))

    return sql_results

def extract_sql_from_alteryx_xml(xml_path):
    tree = etree.parse(xml_path)
    root = tree.getroot()
    return traverse_and_collect_sql(root)

# Example usage
file_path = "path/to/your/workflow.yxmd"
sql_results = extract_sql_from_alteryx_xml(file_path)

for tool_id, sql_tag, sql in sql_results:
    print(f"Tool ID: {tool_id}, Tag: {sql_tag}\nSQL:\n{sql}\n{'-'*40}")
