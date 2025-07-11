import xml.etree.ElementTree as ET

def get_dataframe_column_list(xml_data: str) -> list:
    """
    Parses XML data to extract field names and combines them with
    fixed column names to produce a complete list of DataFrame columns.

    Args:
        xml_data (str): A string containing the XML data.

    Returns:
        list: A list of strings representing the desired DataFrame column names.
    """
    fixed_initial_columns = ['ID', 'PROCESS_NAME']
    extracted_ds_columns = []

    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        # Return default columns or handle error as appropriate
        return fixed_initial_columns 

    for field in root.findall('field'):
        name_element = field.find('name')
        if name_element is not None and name_element.text is not None:
            extracted_ds_columns.append(name_element.text)

    # Combine the fixed initial columns with the extracted DS columns.
    # The order will be ID, PROCESS_NAME, then DS100, DS200, etc., as they appear in XML.
    final_column_list = fixed_initial_columns + extracted_ds_columns

    return final_column_list

# Your XML data
xml_data_string = """
<bulkData>
<field>
<name>DS100</name>
</field>
<field>
<name>DS200</name>
</field>
<field>
<name>DS300</name>
</field>
<field>
<name>DS400</name>
</field>
<field>
<name>DS500</name>
</field>
</bulkData>
"""

# Get the list of column names
desired_column_list = get_dataframe_column_list(xml_data_string)

# Print the resulting list
print("Desired DataFrame Column List:")
print(desired_column_list)