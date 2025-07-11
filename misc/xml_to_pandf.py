import xml.etree.ElementTree as ET
import pandas as pd

def xml_to_dataframe_columns(xml_data: str) -> pd.DataFrame:
    """
    Converts XML data containing field names into a pandas DataFrame
    with those field names as columns.

    Args:
        xml_data (str): A string containing the XML data.

    Returns:
        pd.DataFrame: A pandas DataFrame with columns extracted from the XML.
                      The DataFrame will initially be empty (no rows).
    """
    try:
        # Parse the XML data from the string
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return pd.DataFrame() # Return an empty DataFrame on error

    column_names = []
    # Find all 'field' elements and then 'name' within each field
    for field in root.findall('field'):
        name_element = field.find('name')
        if name_element is not None:
            column_names.append(name_element.text)

    # You specified additional columns 'ID' and 'PROCESS_NAME' that are not in the provided XML.
    # We will prepend these to the list of columns extracted from XML.
    # If these were meant to be extracted from the XML, please provide an example of how they appear in the XML.
    fixed_columns = ['ID', 'PROCESS_NAME']
    
    # Filter out 'DS100' as it's not in your desired layout, and include only the others
    # This assumes that DS100 should always be excluded. If the exclusion rule is more complex,
    # we might need a more sophisticated filtering mechanism.
    final_column_names = fixed_columns + [col for col in column_names if col != 'DS100']


    # Create an empty pandas DataFrame with the extracted column names
    df = pd.DataFrame(columns=final_column_names)
    
    return df

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

# Convert to DataFrame
df_columns = xml_to_dataframe_columns(xml_data_string)

# Print the DataFrame to see the columns
print("DataFrame with extracted columns:")
print(df_columns)
print("\nDataFrame columns:")
print(df_columns.columns.tolist())