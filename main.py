import pandas as pd
from Relation import Relation
from NormalizationManager import NormalizationManager  # Import the NormalizationManager class
from copy import deepcopy
from MVDgenerator import MVDgenerator

def process_composite_key_array(cks_input):
    print(f"Processing composite keys: {cks_input}")
    composite_keys = []
    if cks_input:  # Ensure there is input to process
        for ck in cks_input.split('}'):
            ck = ck.strip().strip(',{}')  # Strip outer braces and any trailing commas
            if ck:  # Ensure non-empty strings are processed
                cleaned_ck = {key.strip() for key in ck.split(',')}
                if cleaned_ck:  # Avoid adding empty sets
                    composite_keys.append(cleaned_ck)
    print(f"Processed composite keys: {composite_keys}")
    return composite_keys


def process_fd_mvd_input(fd_mvd_input):
    """
    Process the input strings to identify Functional Dependencies (FDs) and Multivalued Dependencies (MVDs).
    An FD is identified by '->', and an MVD is identified by '-->>'.
    Ensure that each element in the sets is stripped of leading and trailing spaces.
    """
    fd_list = []
    mvd_list = []

    for line in fd_mvd_input:
        line = line.strip()
        if '-->>' in line:  # Multivalued Dependency
            determinant, dependent = line.split('-->>')
            # Strip spaces and process the determinant and dependent sets
            determinant_set = set(elem.strip() for elem in determinant.strip().lstrip('{').rstrip('}').split(','))
            # Create a single set containing all dependent elements across multiple subsets if applicable
            dependent_elements = dependent.split('|')
            combined_dependent_list= []
            
            for elem_group in dependent_elements:
                elem_set = set(elem.strip() for elem in elem_group.strip().lstrip('{').rstrip('}').split(','))
                combined_dependent_list.append(elem_set)  # Combine all elements into one set
            mvd_list.append((determinant_set, combined_dependent_list))
        elif '-->' in line:  # Functional Dependency
            determinant, dependent = line.split('-->')
            # Strip spaces and process the determinant and dependent sets
            determinant_set = set(elem.strip() for elem in determinant.strip().lstrip('{').rstrip('}').split(','))
            dependent_set = set(elem.strip() for elem in dependent.strip().lstrip('{').rstrip('}').split(','))
            fd_list.append((determinant_set, dependent_set))

    return fd_list, mvd_list

def get_relation_input(file_path):
    print(f"Reading Excel file from path: {file_path}")
    
    # Read the entire file into a DataFrame, replacing any NaN values with empty strings
    df = pd.read_excel(file_path, header=None).fillna('')
    print(f"File read successfully. DataFrame shape: {df.shape}")
    
    # Extract the normalization level from the first row, first element
    normalization_level = df.iloc[0, 0]
    print(f"Normalization Level: {normalization_level}")

    # Find rows that contain '-Rowend-' in the first column
    rowend_rows = df.index[df[0] == '-Rowend-'].tolist()
    print(f"Rows containing '-Rowend-': {rowend_rows}")

    # If no '-Rowend-' rows are found, assume the entire file is one section
    if not rowend_rows:
        rowend_rows = [len(df)]  # Assume data continues to the last row of the file
        print(f"No '-Rowend-' found. Assuming data continues till row: {len(df)}")

    # Table name and attributes
    table_name = df.iloc[1, 0].strip()  # Ensure table name is stripped of spaces
    print(f"Table Name: {table_name}")
    
    attribute_row = df.iloc[2, :].str.strip()  # Strip any extra spaces from attribute names
    attributes = {attr for attr in attribute_row.dropna()}
    print(f"Attributes: {attributes}")

    # Data section (Assuming data starts at row 3 and ends at first '-Rowend-' row)
    data = pd.read_excel(file_path, skiprows=2, nrows=rowend_rows[0] - 3, header=0).fillna('')
    print(f"Data extracted. DataFrame shape: {data.shape}")
    
    # Check if we have enough rows to extract the primary key and other keys
    if rowend_rows[0] + 3 < len(df):
        primary_key = set(elem.strip() for elem in df.iloc[rowend_rows[0] + 1, 0].strip('{} ').split(','))  # Trim keys properly
        print(f"Primary Key (as a set): {primary_key}")
        
        candidate_keys_input = df.iloc[rowend_rows[0] + 2, 0]
        print(f"Raw Candidate Keys: {candidate_keys_input}")

        if candidate_keys_input.strip()=='None':
            candidate_keys=[]
        else:
            candidate_keys = process_composite_key_array(candidate_keys_input)

        multivalued_attributes = [mv.strip() for mv in df.iloc[rowend_rows[0] + 3, 0].split(',')]
        print(f"Multivalued Attributes: {multivalued_attributes}")

        # Process FD and MVD information after '-Rowend-'
        fd_mvd_input = df.iloc[rowend_rows[0] + 4:, 0].tolist()
        fds, mvds = process_fd_mvd_input(fd_mvd_input)

        # Creating the Relation object with FDs and MVDs
        relation = Relation(table_name, attributes, primary_key, candidate_keys, multivalued_attributes, data)
        relation.base_relation=relation

        # Adding FDs to the relation
        for fd in fds:
            relation.add_fd(fd[0], fd[1])

        # Adding MVDs to the relation
        for mvd in mvds:
            relation.add_mvd(mvd[0], mvd[1])

        print(f"Relation object created with Normalization Level: {normalization_level}")
        
        return relation, normalization_level
    else:
        print("Not enough rows in the DataFrame to process primary key, candidate keys, or multivalued attributes.")
        return None, None

if __name__ == "__main__":
    file_path = '4NF_InputFile_1.xlsx'
    print(f"Starting relation extraction for file: {file_path}")
    
    # Get the relation and normalization level from the input file
    relation, normalization_level = get_relation_input(file_path)
    mvdgen=MVDgenerator(relation.df,relation.attributes)
    mvdgen.find_and_validate_all_mvds()
    if relation:
        # Create the NormalizationManager and pass the relation and normalization level
        manager = NormalizationManager([relation], normalization_level)
        
        # Normalize the relations
        normalized_relations = manager.normalize()
        
        # Print the final list of normalized relations
        print("\nFinal List of Relations after Normalization:")
        for rel in normalized_relations:
            print(" \n")
            print(rel)

