import pandas as pd
from itertools import combinations
from collections import defaultdict

class MVDgenerator:
    def __init__(self, df,attributes):
        self.df = df
        self.attributes = attributes

    def is_trivial_mvd(self, determinant, dependent_set):
        """
        Check if an MVD is trivial based on the conditions.
        """
        if dependent_set.issubset(determinant):
            return True
        if determinant.union(dependent_set) == self.attributes:
            return True
        return False

    def remove_unnecessary_attributes(self, df, determinant):
        """
        Group by the determinant and remove attributes that are constant for each unique determinant value.
        """
        grouped_df = df.groupby(list(determinant))
        removable_columns = []
        
        for col in df.columns:
            if col not in determinant:
                # Check if column has the same value across all groups
                unique_values = grouped_df[col].nunique()
                if unique_values.eq(1).all():
                    removable_columns.append(col)
        
        # Drop the removable columns from the DataFrame
        reduced_df = df.drop(columns=removable_columns)
        return reduced_df

    def validate_each_mvd(self, determinant, dependent_sets):
        """
        Validate MVDs using the provided method.
        """
        determinant_columns = list(determinant)
        dependent1_columns, dependent2_columns = dependent_sets
        dependent1_columns = list(dependent1_columns)
        dependent2_columns = list(dependent2_columns)

        # Sort the DataFrame by determinant, dependent set 1, and then dependent set 2
        sorted_df = self.df.sort_values(by=determinant_columns + dependent1_columns + dependent2_columns)

        # Initialize storage for the sets of dependent set 2 values
        value_map = defaultdict(lambda: defaultdict(set))

        # Collect sets of dependent set 2 values for each unique determinant and dependent set 1 combination
        for _, row in sorted_df.iterrows():
            key = tuple(row[col] for col in determinant_columns)
            dep1_tuple = tuple(row[col] for col in dependent1_columns)
            dep2_value = tuple(row[col] for col in dependent2_columns)

            # Store the values in a set for comparison
            value_map[key][dep1_tuple].add(dep2_value)

        # Validate the sets
        for key, dep1_map in value_map.items():
            # For each dependent set 1 combination, check if all dependent set 2 sets are the same
            reference_set = None
            for dep1_tuple, dep2_set in dep1_map.items():
                if reference_set is None:
                    reference_set = dep2_set
                elif reference_set != dep2_set:
                    print(f"MVD condition violated for determinant '{key}', dependent set 1 '{dep1_tuple}'")
                    return False

        print("All MVD conditions hold.")
        return True 

    def find_and_validate_all_mvds(self):
        """
        Generate and validate all possible non-trivial MVDs X ->-> Y | Z.
        """
        mvds = []
        
        # Generate all possible determinants
        for k in range(1, len(self.attributes)):  # Loop over subset sizes for determinant
            for determinant in combinations(self.attributes, k):
                determinant = set(determinant)
                
                # Reduce the DataFrame by removing unnecessary attributes based on the current determinant
                reduced_df = self.remove_unnecessary_attributes(self.df, determinant)
                remaining_attributes = set(reduced_df.columns) - determinant
                
                # Generate possible (Y, Z) splits for remaining attributes
                for i in range(1, len(remaining_attributes)):
                    for dependent1 in combinations(remaining_attributes, i):
                        dependent1 = set(dependent1)
                        dependent2 = remaining_attributes - dependent1
                        
                        # Check if the MVD is trivial
                        if self.is_trivial_mvd(determinant, dependent1) or self.is_trivial_mvd(determinant, dependent2):
                            print(f"Trivial MVD skipped: {determinant} ->-> {dependent1} | {dependent2}")
                            continue
                        
                        # Validate the MVD
                        if self.validate_each_mvd(determinant, (dependent1, dependent2)):
                            print(f"Non-trivial MVD holds: {determinant} ->-> {dependent1} | {dependent2}")
                            mvds.append((determinant, dependent1, dependent2))
        
        if not mvds:
            print("No non-trivial MVDs found.")
        return mvds

