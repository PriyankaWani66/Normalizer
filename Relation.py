from collections import defaultdict
from itertools import chain, combinations

class Relation:
    def __init__(self, tablename, attributes, pk, cks=None, MvalAttr=None, df=None, original=None, base_relation=None):
        self.tablename = tablename
        self.attributes = set(attributes) if isinstance(attributes, list) else attributes  # Convert to set if it’s a list
        self.pk = pk if isinstance(pk, set) else set(pk)  # Ensure pk is stored as a set
        self.cks = cks if cks is not None else []  # Ensure cks is always a list of sets
        self.MvalAttr = MvalAttr
        self.df = df
        self.fd_map = {}  # Functional Dependencies (FD) - Determinant -> Dependent
        self.mvd_map = {}  # Multivalued Dependencies (MVD) - Determinant -> List of dependent sets
        self.original = original
        self.base_relation = base_relation
        self.foreign_keys = []

         # Store foreign keys as a list of dictionaries for easy tracking
    def get_candidate_keys(self):
        """
    Return the candidate keys for this relation. Always return a list,
    even if no candidate keys exist.
    """
        return self.cks if self.cks is not None else []
    def add_fd(self, determinant_set, dependent_set):
        """
        Add a Functional Dependency (FD) to the FD map.
        If the determinant_set already exists in the map, append the new dependent_set
        to the existing one, rather than overwriting it.
        """
        print(f"Adding FD: {determinant_set} --> {dependent_set}")
        determinant_key = frozenset(determinant_set)

        if determinant_key in self.fd_map:
            print(f"Determinant {determinant_set} already exists. Appending {dependent_set} to current dependent set.")
            self.fd_map[determinant_key].update(dependent_set)
        else:
            self.fd_map[determinant_key] = dependent_set

    def add_mvd(self, determinant_set, dependent_list):
        """
        Add a Multivalued Dependency (MVD) to the MVD map.
        The determinant_set is a set of attributes that determine the dependent_list,
        where dependent_list is a list of sets (ilist).
        """
        print(f"Adding MVD: {determinant_set} -->> {dependent_list}")
        determinant_key = frozenset(determinant_set)

        # Initialize the list for this determinant if it does not exist
        if determinant_key not in self.mvd_map:
            self.mvd_map[determinant_key] = []
        
        # Function to check if a list of sets (ilist) is already present in the olist
        def contains_list_of_sets(olist, ilist):
            for sublist in olist:
                if all(a == b for a, b in zip(sublist, ilist)):
                    return True
            return False

        # Check if the dependent list is already in the list for this determinant
        if not contains_list_of_sets(self.mvd_map[determinant_key], dependent_list):
            self.mvd_map[determinant_key].append(dependent_list)
            print(f"Unique dependent list added for determinant {determinant_set}.")
        else:
            print(f"Dependent list already exists for determinant {determinant_set}.")

        print(f"Updated MVD map for {determinant_set}: {self.mvd_map[determinant_key]}")

    def add_fk(self, foreign_key, references):
        """
        Add a foreign key relationship to the relation.
        :param foreign_key: Set of attributes in this relation that form the foreign key.
        :param references: Tuple of (referenced_relation_name, set_of_referenced_attributes).
        """
        fk_entry = {
            'foreign_key': foreign_key,
            'references': references
        }
        self.foreign_keys.append(fk_entry)
        print(f"Added foreign key in relation '{self.tablename}', referencing '{references[0]}' with attributes {references[1]}")

    def copy(self):
        """
        Create a custom deep copy of the Relation object.
        This manually copies all relevant fields.
        """
        new_relation = Relation(
            tablename=self.tablename,
            attributes=self.attributes.copy(),
            pk=self.pk.copy(),  # Copy primary key as a set
            cks=[ck.copy() for ck in self.cks],  # Copy candidate keys as a list of sets
            MvalAttr=self.MvalAttr.copy(),  # Copy multivalued attributes
            df=self.df.copy(),  # Copy the dataframe
            original=self.original,
            base_relation=self.base_relation
        )

        # Deep copy the functional dependencies (FDs)
        new_relation.fd_map = {det: dep.copy() for det, dep in self.fd_map.items()}

        # Deep copy the multivalued dependencies (MVDs)
        new_relation.mvd_map = {det: [dep.copy() for dep in deps] for det, deps in self.mvd_map.items()}

        # Copy foreign keys
        new_relation.foreign_keys = [fk.copy() for fk in self.foreign_keys]

        return new_relation

    def get_primary_key_set(self):
        """
        Return the primary key as a set for the relation.
        """
        return self.pk

    def get_candidate_key_sets(self):
        """
        Return a list of candidate key sets for the relation.
        """
        return self.cks

    def get_all_keys(self):
     """
    Returns a set of all prime attribute keys (attributes that are part of any primary or candidate key).
    """
    # Add primary key as frozenset if it’s not already
     all_keys = {frozenset(self.pk) if not isinstance(self.pk, frozenset) else self.pk}
    
    # Add each candidate key as frozenset if needed
     all_keys.update(frozenset(ck) if not isinstance(ck, frozenset) else ck for ck in self.get_candidate_keys())
    
     return all_keys

    
    def get_non_prime_attributes(self):
        """
    Return a set of non-prime attributes (attributes not part of any key).
    """
    # Ensure self.attributes is a set
        if isinstance(self.attributes, list):
            self.attributes = set(self.attributes)
        
        prime_attributes = set().union(*self.get_all_keys())  # Flatten all prime attributes into a set
        non_prime_attributes = self.attributes - prime_attributes
        return non_prime_attributes
    
    def validate_each_mvd(self, determinant, dependent_sets):
        """
        Validate MVDs by sorting and collecting sets dynamically based on determinant and dependent sets.
        
        Parameters:
        determinant (tuple): The determinant attributes.
        dependent_sets (list of tuples): List of tuples of dependent attributes.
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



    def __str__(self):
        return (f"Table Name: {self.tablename}\n"
                f"Attributes: {self.attributes}\n"
                f"Primary Key: {self.pk}\n"
                f"Candidate Keys: {self.cks}\n"
                f"Multivalued Attributes: {self.MvalAttr}\n"
                f"Functional Dependencies: {self.fd_map}\n"
                f"Multivalued Dependencies: {self.mvd_map}\n"
                f"Foreign Keys: {self.foreign_keys}")

    def generate_textual_representation(self, output_file, normalization_step=None):
        """
        Generate a textual representation of this relation and append it to a single output file.
        """
        output = []

        # Add normalization step as a header
        if normalization_step:
            output.append(f"--- {normalization_step} ---")

        # Table name
        output.append(f"Table: {self.tablename}")

        # Attributes
        output.append(f"- Attributes: {', '.join(self.attributes)}")

        # Primary Key
        pk_str = ', '.join(self.pk)
        output.append(f"- Primary Key: {pk_str}")

        # Candidate Keys
        if self.cks:
            cks_str = ', '.join([' & '.join(ck) for ck in self.cks])
            output.append(f"- Candidate Keys: {cks_str}")
        else:
            output.append(f"- Candidate Keys: None")

        # Multivalued Attributes
        output.append(f"- Multivalued Attributes: {self.MvalAttr if self.MvalAttr else 'None'}")

        # Functional Dependencies
        if self.fd_map:
            fd_output = [f"{set(det)} -> {set(dep)}" for det, dep in self.fd_map.items()]
            output.append(f"- Functional Dependencies: {', '.join(fd_output)}")
        else:
            output.append("- Functional Dependencies: None")

        # Multivalued Dependencies
        if self.mvd_map:
            mvd_output = [f"{set(det)} -->> {tuple(dep)}" for det, dep in self.mvd_map.items()]
            output.append(f"- Multivalued Dependencies: {', '.join(mvd_output)}")
        else:
            output.append("- Multivalued Dependencies: None")

        # Foreign Keys
        if self.foreign_keys:
            fk_output = [f"{fk['foreign_key']} -> {fk['references']}" for fk in self.foreign_keys]
            output.append(f"- Foreign Keys: {', '.join(fk_output)}")
        else:
            output.append("- Foreign Keys: None")

        # Add a blank line after each relation for readability
        output.append("")

        # Write the output to the specified file, appending each relation's details
        with open(output_file, "a") as file:
            file.write("\n".join(output) + "\n")
            print(f"Textual representation for {self.tablename} after {normalization_step} appended to {output_file}")
    def get_prime_attribute_subsets(self):
        """
        Returns all non-empty subsets of prime attributes (attributes that are part of any primary or candidate key).
        """
        prime_attributes = set().union(*self.get_all_keys())  # Combine all prime keys directly as sets
        return list(chain.from_iterable(
        combinations(prime_attributes, r) for r in range(1, len(prime_attributes) + 1)
        ))