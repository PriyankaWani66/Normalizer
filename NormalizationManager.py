from OneNF import OneNF  # Import your normal form classes
from TwoNF import TwoNF
from ThreeNF import ThreeNF
from BCNF import BCNF
from FourNF import FourNF
from FiveNF import FiveNF
from Relation import Relation
from itertools import combinations

class NormalizationManager:
    def __init__(self, relations: list[Relation], normalization_level: str):
        self.relations = relations
        self.normalization_level = normalization_level
        self.normal_forms = [OneNF, TwoNF, ThreeNF, BCNF, FourNF, FiveNF]
        self.normal_form_names = ["1NF", "2NF", "3NF", "BCNF", "4NF", "5NF"]
        self.normal_form_map = {name: index for index, name in enumerate(self.normal_form_names)}
        self.base_relation = relations[0].copy() if relations else None  # Store a copied version of the first base relation

    def normalize(self, output_file="normalized_schema.txt"):
     """
    Normalize the current relations up to the specified normalization level.
    This method will return a list of relations after normalization, ensuring all relations
    adhere to the normalization form before proceeding to the next form.
    """
     required_level_index = self.normal_form_map[self.normalization_level]
     current_level_index = 0

    # Clear existing content in the output file to start fresh
     open(output_file, "w").close()

    # Ensure base relation is defined if relations exist
     base_relation = self.relations[0].copy() if self.relations else None

     while current_level_index <= required_level_index:
        current_nf_class = self.normal_forms[current_level_index]
        current_nf_name = self.normal_form_names[current_level_index]
        stable = False

        # Continue processing until no further changes are necessary
        while not stable:  # Stop renormalization if reaching 5NF
            stable = True
            new_relations = []
            for relation in self.relations:
                if not current_nf_class.isin(relation):
                    stable = False  # Changes are needed, stability not yet reached
                    normalized_relations = current_nf_class.normalise(relation)
                    # Attach the original relation reference to new relations
                    for nr in normalized_relations:
                        nr.base_relation = base_relation
                        nr.original = relation if not relation.original else relation.original
                    new_relations.extend(normalized_relations)
                else:
                    new_relations.append(relation)

            self.relations = new_relations  # Update relations to the newly processed list
            if (current_nf_name == "5NF"):
                break #Non need for renormalisation if it is 5NF


            # Output normalized relations to file after each stabilization attempt
        for relation in self.relations:
                relation.generate_textual_representation(output_file, normalization_step=current_nf_name)

        # If at 5NF, break out of the loop and finalize
        if current_nf_name == "5NF":
            print("Reached 5NF, final normalization achieved without further checks.")
            break

        # Clean redundant tables only for levels up to 4NF
        if current_level_index < self.normal_form_map["5NF"]:
            self.clean_redundant_tables(current_nf_name)

        current_level_index += 1  # Move to the next level of normalization

    # Assign foreign keys with priorities after final normalization step
     self.assign_foreign_keys_with_priorities()

    # Final output generation for the last normalization step
     for relation in self.relations:
        normal_form = next(key for key, value in self.normal_form_map.items() if value == required_level_index)
        relation.generate_textual_representation(output_file, normalization_step=normal_form)

     return self.relations
    def clean_redundant_tables(self, curr_nf_name: str):
        """ Clean tables that are redundant based on attribute sets. """
        used_attrs_sets = []  # Track attribute sets already used
        relations_to_remove = set()  # Relations that will be marked as redundant and removed

        # First pass: collect attribute sets from all relations
        print("Starting first pass to collect attribute sets from all relations...")
        for index, relation in enumerate(self.relations):
            relation_attrs = relation.attributes
            print(f"Collecting attributes for relation {relation.tablename}: {relation_attrs}")
            used_attrs_sets.append((index, relation, relation_attrs))  # Store index along with relation and attributes
        print("Collected user attribute set", used_attrs_sets)

        # Second pass: check for duplicate attribute sets using index matching
        print("Starting second pass to check for duplicate attribute sets...")
        for i, relation, relation_attrs in used_attrs_sets:
            for j, _, used_attrs in used_attrs_sets:
                if i != j and relation_attrs == used_attrs:
                    if i > j:  # Only mark the relation with the higher index as redundant
                        print(f"Marking redundant relation: {relation.tablename} with identical attributes (matched with relation at index {j}).")
                        relations_to_remove.add(relation)
                        break

        # Third pass: mark leftover tables that are subsets of other relations' attributes
        print("Starting third pass to mark leftover tables that are subsets of other relations...")
        for relation in self.relations:
            if relation.tablename.endswith(f"leftover_{curr_nf_name}"):
                relation_attrs = relation.attributes
                print("Leftover", relation_attrs)
                for _, _, used_attrs in used_attrs_sets:
                    if relation_attrs.issubset(used_attrs) and relation_attrs != used_attrs:
                        print(f"Marking leftover relation as redundant: {relation.tablename} (subset of attributes: {used_attrs})")
                        relations_to_remove.add(relation)
                        break
        

        # Remove the redundant relations

        print(f"Relations marked for removal: {[relation.tablename for relation in relations_to_remove]}")
        self.relations = [relation for relation in self.relations if relation not in relations_to_remove]
        print(f"Removed {len(relations_to_remove)} redundant relations.")
        
    def assign_foreign_keys_with_priorities(self):
     """
    Assign foreign keys by prioritizing subsets based on primary key size,
    non-prime attribute size, and the relation's position in `self.relations`.
    If the top-priority match references the same relation, the FK is left as None.
    """
     print("Starting prioritized foreign key assignment...")

     for relation in self.relations:
        assigned_foreign_keys = set()

        # Collect all primary and candidate keys from other relations
        primary_and_candidate_keys = [
            (index, other_relation, key_set)
            for index, other_relation in enumerate(self.relations)
            for key_set in [other_relation.pk] + other_relation.get_candidate_keys()
        ]

        # Sort based on priority: PK size, lexicographic PK, non-prime attr count, relation index
        primary_and_candidate_keys.sort(key=lambda x: (
            len(x[2]),                   # Primary key size
            str(x[2]),                   # Lexicographic order of primary key
            -len(x[1].get_non_prime_attributes()),  # Descending order of non-prime attribute count
            x[0]                         # Position in self.relations
        ))

        print("primary_and_candidate_keys:", primary_and_candidate_keys)

        # Iterate over subsets of relation attributes in increasing size
        for size in range(1, len(relation.attributes) + 1):
            for subset in combinations(relation.attributes, size):
                subset = frozenset(subset)

                # Skip if any existing FK is a subset of the current subset
                if any(fk['foreign_key'].issubset(subset) for fk in relation.foreign_keys):
                    continue

                # Process the sorted primary and candidate keys

                for _, other_relation, key_set in primary_and_candidate_keys:
                    if subset == key_set:
                        # Exact match: Check if subset is not already covered by an FK
                        if relation != other_relation:
                            relation.add_fk(foreign_key=subset, references=(other_relation.tablename, key_set))
                            assigned_foreign_keys.add(subset)
                            print(f"Assigned FK (Exact Match): {subset} in {relation.tablename} -> {other_relation.tablename} referencing {key_set}")
                        break  # Move to next subset if exact match is handled

                    elif subset.issubset(key_set):
                        # Subset match: Check if subset is not already covered by an FK
                        if relation != other_relation:
                            relation.add_fk(foreign_key=subset, references=(other_relation.tablename, key_set))
                            assigned_foreign_keys.add(subset)
                            print(f"Assigned FK (Subset Match): {subset} in {relation.tablename} -> {other_relation.tablename} referencing {key_set}")
                        break  # Move to next subset if subset match is handled

# Helper to ensure FK assignment rules
    def can_assign_fk(self, relation, other_relation, foreign_key_set):
     """
    Validates if foreign_key_set can be assigned as an FK in relation to other_relation.
    """
     return relation != other_relation and len(other_relation.attributes) >= len(relation.attributes) and self.relations.index(other_relation)>self.relations.index(relation)
    def fk_exists(self, relation, foreign_key_attribute):
     """
    Checks if a foreign key attribute already exists in a relation's foreign keys.
    """
     for fk in relation.foreign_keys:
        if foreign_key_attribute in fk['foreign_key']:
            return True
     return False
    