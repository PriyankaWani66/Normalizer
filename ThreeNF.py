from Relation import Relation
from copy import deepcopy

class ThreeNF:

    @staticmethod
    def isin(relation: Relation) -> bool:
        """
        Checks if the relation (table) is in 3NF.
        A relation is in 3NF if for every functional dependency X -> Y, either:
        - X is a superkey, or
        - Y is a prime attribute (i.e., part of some candidate key).
        """
        print(f"Checking if the relation {relation.tablename} is in 3NF.")
        # Iterate through functional dependencies (FDs) in the relation
        for determinant, dependent in relation.fd_map.items():
            # Check if the determinant (X) is a superkey
            if not ThreeNF.is_superkey(relation, determinant):
                # If not a superkey, check if dependent (Y) is a prime attribute
                if not ThreeNF.is_prime_attribute(relation, dependent):
                    print(f"FD {determinant} -> {dependent} violates 3NF.")
                    return False
        # If all FDs satisfy the conditions of 3NF.
        print("Relation is in 3NF.")
        return True

    @staticmethod
    def is_superkey(relation: Relation, determinant_set: frozenset) -> bool:
        """
        Checks if the given determinant_set is a superkey (i.e., it contains the primary key or any candidate key).
        """
        # Check if the determinant is the primary key or a superkey
        if determinant_set.issuperset(relation.pk):
            return True
        
        # Ensure cks is not None before iterating
        if relation.cks:
            for ck in relation.cks:
                if determinant_set.issuperset(ck):
                    return True
        return False

    @staticmethod
    def is_prime_attribute(relation: Relation, dependent_set: frozenset) -> bool:
        """
        Checks if every attribute in the dependent set is a prime attribute (i.e., part of any candidate key).
        """
        # If candidate keys exist, check if the dependent attributes are part of the candidate keys
        if relation.cks:
            for attr in dependent_set:
                if any(attr in ck for ck in relation.cks):
                    return True
        # Also check if the primary key includes the attribute (if it's part of the primary key)
        if all(attr in relation.pk for attr in dependent_set):
            return True
        return False

    @staticmethod
    def normalise(relation: Relation):
        """
        Normalize the relation to 3NF by identifying violations and decomposing relations if necessary.
        """
        print(f"Starting 3NF normalization for relation: {relation.tablename}")
        normalized_relations = []
        if( isinstance(relation.pk, set)):
            new_pk=relation.pk
        elif isinstance(relation.pk, list):
            set(relation.pk)
        elif(isinstance(relation.pk, str)):
            new_pk=set()
            new_pk=new_pk.add(relation.pk)

        # Create a copy of the original relation to work with
        remaining_relation = Relation(
            tablename=relation.tablename,
            attributes=relation.attributes.copy(),
           
            
            pk=new_pk,
            cks=relation.cks.copy() if relation.cks is not None else [],  # Handle NoneType
            MvalAttr=relation.MvalAttr,
            df=relation.df.copy() if relation.df is not None else None
        )
        remaining_fd_map = relation.fd_map.copy()

        print(f"Initial attributes of relation {relation.tablename}: {relation.attributes}")
        print(f"Initial FD map: {relation.fd_map}")

        #relation_counter = 1
        base_name = relation.tablename+"3NF"  # Capture the base name of the relation
        for determinant, dependent in relation.fd_map.items():
            print(f"Analyzing FD: {determinant} -> {dependent}")

            # Check if the determinant is not already part of the primary key or candidate keys
            if not ThreeNF.is_superkey(relation, determinant):
                print(f"Decomposing relation based on FD {determinant} -> {dependent}.")
                
                # Create a new relation based on this FD
                new_relation_attributes = set(determinant).union(dependent)
                new_relation_fd_map = {determinant: dependent}  # Assign this FD to the new relation

                # Handle primary key assignment properly
                new_relation_pk = set()

# Add `determinant` to the set, ensuring it's added as a single item
                new_relation_pk.add(determinant if isinstance(determinant, (set, frozenset)) else frozenset([determinant]))
                
                new_relation = Relation(
                    tablename=f"{base_name}",
                    attributes=new_relation_attributes,
                    pk=new_relation_pk,  # Ensure pk is a set or a single attribute
                    cks=[],  # Candidate keys will be handled separately
                    MvalAttr=None,
                    df=relation.df[list(new_relation_attributes)].copy() if relation.df is not None else None
                )
                print(f"New relation created: {new_relation.tablename} with attributes {new_relation.attributes} and primary key {new_relation.pk}")
                
                # Assign the FD to the new relation
                new_relation.fd_map = {determinant: dependent}
                for mvd_determinant, mvd_dependent_lists in deepcopy(relation.mvd_map).items():
                 for mvd_dependent_list in mvd_dependent_lists:
                    dependent_union=set().union(*mvd_dependent_list)
                    mvd_union = mvd_determinant.union(dependent_union)
                    if mvd_union - new_relation_attributes == set():                       
                        print(f"Moving MVD: {mvd_determinant} -->> {mvd_dependent_list}")
                        new_relation.add_mvd(mvd_determinant, mvd_dependent_list)  # Transfer the whole set

                # Add the new relation to the list of normalized relations
                normalized_relations.append(new_relation)

                # Remove the dependent attributes from the remaining relation
                remaining_relation.attributes = [attr for attr in remaining_relation.attributes if attr not in dependent]
                print(f"Remaining attributes after decomposition: {remaining_relation.attributes}")

                # Remove the FD from the remaining FD map as it's assigned to the new relation
                del remaining_fd_map[determinant]
                    
                #relation_counter += 1

        # Update FD to remove only removed attributes, discard if no dependents remain
        remaining_relation.fd_map = {
            det: dep.intersection(remaining_relation.attributes) 
            for det, dep in remaining_fd_map.items() 
            if dep.intersection(remaining_relation.attributes)
        }

        # Add the remaining decomposed relation if it still holds attributes beyond just the primary key
        if len(remaining_relation.attributes) > 1:
            print(f"Remaining relation has attributes: {remaining_relation.attributes}")
            normalized_relations.append(remaining_relation)
        else:
            print(f"Remaining relation contains only the primary key {remaining_relation.pk}, skipping its addition.")

        # Update candidate keys for the remaining relation
        remaining_relation.cks = ThreeNF.identify_candidate_keys(remaining_relation)

        # Update candidate keys for new decomposed relations
        for rel in normalized_relations:
            rel.cks = ThreeNF.identify_candidate_keys(rel)
        for mvd_determinant, mvd_dependent_lists in deepcopy(relation.mvd_map).items():
                for mvd_dependent_list in mvd_dependent_lists:
                    dependent_union=set().union(*mvd_dependent_list)
                    mvd_union = mvd_determinant.union(dependent_union)
                    if mvd_union - remaining_relation.attributes == set():                       
                        print(f"Moving MVD: {mvd_determinant} -->> {mvd_dependent_list}")
                        remaining_relation.add_mvd(mvd_determinant, mvd_dependent_list)  # Transfer the whole set

           
        # Handle primary key and candidate key updates for remaining and new relations
        ThreeNF.update_keys(relation, normalized_relations, remaining_relation)

        # Return the list of normalized relations
        print(f"Normalization complete. Normalized relations: {[rel.tablename for rel in normalized_relations]}")
        return normalized_relations

    @staticmethod
    def update_keys(original_relation, normalized_relations, remaining_relation):
        """
        Updates primary keys and candidate keys for the new and remaining relations after decomposition.
        """
        print(f"Updating primary and candidate keys for relations...")

        # For the remaining relation, check if the primary key is still valid
        if not all(attr in remaining_relation.attributes for attr in original_relation.pk):
            print(f"Primary key {original_relation.pk} is split across relations.")
            # The primary key has been split across relations, handle this appropriately
            remaining_relation.pk = ThreeNF.recompute_primary_key(remaining_relation, original_relation)

        # Ensure that if the primary key is a single attribute, it is not represented as a set
        if isinstance(remaining_relation.pk, (set, frozenset)) and len(remaining_relation.pk) == 1:
            if isinstance(list(remaining_relation.pk)[0],str):
                remaining_relation_pk=set()
                remaining_relation_pk.add(list(remaining_relation.pk)[0])
            remaining_relation.pk = remaining_relation_pk  # Extract the single attribute

        # Now, identify valid candidate keys for the remaining relation
        remaining_relation.cks = ThreeNF.identify_candidate_keys(remaining_relation)

        # If no valid candidate keys are found, set candidate keys to None
        if not remaining_relation.cks or remaining_relation.cks == ['']:
            remaining_relation.cks = None

        # For each new relation, ensure the determinant is the primary key and check candidate keys
        for rel in normalized_relations:
            if rel != remaining_relation:
                print(f"Setting primary key for {rel.tablename}")
                rel.pk = list(rel.pk)[0] if isinstance(rel.pk, (set, frozenset)) and len(rel.pk) == 1 else rel.pk  # Handle single attribute pk
                rel.cks = ThreeNF.identify_candidate_keys(rel)
                # Ensure the candidate keys are set to None if no valid candidate keys are found
                if not rel.cks or rel.cks == ['']:
                    rel.cks = None
                print(f"Primary key for {rel.tablename}: {rel.pk}, Candidate keys: {rel.cks}")

    @staticmethod
    def recompute_primary_key(relation, original_relation):
        """
        Recomputes the primary key for the remaining relation if the original primary key is split.
        """
        print(f"Recomputing primary key for {relation.tablename}")
        
        # Check if original_relation.cks is None before iterating
        if original_relation.cks is None:
            print(f"No candidate keys found in the original relation. Falling back to original primary key.")
            return relation.pk  # Return the original primary key if no candidate keys are available

        for ck in original_relation.cks:
            if all(attr in relation.attributes for attr in ck):
                print(f"Candidate key {ck} is intact and becomes the new primary key.")
                return ck  # If a candidate key is intact, it becomes the new primary key

        print(f"Using fallback primary key: {relation.pk}")
        return relation.pk  # Fallback to the original primary key if no better option exists

    @staticmethod
    def identify_candidate_keys(relation: Relation):
        """
        Identifies candidate keys for the new relations after decomposition.
        """
        print(f"Identifying candidate keys for {relation.tablename}")
        # Ensure relation.cks is initialized as a list if it’s None
        if relation.cks is None:
            relation.cks = []
        candidate_keys = []

        # Check which candidate keys are still valid in the new relation
        for ck in relation.cks:
            if all(attr in relation.attributes for attr in ck):
                candidate_keys.append(ck)

        # If no candidate keys are found, leave it empty
        if not candidate_keys:
            return None

        print(f"Candidate keys for {relation.tablename}: {candidate_keys}")
        return candidate_keys
