from Relation import Relation
from copy import deepcopy

class BCNF:
   
    @staticmethod
    def isin(relation: Relation) -> bool:
        """
        Checks if the relation (table) is in BCNF.
        A relation is in BCNF if for every functional dependency X -> Y:
        - X is a superkey of the relation.
        """
        print(f"Checking if the relation {relation.tablename} is in BCNF with updated FDs.")
        for determinant, dependent in relation.fd_map.items():
            if not BCNF.is_superkey(relation, determinant):
                print(f"FD {determinant} -> {dependent} violates BCNF.")
                return False
        print("Relation is in BCNF.")
        return True

    @staticmethod
    def is_superkey(relation: Relation, determinant_set: frozenset) -> bool:
        """
        Checks if the given determinant_set is a superkey (i.e., it contains the primary key or any candidate key).
        """
        if determinant_set.issuperset(relation.pk):
            return True
        if relation.cks:
            for ck in relation.cks:
                if determinant_set.issuperset(ck):
                    return True
        return False

    @staticmethod
    def normalise(relation: Relation):
        """
        Normalize the relation to BCNF by decomposing relations if any FD violates BCNF.
        """
        print(f"Starting BCNF normalization for relation: {relation.tablename}")
        normalized_relations = []
        decomposition_counter = 1
        
        # Handle primary key as a frozenset
        if isinstance(relation.pk, str):
            relation.pk = frozenset([relation.pk])
        elif isinstance(relation.pk, (set, list)):
            relation.pk = frozenset(relation.pk)
        
        # Copy the original relation for processing remaining attributes
        remaining_relation = Relation(
            tablename=relation.tablename,
            attributes=relation.attributes.copy(),
            pk=relation.pk,
            cks=relation.cks.copy() if relation.cks is not None else [],
            MvalAttr=relation.MvalAttr,
            df=relation.df.copy() if relation.df is not None else None
        )
        remaining_fd_map = relation.fd_map.copy()
        
        print(f"Initial attributes of relation {relation.tablename}: {relation.attributes}")
        
        base_name = relation.tablename

        for determinant, dependent in list(relation.fd_map.items()):
            if not BCNF.is_superkey(relation, determinant):
                print(f"Decomposing relation based on FD {determinant} -> {dependent} violating BCNF.")
                
                # Create a new relation based on the FD
                new_relation_name = f"{base_name}_part{decomposition_counter}" if decomposition_counter > 1 else base_name
                decomposition_counter += 1
                new_pk = frozenset(determinant) if isinstance(determinant, (set, frozenset)) else frozenset([determinant])
                new_relation_attributes = set(determinant).union(dependent)
                
                # Filter FDs for the new relation based on relevant attributes
                new_relation_fd_map = {
                    det: dep for det, dep in remaining_fd_map.items()
                    if det.issubset(new_relation_attributes) and dep.issubset(new_relation_attributes)
                }
                
                new_relation = Relation(
                    tablename=new_relation_name,
                    attributes=new_relation_attributes,
                    pk=new_pk,
                    cks=[],  # Candidate keys are processed separately
                    MvalAttr=None,
                    df=relation.df[list(new_relation_attributes)].copy() if relation.df is not None else None
                )
                
                print(f"New relation created: {new_relation.tablename} with attributes {new_relation.attributes} and primary key {new_relation.pk}")
                new_relation.fd_map = new_relation_fd_map

                for mvd_determinant, mvd_dependent_lists in deepcopy(relation.mvd_map).items():
                    for mvd_dependent_list in mvd_dependent_lists:
                        dependent_union = set().union(*mvd_dependent_list)
                        mvd_union = mvd_determinant.union(dependent_union)
                        if mvd_union <= new_relation_attributes:                       
                            print(f"Moving MVD: {mvd_determinant} -->> {mvd_dependent_list}")
                            new_relation.add_mvd(mvd_determinant, mvd_dependent_list)

                normalized_relations.append(new_relation)

                # Remove the FD from the remaining relation's FD map immediately
                del remaining_fd_map[determinant]
                
                # Remove dependent attributes from remaining relation's attributes
                remaining_relation.attributes = [attr for attr in remaining_relation.attributes if attr not in dependent]
                print(f"Remaining attributes after decomposition: {remaining_relation.attributes}")
                
                remaining_fd_map = {
                    det.intersection(remaining_relation.attributes): dep.intersection(remaining_relation.attributes)
                    for det, dep in remaining_fd_map.items()
                    if det.intersection(remaining_relation.attributes) and dep.intersection(remaining_relation.attributes)
                }
                
                # Update primary key of the remaining relation if attributes are removed
                remaining_relation.pk = BCNF.recompute_primary_key(remaining_relation, relation)

        remaining_relation.fd_map = remaining_fd_map
        if len(remaining_relation.attributes) > 1:
            print(f"Remaining relation has attributes: {remaining_relation.attributes}")
            normalized_relations.append(remaining_relation)
        else:
            print(f"Remaining relation contains only the primary key {remaining_relation.pk}, skipping its addition.")
        
        remaining_relation.cks = BCNF.identify_candidate_keys(remaining_relation)
        for mvd_determinant, mvd_dependent_lists in deepcopy(relation.mvd_map).items():
            for mvd_dependent_list in mvd_dependent_lists:
                dependent_union = set().union(*mvd_dependent_list)
                mvd_union = mvd_determinant.union(dependent_union)
                if mvd_union <= remaining_relation.attributes:
                    print(f"Moving MVD: {mvd_determinant} -->> {mvd_dependent_list}")
                    remaining_relation.add_mvd(mvd_determinant, mvd_dependent_list)

        for rel in normalized_relations:
            rel.cks = BCNF.identify_candidate_keys(rel)

        BCNF.update_keys(relation, normalized_relations, remaining_relation)

        print(f"BCNF normalization complete. Normalized relations: {[rel.tablename for rel in normalized_relations]}")
        return normalized_relations

    @staticmethod
    def recompute_primary_key(relation, original_relation):
        new_primary_key = frozenset(attr for attr in original_relation.pk if attr in relation.attributes)
        if len(new_primary_key) < 1:
            new_primary_key = frozenset(relation.attributes)
            print(f"No valid primary key remains, using all attributes as primary key: {new_primary_key}")
        else:
            print(f"Updated primary key after removal of attributes: {new_primary_key}")
        return new_primary_key

    @staticmethod
    def update_keys(original_relation, normalized_relations, remaining_relation):
        print("Updating primary and candidate keys for BCNF relations...")
        if not all(attr in remaining_relation.attributes for attr in original_relation.pk):
            remaining_relation.pk = BCNF.recompute_primary_key(remaining_relation, original_relation)
        if isinstance(remaining_relation.pk, (set, frozenset)) and len(remaining_relation.pk) == 1:
            remaining_relation.pk = frozenset([next(iter(remaining_relation.pk))])
        remaining_relation.cks = BCNF.identify_candidate_keys(remaining_relation)
        if not remaining_relation.cks:
            remaining_relation.cks = None
        for rel in normalized_relations:
            if rel != remaining_relation:
                rel.pk = frozenset([next(iter(rel.pk))]) if isinstance(rel.pk, (set, frozenset)) and len(rel.pk) == 1 else rel.pk
                rel.cks = BCNF.identify_candidate_keys(rel)
                if not rel.cks:
                    rel.cks = None

    @staticmethod
    def find_minimal_superkey(relation):
        if relation.fd_map:
            for determinant in relation.fd_map.keys():
                if determinant.issubset(relation.attributes):
                    return determinant
        return relation.attributes

    @staticmethod
    def identify_candidate_keys(relation: Relation):
        # Initialize candidate keys as an empty list if `cks` is None
        if relation.cks is None:
            relation.cks = []
        
        candidate_keys = [ck for ck in relation.cks if all(attr in relation.attributes for attr in ck)]
        return candidate_keys if candidate_keys else None

