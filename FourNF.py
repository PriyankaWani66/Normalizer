from collections import defaultdict
from copy import deepcopy
from Relation import Relation
from MVDgenerator import MVDgenerator
class FourNF:
    @staticmethod
    def isin(relation: Relation):
        """
        Check if the given relation is in 4NF, including verifying that the MVDs are valid.
        """
        valid_mvds = defaultdict(list)

        for determinant, dependent_lists in relation.mvd_map.items():
            print("--------------------------------",dependent_lists)
            is_valid = True
            valid_mvds[determinant]=[]
            mvdgen=MVDgenerator(relation.df,relation.attributes)
            mvdgen.find_and_validate_all_mvds()
            for dependent_list in dependent_lists:
                if relation.validate_each_mvd(determinant,dependent_list):
                    valid_mvds[determinant].append(dependent_list)
                else:
                    print(f"Invalid MVD detected: {determinant} -->> {dependent_list}")
                    is_valid = False

            if not is_valid:
                print(f"Warning: Some MVDs for determinant {determinant} are invalid, but this does not directly affect 4NF compliance.")

        # Check if the relation is in 4NF using valid MVDs
        for determinant, dependent_lists in valid_mvds.items():
            print("-----------------------dependent_sets",dependent_lists)
            for dependent_list in dependent_lists:
                for dependent_set in dependent_list:

                    if not (dependent_set.issubset(determinant) or
                        determinant.union(dependent_set) == relation.attributes):
                        print(f"Relation is not in 4NF due to non-trivial MVD: {determinant} -->> {dependent_set}")
                        return False
        mvdgen=MVDgenerator(relation.df,relation.attributes)
        generated_mvds=mvdgen.find_and_validate_all_mvds()
        for [determinant_set,dependent_set1,dependent_set2] in generated_mvds:

            relation.add_mvd(determinant_set,[dependent_set1, dependent_set2])
        if generated_mvds:
            return False
        
        return True

    @staticmethod
    def normalise(relation: Relation):
        valid_mvds = defaultdict(list)
        new_relations = []
        collected_attrs = set()  # Track all attributes that are used in decomposition

        # Step 1: Validate MVDs using the is_valid_mvd() method from the Relation class
        for determinant, dependent_lists in relation.mvd_map.items():
            is_valid = True
            for dependent_list in dependent_lists:
                if relation.validate_each_mvd(determinant,dependent_list):
                    valid_mvds[determinant].append(deepcopy(dependent_list))
                else:
                    print(f"Invalid MVD detected: {determinant} -->> {dependent_list}")
                    is_valid = False

            if not is_valid:
                print(f"Warning: Some MVDs for determinant {determinant} are invalid, but this does not directly affect 4NF compliance.")

        # Step 2: Check if the relation is in 4NF using valid MVDs
        for determinant, dependent_lists in valid_mvds.items():
            for dependent_list in dependent_lists:
                
                        for dependent_set in dependent_list:
                            
                            if  (dependent_set.issubset(determinant) or
                        determinant.union(dependent_set) == relation.attributes):
                                continue
                            new_attr = determinant.union(deepcopy(dependent_set))  # Determinant + dependent
                            new_pk = deepcopy(determinant).union(dependent_set)  # Extend the primary key to include dependent
                            
                            # Debug: Print the attributes of the new relation
                            print(f"New Relation Attributes: {new_attr}")
                            print(f"Primary Key: {new_pk}")
                            
                            # Keep candidate keys that are subsets of new attributes
                            new_cks = [deepcopy(ck) for ck in relation.cks if ck <= new_attr]
                            
                            # Debug: Check if there are columns for new attributes
                            print(f"Creating DataFrame for new relation from columns: {list(new_attr)}")
                            print(f"Available columns in DataFrame: {relation.df.columns}")

                            # Create the new relation
                            new_relation = Relation(
                                tablename=f"{relation.tablename}_Decomposed_{determinant}_{dependent_set}",
                                attributes=deepcopy(new_attr),
                                pk=deepcopy(new_pk),
                                cks=deepcopy(new_cks),
                                MvalAttr=set(),  # No multivalued attributes in the new relation
                                df=relation.df[list(new_attr)].copy(),  # Project the DataFrame for the new relation
                                original=relation,  # Keep a reference to the original relation
                                base_relation=relation.base_relation  # Keep a reference to the base relation
                            )

                            # Clear out the MVD map and FD map for the new relation
                            for mvd_determinant, mvd_dependent_lists in deepcopy(relation.mvd_map).items():
                                for mvd_dependent_list in mvd_dependent_lists:
                                    dependent_union=set().union(*mvd_dependent_list)
                                    mvd_union = mvd_determinant.union(dependent_union)
                                    if mvd_union - new_relation.attributes == set():                       
                                        print(f"Moving MVD: {mvd_determinant} -->> {mvd_dependent_list}")
                                        new_relation.add_mvd(mvd_determinant, mvd_dependent_list)  # Transfer the whole set

                            new_relation.fd_map = {}

                            # Track the attributes involved in this new relation
                            collected_attrs.update(new_attr)

                            new_relations.append(new_relation)
                            print(f"Created new relation for MVD: {new_relation.tablename}")

        # Step 3: Handle leftover attributes (if any)
        leftover_attrs = relation.attributes - collected_attrs
        print("Leftover attributes: ", leftover_attrs)

        if leftover_attrs:
            new_relation_attr = leftover_attrs.union(relation.pk)  # Ensure the primary key is included in the leftover relation
            print(f"Leftover relation attributes: {new_relation_attr}")
            
            # Keep candidate keys that are subsets of the leftover attributes
            new_cks = [deepcopy(ck) for ck in relation.cks if ck <= new_relation_attr]

            # Debug: Check if there are columns for leftover attributes
            print(f"Creating DataFrame for leftover relation from columns: {list(new_relation_attr)}")
            print(f"Available columns in DataFrame: {relation.df.columns}")

            # Create a new DataFrame for the leftover relation
            new_df = relation.df[list(new_relation_attr)].copy()

            # Create the leftover relation
            leftover_relation = Relation(
                tablename=f"{relation.tablename}_leftover_4NF",
                attributes=deepcopy(new_relation_attr),
                pk=deepcopy(relation.pk),
                cks=deepcopy(new_cks),
                MvalAttr=set(),  # No multivalued attributes
                df=new_df.copy(),
                original=relation,  # Keep a reference to the original relation
                base_relation=relation.base_relation  # Keep a reference to the base relation
            )

            # Clear out the MVD map for the leftover relation
            leftover_relation.mvd_map = {}

            # Step 4: Handle FDs for leftover relation
            for fd_determinant, fd_dependents in relation.fd_map.items():
                # Union of determinant and dependents
                fd_union = fd_determinant.union(fd_dependents)
                
                # If this union is a subset of leftover attributes, add this FD to the leftover relation
                if fd_union <= new_relation_attr:
                    leftover_relation.add_fd(deepcopy(fd_determinant), deepcopy(fd_dependents))
                    print(f"Added FD: {fd_determinant} --> {fd_dependents} to leftover relation: {leftover_relation.tablename}")

            # Add the leftover relation to the list of new relations
            new_relations.append(leftover_relation)
            print(f"Created leftover relation: {leftover_relation.tablename}")

        return new_relations
