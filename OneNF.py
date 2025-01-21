import pandas as pd
from copy import deepcopy  # Use deepcopy to ensure we are copying data by value, not reference
from Relation import Relation

class OneNF:
    @staticmethod
    def isin(table: Relation) -> bool:
        """
        Check if the relation is in 1NF.
        A relation is not in 1NF if it has multivalued attributes.
        """
        if table.MvalAttr and table.MvalAttr != ['']:
            print(f"Table is not in 1NF because it has multivalued attributes: {table.MvalAttr}")
            return False
        return True

    @staticmethod
    def normalise(relation: Relation):
        """
        Normalize the relation to 1NF by handling multivalued attributes.
        For each multivalued attribute, create a new relation with normalized data.
        Also, create a relation with the leftover attributes (PK + non-multivalued attributes).
        Only include the FDs and MVDs that haven't been moved to the new relations.
        """
        print("Normalizing to 1NF...")
        normalized_relations = []
        print("fds original map", relation.fd_map.items())

        # Handle each multivalued attribute separately
        for mv_attr in relation.MvalAttr:
            new_relation_name = f"NormalisedOneNF_{mv_attr}"
            new_pk = relation.pk.union({mv_attr})
            new_attributes = new_pk
            new_cks = []
            new_mval_attr = set()

            normalized_data = []

            for index, row in relation.df.iterrows():
                mv_values = row[mv_attr].strip('{}').split(',') if isinstance(row[mv_attr], str) and '{' in row[mv_attr] else [row[mv_attr]]

                for value in mv_values:
                    value = value.strip()
                    new_row = deepcopy(list(row[list(relation.pk)])) + [value]
                    normalized_data.append(new_row)

            new_columns = list(relation.pk) + [mv_attr]
            new_df = pd.DataFrame(normalized_data, columns=new_columns)

            new_relation = Relation(
                tablename=new_relation_name,
                attributes=new_attributes,
                pk=new_pk,
                cks=new_cks,
                MvalAttr=new_mval_attr,
                df=new_df.copy(),
                original=relation,
                base_relation=relation.base_relation
            )

            print(f"Created new relation: {new_relation_name}")

            # Handle FDs: Transfer FDs that have the multivalued attribute as the dependent
            for fd_determinant, fd_dependents in deepcopy(relation.fd_map).items():
                # Ensure fd_dependents is processed as a set of attributes
                if isinstance(fd_dependents, str):
                    fd_dependents = {fd_dependents}  # Convert to set if it's a single string
                elif isinstance(fd_dependents, set) and len(fd_dependents) == 1 and isinstance(next(iter(fd_dependents)), str):
                    # Split the only element if it represents multiple attributes
                    fd_dependents = {attr.strip() for attr in next(iter(fd_dependents)).split(',')}
                print("FD dependents:", fd_dependents)
                
                print("FD dependents:", len(fd_dependents))
                
                for fd_dependent in fd_dependents:
                    fd_union = fd_determinant.union({fd_dependent})
                    print("FD union:", fd_union)
                    if fd_union - new_attributes == set():
                        print(f"Moving FD: {fd_determinant} -> {mv_attr}")
                        new_relation.add_fd(fd_determinant, {mv_attr})

            # Handle MVDs: Transfer the entire dependent set if it contains the multivalued attribute
            for mvd_determinant, mvd_dependent_lists in deepcopy(relation.mvd_map).items():
                for mvd_dependent_list in mvd_dependent_lists:
                    dependent_union=set().union(*mvd_dependent_list)
                    mvd_union = mvd_determinant.union(dependent_union)
                    if mvd_union - new_attributes == set():                       
                        print(f"Moving MVD: {mvd_determinant} -->> {mvd_dependent_list}")
                        new_relation.add_mvd(mvd_determinant, mvd_dependent_list)  # Transfer the whole set

            normalized_relations.append(new_relation)

        # Handle leftover attributes (Primary Key + Non-Multivalued Attributes)
        leftover_attributes = relation.pk.union(relation.attributes - set(relation.MvalAttr))  # Fix: Convert MvalAttr to set
        leftover_data = relation.df[list(leftover_attributes)].copy()

        # Create a new relation for the leftover attributes
        leftover_relation = Relation(
            tablename=f"{relation.tablename}_LeftoverAttributes",
            attributes=leftover_attributes,
            pk=relation.pk,
            cks=relation.cks,  # Directly copying candidate keys from original relation
            MvalAttr=set(),
            df=leftover_data
        )

        # Add remaining FDs that were not moved to other normalized relations
        for fd_determinant, fd_dependent in deepcopy(relation.fd_map).items():
            # Remove any multivalued attributes from the dependent set
            fd_dependent_without_mv = fd_dependent - set(relation.MvalAttr)
            if fd_dependent_without_mv:
                print(f"Adding FD to leftover relation: {fd_determinant} -> {fd_dependent_without_mv}")
                leftover_relation.add_fd(fd_determinant, fd_dependent_without_mv)
            else:
                print(f"Skipping FD {fd_determinant} -> {fd_dependent} because dependent set became empty after removing multivalued attributes")

        # Add remaining MVDs that were not moved to other normalized relations
        for mvd_determinant, mvd_dependent_lists in deepcopy(relation.mvd_map).items():
                for mvd_dependent_list in mvd_dependent_lists:
                    dependent_union=set().union(*mvd_dependent_list)
                    mvd_union = mvd_determinant.union(dependent_union)
                    if mvd_union - leftover_attributes == set():                       
                        print(f"Moving MVD: {mvd_determinant} -->> {mvd_dependent_list}")
                        leftover_relation.add_mvd(mvd_determinant, mvd_dependent_list)  # Transfer the whole set

           

        print(f"Created relation for leftover attributes: {leftover_relation.tablename}")
        normalized_relations.append(leftover_relation)

        return normalized_relations
