from Relation import Relation
from copy import deepcopy
import pandas as pd

class TwoNF:
    @staticmethod
    def isin(table: Relation) -> bool:
        """ Check if the relation is in 2NF by identifying partial dependencies. """
        # Combine primary key with candidate keys if not already included
        isrenormalisation=False
        if('2NF' in table.tablename):
            isrenormalisation=True
        

        keys = [table.base_relation.pk] + list(table.base_relation.cks)
        if(isrenormalisation):
            keys = [table.pk] + list(table.cks)

        prime_attributes = set().union(*keys)

        # Check for partial dependencies
        for determinant, dependents in table.fd_map.items():
            for key in keys:
                if (dependents - prime_attributes) and (determinant <= prime_attributes) and (determinant not in keys):
                    return False
        return True

    @staticmethod
    def normalise(relation: Relation):
        """ Normalize the relation to 2NF by removing partial dependencies. """
        original_keys = [relation.base_relation.pk] + list(relation.base_relation.cks)
        prime_attributes = set().union(*original_keys)
        new_relations = []
        collected_attrs = set()  # Collect attributes used in partial dependencies

        # Iterate through each FD in the relation
        for determinant, dependents in relation.fd_map.items():
            # Identify partial dependencies based on the prime attributes
            if (dependents - prime_attributes) and (determinant <= prime_attributes) and (determinant not in original_keys):
                
                new_pk = determinant
                new_cks = [ck for ck in relation.cks if ck <= determinant.union(dependents)]
                new_attr = determinant.union(dependents)

                # Create a new DataFrame for the relation
                if relation.df is not None:
                    new_df = deepcopy(relation.df[list(new_attr)])
                else:
                    new_df = pd.DataFrame(columns=list(new_attr))

                # Create the new relation
                new_relation = Relation(
                    tablename=f"{relation.tablename}_2NF_{'_'.join(sorted(new_attr))}",
                    attributes=new_attr,
                    pk=new_pk,
                    cks=new_cks,
                    MvalAttr=set(),
                    df=new_df
                )
                new_relation.add_fd(determinant, dependents)
                # Recursive FD integration to preserve dependencies
                TwoNF.recursive_fd_mvd_integration(new_relation, relation)
                for fd_determinant, fd_dependents in deepcopy(relation.fd_map).items():
                # Ensure fd_dependents is processed as a set of attributes
                    original_keys = [relation.base_relation.pk] + list(relation.base_relation.cks)
                    prime_attributes = set().union(*original_keys)
                    if (fd_dependents - prime_attributes) and (fd_determinant <= prime_attributes) and (fd_determinant not in original_keys):
                        continue
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
                        if fd_union - new_relation.attributes == set():
                            print(f"Moving FD: {fd_determinant} -> {fd_dependent}")
                            new_relation.add_fd(fd_determinant, {fd_dependent})
                for fd_determinant, fd_dependents in deepcopy(relation.fd_map).items():
                    original_keys = [relation.base_relation.pk] + list(relation.base_relation.cks)
                    prime_attributes = set().union(*original_keys)
                    if (fd_dependents - prime_attributes) and (fd_determinant <= prime_attributes) and (fd_determinant not in original_keys):
                        if(fd_determinant==determinant and fd_dependents==dependents):
                            continue
                        print('fd_determinant',fd_determinant)
                        print("fd_dependents->->->->",fd_dependents)
                        print("new_relatoion_attr",new_relation.attributes)
                        print(fd_dependents<=dependents)
                        print(fd_determinant<determinant)
                        if(fd_dependents<=dependents and fd_determinant<determinant):
                            new_relation.attributes=new_relation.attributes-fd_dependents
                            print(new_relation.attributes)
                            for new_fd_det,new_fd_deps in new_relation.fd_map.items():
                                if fd_dependents <=new_fd_deps:
                                    new_fd_deps=new_fd_deps-fd_dependents
                                    new_relation.fd_map[new_fd_det]=new_fd_deps


                new_relations.append(new_relation)
                collected_attrs.update(new_relation.attributes)  # Track attributes involved in violations

                print(f"Created new 2NF relation: {new_relation.tablename}")

        # Create a new relation for the leftover attributes
        leftover_attrs = relation.attributes - collected_attrs
        print("leftover_attr------------------------", leftover_attrs)
        if True:
            if not leftover_attrs:
                leftover_attrs=set()
            new_relation_attr = leftover_attrs.union(relation.base_relation.pk).union(*relation.cks)
            print("-----NEW_relationAttr",new_relation_attr)  # Add PK if not already present
            new_cks = [ck for ck in relation.base_relation.cks if ck <= new_relation_attr]

            # Create a new DataFrame for the leftover relation
            print(new_relation_attr)
            if relation.base_relation.df is not None and new_relation_attr:
                new_df = deepcopy(relation.base_relation.df[list(new_relation_attr)])
            else:
                new_df = pd.DataFrame(columns=list(new_relation_attr))

            # Create the leftover relation
            leftover_relation = Relation(
                tablename=f"{relation.tablename}_leftover_2NF",
                attributes=new_relation_attr,
                pk=relation.base_relation.pk,
                cks=new_cks,
                MvalAttr=set(),
                df=new_df
            )
            for fd_determinant, fd_dependents in relation.fd_map.items():
    # Ensure fd_determinant is a set
                fd_determinant = set(fd_determinant) if isinstance(fd_determinant, (set, frozenset)) else {fd_determinant}
    # Ensure fd_dependents is a set of attributes
                fd_dependents = set(fd_dependents) if isinstance(fd_dependents, (set, frozenset)) else {fd_dependents}
    
                for fd_dependent in fd_dependents:
        # Convert fd_dependent to a set if it's a single attribute (string)
                    fd_dependent_set = {fd_dependent} if isinstance(fd_dependent, str) else fd_dependent
        # Check if the union of fd_determinant and fd_dependent_set is within leftover_attrs
                    if fd_determinant.union(fd_dependent_set) <= new_relation_attr:
                        leftover_relation.add_fd(fd_determinant, fd_dependent_set)
            
            for mvd_determinant,mvd_dependent_lists in relation.mvd_map.items():
                for mvd_dependent_list in mvd_dependent_lists:
                    mvd_dependent_union=set().union(*mvd_dependent_list)
                    if mvd_dependent_union.union(mvd_determinant)<=new_relation_attr:
                        leftover_relation.add_mvd(mvd_determinant,mvd_dependent_list)



            new_relations.append(leftover_relation)
            print(f"Created leftover relation: {leftover_relation.tablename}")

        return new_relations

    @staticmethod
    def recursive_fd_mvd_integration(new_relation: Relation, relation: Relation):
     """ Recursively integrate functional dependencies (FDs) and multivalued dependencies (MVDs) into the new relation, ensuring FD and MVD preservation. """
     added = True
     while added:
        added = False
        current_attrs = new_relation.attributes
        current_pk = new_relation.pk

        # Handle Functional Dependencies (FDs)
        for fd_determinant, fd_dependents in relation.fd_map.items():
            original_keys = [relation.base_relation.pk] + list(relation.base_relation.cks)
            prime_attributes = set().union(*original_keys)
            if (fd_dependents - prime_attributes) and (fd_determinant <= prime_attributes) and (fd_determinant not in original_keys):
                continue
            fd_union = fd_determinant.union(fd_dependents)
            
            # Check if FD determinant is a subset of attributes minus primary key
            if fd_determinant <= current_attrs and not fd_determinant.issubset(current_pk):
                missing_attrs = fd_union - current_attrs
                if missing_attrs:
                    # Create a new frozenset with the added attributes
                    new_relation.attributes = current_attrs.union(missing_attrs)
                    new_relation.df = relation.df[list(new_relation.attributes)].copy()
                    new_relation.add_fd(fd_determinant, fd_dependents)
                    new_relation.cks = [ck for ck in relation.base_relation.cks if ck <= new_relation.attributes]

                    added = True

        # Handle Multivalued Dependencies (MVDs)
        for mvd_determinant, mvd_dependents_lists in relation.mvd_map.items():
            for mvd_dependent_list in mvd_dependents_lists:
                mvd_union = mvd_determinant.union(*mvd_dependent_list)
                
                # Check if MVD determinant is a subset of attributes minus primary key
                if mvd_determinant <= current_attrs and not mvd_determinant.issubset(current_pk):
                    missing_attrs = mvd_union - current_attrs
                    if missing_attrs:
                        # Create a new frozenset with the added attributes
                        new_relation.attributes = current_attrs.union(missing_attrs)
                        new_relation.df = relation.df[list(new_relation.attributes)].copy()
                        new_relation.add_mvd(mvd_determinant, mvd_dependent_list)
                        new_relation.cks = [ck for ck in relation.base_relation.cks if ck <= new_relation.attributes]

                        added = True

     print(f"Completed FD/MVD integration for {new_relation.tablename} with attributes {new_relation.attributes}")
