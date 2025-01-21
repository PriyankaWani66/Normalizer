from Relation import Relation
from itertools import chain, combinations
import pandas as pd
from copy import deepcopy

class FiveNF:
    @staticmethod
    def get_all_subsets(attributes):
        """ Helper function to get all non-empty subsets of attributes, excluding single-element sets """
        subsets = list(chain.from_iterable(combinations(attributes, r) for r in range(2, len(attributes))))
        print(f"Generated subsets: {subsets}")
        return subsets

    @staticmethod
    def get_partitions(attributes):
        """ Generate all valid partitions of a set of attributes into N subsets """
        all_subsets = FiveNF.get_all_subsets(attributes)
        partitions = []
        
        for i in range(2, len(attributes) // 2 + 2):
            for partition in combinations(all_subsets, i):
                if any(len(subset) == 1 for subset in partition) or any(set(subset) == set(attributes) for subset in partition):
                    continue

                if set().union(*partition) == set(attributes):
                    valid = True
                    for idx, subset in enumerate(partition):
                        if not any(set(subset).intersection(set(partition[j])) for j in range(len(partition)) if j != idx):
                            valid = False
                            break

                    if valid:
                        print(f"Valid partition found: {partition}")
                        partitions.append(partition)
                    else:
                        print(f"Invalid partition due to no common attributes: {partition}")

        print(f"Generated partitions: {partitions}")
        return partitions

    @staticmethod
    def test_lossless_join_multiple(subsets, original_df):
        """ Perform a lossless join test for multiple partitions """
        print(f"Testing lossless join for partition: {subsets}")
        common_columns = set(subsets[0])
        result_df = original_df[list(common_columns)].drop_duplicates()
        
        for subset in subsets[1:]:
            subset_df = original_df[list(subset)].drop_duplicates()
            common_columns = common_columns.intersection(set(subset))
            print(f"Common columns for join: {common_columns}")
            if not common_columns:
                print(f"No common columns found between subsets {subsets[0]} and {subset}, invalid join.")
                return False
            result_df = pd.merge(result_df, subset_df, on=list(common_columns), how='inner')

        joined_columns = list(set().union(*subsets))
        valid_columns = [col for col in joined_columns if col in result_df.columns and col in original_df.columns]
        result_match = result_df[valid_columns].equals(original_df[valid_columns])
        print(f"Join result matches original: {result_match}")

        union_of_subsets = set().union(*subsets)
        if union_of_subsets != set(original_df.columns):
            print(f"Union of subsets {subsets} does not match the original attributes. Skipping.")
            return False

        if result_match:
            with open("Losslessresultsubsets.txt", "a") as file:
                file.write(f"Valid lossless join partition: {subsets}\n")
        return result_match

    @staticmethod
    def normalise(relation: Relation):
        """ Normalize the relation to 5NF if it's not already in 5NF """
        if FiveNF.isin(relation):
            print(f"Relation {relation.tablename} is already in 5NF. No normalization needed.")
            return [relation]

        attributes = relation.df.columns
        partitions = FiveNF.get_partitions(attributes)
        new_relations = []
        last_decomposition = []

        output_file = "OutputFileFor5NF.txt"
        with open(output_file, "w") as file:
            file.write("Normalization process for 5NF\n")
            file.write(f"Original attributes: {relation.attributes}\n\n")
            
            for partition in partitions:
                if FiveNF.test_lossless_join_multiple(partition, relation.df):
                    print(f"Join dependency detected for partition: {partition}")
                    union_of_partition = set().union(*partition)
                    if union_of_partition != set(attributes):
                        print(f"Partition {partition} does not cover all attributes. Skipping.")
                        file.write(f"Partition {partition} does not cover all attributes. Skipping.\n")
                        continue

                    last_decomposition = []

                    for subset in partition:
                        new_relation = Relation(
                            tablename=f"{relation.tablename}_Decomposed_{'_'.join(subset)}",
                            attributes=set(subset),
                            pk={col for col in subset if col in relation.pk},
                            cks=[ck for ck in relation.cks if ck <= set(subset)],
                            MvalAttr=set(),
                            df=relation.df[list(subset)].drop_duplicates().copy(),
                            original=relation,
                            base_relation=relation.base_relation
                        )
                        new_relation.mvd_map = {}
                        new_relation.fd_map = {}
                        
                        for fd_determinant, fd_dependents in deepcopy(relation.fd_map).items():
                            if fd_determinant <= set(subset):
                                fd_dependent_subset = fd_dependents & set(subset)
                                if fd_dependent_subset:
                                    new_relation.add_fd(fd_determinant, fd_dependent_subset)

                        # Ensure the primary key is also added as a candidate key if it’s minimal
                        if new_relation.pk and new_relation.pk not in new_relation.cks:
                            new_relation.cks.append(new_relation.pk)

                        last_decomposition.append(new_relation)
                        print(f"Created new relation: {new_relation.tablename} from {subset}")
                    file.write(f"Appended decomposition: {partition}\n")

            file.write("\nSchema details for the final lossless decomposition:\n")
            for final_relation in last_decomposition:
                file.write(f"\nTable: {final_relation.tablename}\n")
                file.write(f"- Attributes: {', '.join(final_relation.attributes)}\n")
                file.write(f"- Primary Key: {', '.join(final_relation.pk) if final_relation.pk else 'None'}\n")
                file.write(f"- Candidate Keys: {', '.join(map(str, final_relation.cks)) if final_relation.cks else 'None'}\n")
                file.write("- Multivalued Attributes: None\n")
                file.write(f"- Functional Dependencies: {', '.join(f'{det} -> {dep}' for det, dep in final_relation.fd_map.items()) if final_relation.fd_map else 'None'}\n")
                file.write("- Foreign Keys: None\n")

        return last_decomposition

    @staticmethod
    def isin(relation: Relation) -> bool:
        """ Check if the relation is in 5NF """
        attributes = relation.df.columns
        partitions = FiveNF.get_partitions(attributes)
        for partition in partitions:
            if FiveNF.test_lossless_join_multiple(partition, relation.df):
                print(f"Join dependency detected for partition: {partition}")
                return False
        print(f"Relation {relation.tablename} is in 5NF")
        return True
