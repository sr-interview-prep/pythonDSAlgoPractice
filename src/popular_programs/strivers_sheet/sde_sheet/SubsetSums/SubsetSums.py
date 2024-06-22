from typing import List


class SubsetSums:
    def __init__(self):
        self.result = []

    def subset_sums(self, input_list: List, index: int, summ: int):
        if index == len(input_list):
            self.result.append(summ)
            return
        # picking
        self.subset_sums(input_list=input_list, index=index + 1, summ=summ + input_list[index])
        # not picking
        self.subset_sums(input_list=input_list, index=index + 1, summ=summ)

    def helper_subset_sums(self, input_list: List) -> List:
        self.subset_sums(input_list=input_list, index=0, summ=0)
        return self.result
