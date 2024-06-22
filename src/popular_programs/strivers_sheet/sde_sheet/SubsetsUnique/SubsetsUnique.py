from typing import List


class SubsetsUnique:
    @staticmethod
    def subsets_unique(input_list: List):
        result_list = []
        subset = []

        def recursion(index: int):

            # .copy() to avoid original reference of the list
            result_list.append(subset.copy())

            for i in range(index, len(input_list)):
                if i != index and input_list[i] == input_list[i - 1]:
                    continue
                subset.append(input_list[i])
                recursion(index=i + 1)
                subset.pop()

        input_list.sort()
        recursion(index=0)

        return result_list


def test_subsets_unique():
    input_list = [1, 2, 2]
    expected_list = [[], [1], [1, 2], [1, 2, 2], [2], [2, 2]]
    subsets_unique = SubsetsUnique()
    result = subsets_unique.subsets_unique(input_list=input_list)
    assert result == expected_list
