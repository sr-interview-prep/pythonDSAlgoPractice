from typing import List


class SubsetsUnique:
    @staticmethod
    def subsets_unique(input_list: List):
        result = []
        subset = []

        def dfs(i):
            if i >= len(input_list):
                result.append(subset.copy())
                return
            subset.append(input_list[i])
            dfs(i + 1)
            subset.pop()
            dfs(i + 1)

        dfs(0)

        return result


if __name__ == "__main__":
    input_list = [1, 2, 3]
    expected_list = [[], [1], [1, 2], [1, 2, 3], [1, 3], [2], [2, 3], [3]]
    subsets_unique = SubsetsUnique()
    result = subsets_unique.subsets_unique(input_list=input_list)
    assert sorted(result) == sorted(expected_list)
