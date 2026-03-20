from typing import List


def combination_sum_limited_sample(input_list: List, target: int):
    result = []
    subset = []

    def recursion(ind, summ):

        for i in range(ind, len(input_list)):

            if input_list[i] == input_list[i - 1] and i > ind:
                continue

            summ = summ + input_list[i]

            if summ > target:
                summ = summ - input_list[i]
                return

            if summ == target:
                subset.append(input_list[i])
                result.append(subset.copy())
                summ = summ - input_list[i]
                subset.pop()
                return

            if summ < target:
                subset.append(input_list[i])
                recursion(i + 1, summ)
                subset.pop()
                summ = summ - input_list[i]

    recursion(ind=0, summ=0)
    return result


if __name__ == "__main__":
    input_list = [1, 1, 1, 2, 2]
    target = 4
    expected_list = [[1, 1, 2], [2, 2]]
    result = combination_sum_limited_sample(input_list, target)
    assert result == expected_list
