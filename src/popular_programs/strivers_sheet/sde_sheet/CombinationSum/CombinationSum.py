def combination_sum(input_list, target):
    result = []
    subset = []
    summ = 0
    
    # Go with the assumption of having this sorted to skip a lot of unwarranted cases
    input_list.sort()

    def recursion(index, summ):

        ## For invalid cases
        if index == len(input_list):
            return

        summ = summ + input_list[index]

        if summ > target:
            return

        if summ < target:
            subset.append(input_list[index])
            recursion(index=index, summ=summ)
            subset.pop()
            summ = summ - input_list[index]

        if summ == target:
            subset.append(input_list[index])
            result.append(subset.copy())
            subset.pop()
            summ = summ - input_list[index]
            return

        recursion(index=index + 1, summ=summ)

    recursion(index=0, summ=0)

    return result


if __name__ == "__main__":
    input_list = [2, 3, 6, 7]
    target = 7
    expected_list = [[2, 2, 3], [7]]
    result = combination_sum(input_list, target)
    assert result == expected_list
