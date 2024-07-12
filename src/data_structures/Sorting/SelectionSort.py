"""
Algorithm
1) Go through the entire list to find the index of element with least value
2) Replace first element with that min value index element
3) Repeat the process with the 2nd element in the list
"""


def selection_sort(my_list):
    for i in range(len(my_list) - 1):
        min_index = i
        for j in range(i + 1, len(my_list)):
            if my_list[j] < my_list[min_index]:
                min_index = j
        if (
            i != min_index
        ):  # the swap should take place only if the min index has changed
            temp = my_list[i]
            my_list[i] = my_list[min_index]
            my_list[min_index] = temp
    return my_list


print(selection_sort([2, 4, 5, 1, 3, 6]))
