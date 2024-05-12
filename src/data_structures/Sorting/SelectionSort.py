"""
Algorithm
1) Go through the entire list to find the index of element with least value
2) Replace first element with that min value index element
3) Repeat the process with the 2nd element in the list

Best Case: O(n^2) - Selection sort always requires scanning through the entire unsorted portion of the list, even if it's already sorted.
Average Case: O(n^2) - Like bubble sort, selection sort also has nested loops, resulting in quadratic time complexity.
Worst Case: O(n^2) - This occurs when the list is sorted in reverse order or nearly sorted, as it requires the maximum number of comparisons and swaps.


This however, does lesser no. of swaps as compared to bubble sort so this is faster than bubble sort for:
- Longer lists
- And Not for the best case scenarios as for that bubble sort has o(n) time complexity
"""


def swap(my_list, index_1, index_2):
    tmp = my_list[index_1]
    my_list[index_1] = my_list[index_2]
    my_list[index_2] = tmp
    return my_list


def selection_sort(my_list):
    for i in range(0, len(my_list)):
        min_index = i
        for j in range(i + 1, len(my_list)):
            if my_list[j] < my_list[min_index]:
                min_index = j
        if i != min_index:  # the swap should take place only if the min index has changed
            my_list = swap(my_list, i, min_index)
    return my_list
