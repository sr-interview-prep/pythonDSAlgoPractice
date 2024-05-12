"""
Algoritm
1) Take first element as the pivot
2) Check from 2nd element if that is greater than the pivot element, if yes inc the swap_index and then swap i and and swap_index value
3) At the end of the loop swap the swap_index and pivot_index value. This will ensure that all small elements are left of pivot and big elements are right of pivot
4) The call quick sort on the left elements, and right elements in a recursion with base case being left index should be greater than right index


Time Complexity:
Best Case:
O(nlogn)
Scenario: Occurs when the pivot selection and input data lead to perfectly balanced partitions at each step of the recursion.
Explanation: In the best-case scenario, Quicksort efficiently divides the input array into two equal-sized partitions at each step, resulting in a balanced recursion tree. This balanced partitioning ensures that the algorithm performs a logarithmic number of levels of recursion, with each level requiring linear time to perform partitioning operations.

Average Case:
O(nlogn)
Scenario: Most common scenario encountered in practice when sorting random or uniformly distributed input data.
Explanation: In the average-case scenario, Quicksort divides the input array into roughly equal-sized partitions at each step of the recursion. With each recursive call, the size of the partitions is halved, resulting in a logarithmic number of levels in the recursion tree. On each level, the algorithm performs linear operations (partitioning), resulting in an overall time complexity of


Worst Case:
O(n^2)
Scenario: Occurs when the pivot selection and input data lead to highly unbalanced partitions at each step of the recursion.
Explanation: In the worst-case scenario, Quicksort can degrade to
time complexity. This occurs when the pivot selection strategy consistently chooses a pivot that partitions the input array into highly unbalanced partitions, such as when the smallest or largest element is consistently chosen as the pivot. In such cases, the partitions may contain only one element, resulting in quadratic time complexity.
"""
from typing import List

'''
[4, 6, 1, 7, 3, 2, 5]
[4, 1, 6, 7, 3, 2, 5]
[4, 1, 3, 7, 6, 2, 5]
[4, 1, 3, 2, 6, 7, 5]
[2, 1, 3, 4, 6, 7, 5] -- End of 1st pivot
'''


def swap(my_list, index1, index2):
    temp = my_list[index1]
    my_list[index1] = my_list[index2]
    my_list[index2] = temp
    return my_list


def find_pivot_and_partition(my_list, left_index, end_index):
    pivot_value = my_list[left_index]
    possible_pivot_index = left_index

    for i in range(left_index + 1, end_index + 1):
        current_element = my_list[i]

        if current_element < pivot_value:
            possible_pivot_index += 1
            # incrementing the possible pivot index as we found another element lesser than pivot value

            my_list = swap(my_list, possible_pivot_index, i)
            # making sure the value lesser than pivot value stays left to it, so doing the swap

    # Now we have the proper pivot_index for the pivot value, therefore, doing the swap in order to:
    #   place the pivot value in its pivot index
    my_list = swap(my_list, possible_pivot_index, left_index)
    return possible_pivot_index


def quick_sort(my_list, left, right):
    # Recursive function to perform Quick Sort
    if left < right:
        # the below statements will make sure we use median element as the pivot index for best case scenario
        # pivot_index = (left + right) // 2
        # swap(my_list, pivot_index, left)

        pivot_index = find_pivot_and_partition(my_list, left, right)  # Find the pivot index and do the swapping

        quick_sort(my_list, left, pivot_index - 1)  # Sort the left sub-array
        quick_sort(my_list, pivot_index + 1, right)  # Sort the right sub-array
    return my_list  # Return the sorted list


def quick_sort_helper(my_list: List):
    # Helper function to call quick_sort with initial parameters
    return quick_sort(my_list, 0, len(my_list) - 1)  # Call quick_sort with left and right indices
