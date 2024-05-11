"""
Algorithm
Start at the end of the list and look at each pair of neighboring numbers.
Compare each pair: If the first number is bigger than the second, swap them.
Keep doing this: Move through the list, comparing and swapping pairs, until you've gone through the whole list once.
Repeat: Go through the list again, but this time stop before the last element because the biggest number is already in its place.
Keep repeating: Each time you go through the list, the biggest unsorted number "bubbles up" to its correct position.
Keep going until the list is sorted: Keep repeating these steps until the whole list is sorted from smallest to largest.
Finished: Once no more swaps are needed, the list is sorted, and you're done!
"""
from typing import List


def bubble_sort(my_list: List):
    for i in range(len(my_list) - 1, 0, -1):
        for j in range(i):  # first time whole list, 2nd time n-1, n-2. just the first number
            if my_list[j] > my_list[j + 1]:
                temp = my_list[j]
                my_list[j] = my_list[j + 1]
                my_list[j + 1] = temp
    return my_list
