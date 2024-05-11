"""
Algorithm
Start with the first element of the list and look at each pair of neighboring numbers.
Compare each pair: If the first number is bigger than the second, swap them.
Keep doing this: Move through the list, comparing and swapping pairs, until you've gone through the whole list once.
Repeat: Go through the list again, but this time stop before the last element because the biggest number is already in its place.
Keep repeating: Each time you go through the list, the biggest unsorted number "bubbles up" to its correct position.
Keep going until the list is sorted: Keep repeating these steps until the whole list is sorted from smallest to largest.
Finished: Once no more swaps are needed, the list is sorted, and you're done!
"""
from typing import List


class BubbleSort:

    @staticmethod
    def ascending(input_list: List):
        for i in range(len(input_list) - 1, 0, -1):
            for j in range(i):
                if input_list[j] > input_list[j + 1]:
                    temp = input_list[j]
                    input_list[j] = input_list[j + 1]
                    input_list[j + 1] = temp
        return input_list

    @staticmethod
    def descending(input_list: List):
        for i in range(len(input_list) - 1, 0, -1):
            for j in range(i):
                if input_list[j] < input_list[j + 1]:
                    temp = input_list[j]
                    input_list[j] = input_list[j + 1]
                    input_list[j + 1] = temp
        return input_list
