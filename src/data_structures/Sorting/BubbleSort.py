"""
Algorithm
Start with the 1st element
Compare it with its adjacent numbers in sequence from left to right
If the right number is greater than the left number, swap them, to make sure the right element is larger
At the end of 1st iteration, the largest number of the list bubbles up to the right most index of the list
Now, repeat the above process, and just that the iteration will not include the last element of the list which is already in its correct position
In a manner of speaking, the iterations follow the below approach:
n(n-1)(n-2)..... = n! => Time complexity is O(n!)
"""
from typing import List


class BubbleSort:

    @staticmethod
    def ascending(input_list: List):
        for i in range(len(input_list) - 1, 0, -1):
            for j in range(0, i):
                if input_list[j + 1] < input_list[j]:
                    tmp = input_list[j + 1]
                    input_list[j + 1] = input_list[j]
                    input_list[j] = tmp
        return input_list

    @staticmethod
    def descending(input_list: List):
        for i in range(len(input_list) - 1, 0, -1):
            for j in range(0, i):
                if input_list[j] < input_list[j + 1]:
                    temp = input_list[j]
                    input_list[j] = input_list[j + 1]
                    input_list[j + 1] = temp
        return input_list
