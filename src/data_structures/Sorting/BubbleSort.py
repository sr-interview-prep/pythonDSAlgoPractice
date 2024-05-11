"""
Algorithm
Start with the 1st element
Check if the next element is less than the left element, if yes swap
At the end of this iteration, the largest number bubbles up to the right most index of the list
Now, repeat the process and go only until the penultimate element as the last element is already in its correct position
So the time complexity of operations is n(n-1)(n-2).. = n!
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
                if input_list[j + 1] > input_list[j]:
                    tmp = input_list[j + 1]
                    input_list[j + 1] = input_list[j]
                    input_list[j] = tmp
        return input_list
