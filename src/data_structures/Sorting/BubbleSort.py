"""
Algorithm
Start with the 1st element
Check if the next element is less than the left element, if yes swap
At the end of this iteration, the largest number bubbles up to the right most index of the list
Now, repeat the process and go only until the penultimate element as the last element is already in its correct position
So the time complexity of operations is n(n-1)(n-2).. = n!


Best Case: O(n) - This occurs when the list is already sorted, and the algorithm needs only one pass to confirm this.
-- I cannot understand this one
Average Case: O(n^2) - In the average case, bubble sort requires nested loops, resulting in quadratic time complexity.
Worst Case: O(n^2) - This occurs when the list is sorted in reverse order, and bubble sort needs to make the maximum number of swaps.

"""
from typing import List


class BubbleSort:

    @staticmethod
    def sort(input_list: List):
        n = len(input_list)
        counter = 0
        for i in range(n - 1, 0, -1):
            swapped = False  # Flag to track if any swaps occurred
            for j in range(i):
                if input_list[j + 1] < input_list[j]:
                    tmp = input_list[j + 1]
                    input_list[j + 1] = input_list[j]
                    input_list[j] = tmp
                    swapped = True  # Set flag to True if a swap occurs
                counter += 1
                print(input_list)
            if not swapped:
                # If no swaps occurred during this pass, the list is already sorted
                break
        print(counter)
        return input_list
