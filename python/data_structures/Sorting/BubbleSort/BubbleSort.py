"""
Algorithm
Start with the 1st element
Check if the next element is less than the left element, if yes swap
At the end of this iteration, the largest number bubbles up to the right most index of the list
Now, repeat the process and go only until the penultimate element as the last element is already in its correct position
So the time complexity of operations is n(n-1)(n-2).. = n!


Best Case: O(n) - This occurs when the list is already sorted, and the algorithm needs only one pass to confirm this.
-- We are able to do this using the swapped flag
Average Case: O(n^2) - In the average case, bubble sort requires nested loops, resulting in quadratic time complexity.
Worst Case: O(n^2) - This occurs when the list is sorted in reverse order, and bubble sort needs to make the maximum number of swaps.

Actual Calculation for Best case scenario:
- N-1
- Because if N-1 comparisons are done, the nth element by default is considered sorted

Actual Calculation:
- For a list of length 𝑛, in each iteration of the outer loop, the inner loop performs n−1 comparisons.
- The number of comparisons decreases by 1 in each subsequent iteration of the outer loop
- Therefore, the total number of comparisons :
 = (n-1)+(n-2)+(n-3)...+1
 = 1+2+....(n-2)+(n-1)
 = (n-1)((n-1)+1)/2
 =n(n-1)/2
"""
from typing import List


def swap(input_list, index_1, index_2):
    temp = input_list[index_1]
    input_list[index_1] = input_list[index_2]
    input_list[index_2] = temp
    return input_list


class BubbleSort:

    @staticmethod
    def sort(input_list: List):
        iterations = 0
        n = len(input_list)
        swap_flag = False
        for i in range(n - 1, 0, -1):
            for j in range(i):
                iterations += 1
                if input_list[j + 1] < input_list[j]:
                    swap_flag = True
                    input_list = swap(input_list, j + 1, j)
            if not swap_flag:
                break
        return input_list, iterations
