"""Problem Statement: Given an integer array arr, find the contiguous subarray (containing at least one number) which
has the largest sum and returns its sum and prints the subarray."""
import sys

"""
Example 1:
Input:
 arr = [-2,1,-3,4,-1,2,1,-5,4] 

Output:
 6 

Explanation:
 [4,-1,2,1] has the largest sum = 6. 

Examples 2:
Input:
 arr = [1] 

Output:
 1 

Explanation:
 Array has only one element and which is giving positive sum of 1
"""


class KadanesAlgo:
    def __init__(self, arr):
        self.arr = arr

    def brute_force(self):
        maxi = -sys.maxsize - 1  # minimum value integer in python

        n = len(self.arr)
        for i in range(n):
            for j in range(i, n):
                summ = 0
                for k in range(i, j + 1):
                    summ += self.arr[k]

                maxi = max(maxi, summ)
        return maxi

    def better_approach(self):
        maxi = -sys.maxsize - 1  # minimum value integer in python

        n = len(self.arr)
        for i in range(n):
            summ = 0
            for j in range(i, n):
                summ += self.arr[j]
                maxi = max(maxi, summ)
        return maxi

    def optimal_approach_ka(self):
        maxi = -sys.maxsize - 1  # maximum sum
        summ = 0

        n = len(self.arr)
        for i in range(n):
            summ += self.arr[i]

            if summ > maxi:
                maxi = summ

            # If sum < 0: discard the sum calculated
            if summ < 0:
                summ = 0

        # To consider the sum of the empty subarray
        # uncomment the following check:

        if maxi < 0:
            maxi = 0

        return maxi

    def optimal_approach_with_sub_array(self):
        maxi = -sys.maxsize - 1  # maximum sum
        summ = 0

        start = 0
        ans_start, ans_end = -1, -1
        n = len(self.arr)
        for i in range(n):

            if summ == 0:
                start = i  # starting index

            summ += self.arr[i]

            if summ > maxi:
                maxi = summ

                ans_start = start
                ans_end = i

            # If sum < 0: discard the sum calculated
            if summ < 0:
                summ = 0

        # printing the subarray:
        sub_array = self.arr[ans_start:ans_end + 1]

        # To consider the sum of the empty subarray
        # uncomment the following check:

        # if maxi < 0:
        #     maxi = 0

        return maxi, sub_array
