"""
Works best in the case of small Array's (smaller than 10)
Where possibility of achieving linear time complexity is more
"""

'''
Algorithm
Start from the second element (index 1) of the list.
Pick the current element and store it in a temporary variable (temp).
Compare with previous elements: Move backwards through the sorted portion of the list (from the current position to the beginning)
Shifting each element to the right until you find the correct position for the current element.
Insert the element: Once you find the correct position (either because you reached the beginning of the list or you found an element smaller than the current one), insert the temporary variable (temp) into that position.
Repeat: Continue this process for each element in the list, expanding the sorted portion with each iteration.
Return the sorted list.
'''


# [4, 2, 6, 5, 1, 3]
# [2, 4, 6, 5, 1, 3]
# [2, 4, 6, 5, 1, 3]
# [2, 4, 6, 5, 1, 3]
# [2, 4, 5, 6, 1, 3]
# [2, 4, 5, 6, 1, 3]
# [2, 4, 5, 1, 6, 3]
# [2, 4, 1, 5, 6, 3]
# [2, 1, 4, 5, 6, 3]
# [1, 2, 4, 5, 6, 3]
# [1, 2, 4, 5, 6, 3]
# [1, 2, 4, 5, 3, 6]
# [1, 2, 4, 3, 5, 6]
# [1, 2, 3, 4, 5, 6]

def insertion_sort(my_list):
    for i in range(1, len(my_list)):
        temp = my_list[i]
        j = i - 1
        while temp < my_list[j] and j > -1:
            print(my_list)
            my_list[j + 1] = my_list[j]
            my_list[j] = temp
            j -= 1
        print(my_list)
    return my_list
