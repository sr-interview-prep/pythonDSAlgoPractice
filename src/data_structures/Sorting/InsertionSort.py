"""
Works best in the case of small Array's (smaller than 10)
Where possibility of achieving linear time complexity is more
"""

'''
Algorithm
Start from the second element (index 1) of the list.
Pick the current element and store it in a temporary variable (temp).
Compare current element to each of the left side elements of the list
    - If  current_element<left_element:
        left_element is moved to its right
        left_element=current_element
    - else: 
        break the loop
        this means
            current element is inserted in its right place given the sorted array on the left 
            left portion of the current element is completely sorted
At the end of 1st iteration:
    - The list left to the current element's initial position is sorted
    - And the way we do it is by making sure     
'''


# current_element:  2
# [4, 2, 6, 5, 1, 3]
# [2, 4, 6, 5, 1, 3]
# current_element:  6
# [2, 4, 6, 5, 1, 3]
# current_element:  5
# [2, 4, 6, 5, 1, 3]
# [2, 4, 5, 6, 1, 3]
# current_element:  1
# [2, 4, 5, 6, 1, 3]
# [2, 4, 5, 1, 6, 3]
# [2, 4, 1, 5, 6, 3]
# [2, 1, 4, 5, 6, 3]
# [1, 2, 4, 5, 6, 3]
# current_element:  3
# [1, 2, 4, 5, 6, 3]
# [1, 2, 4, 5, 3, 6]
# [1, 2, 4, 3, 5, 6]
# [1, 2, 3, 4, 5, 6]


def insertion_sort(my_list):
    print(my_list)
    for i in range(1, len(my_list)):
        current_element = my_list[i]
        print("current_element: ", current_element)

        j = i - 1
        while my_list[j] > current_element and j >= 0:
            print(my_list)
            my_list[j + 1] = my_list[j]
            my_list[j] = current_element
            j -= 1
        print(my_list)
    return my_list
