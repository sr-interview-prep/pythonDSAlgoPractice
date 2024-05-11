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
        decrement the index of left_element to sort the left portion of the list
    - else: 
        break the loop
        this means
            current element is inserted in its right place given the sorted array on the left 
            left portion of the current element is completely sorted
At the end of 1st iteration:
    - The list left to the current element's initial position is sorted
    - And the way we do it is by making sure
    
Best Case: O(n) - Occurs when the list is already sorted. In this case, insertion sort performs only one comparison per element, resulting in linear time complexity.
Average Case: O(n^2) - In the average case, insertion sort requires nested loops, resulting in quadratic time complexity.
Worst Case: O(n^2) - Occurs when the list is sorted in reverse order, and each element must be compared with and potentially moved past every other element in the sorted portion of the list.     
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
    iterations = 0
    for i in range(1, len(my_list)):
        current_element = my_list[i]
        print("current_element: ", current_element)

        j = i - 1
        while current_element < my_list[j] and j >= 0:
            my_list[j + 1] = my_list[j]
            my_list[j] = current_element
            j -= 1
            iterations += 1
            
        print(my_list)
        iterations += 1
    return my_list, iterations
