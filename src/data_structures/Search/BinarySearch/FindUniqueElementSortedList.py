"""
There is only one unique element, and remaining elements are all having exactly 2 values
"""


def find_unique_element_sorted_list(sorted_list):
    left = 0
    right = len(sorted_list) - 1

    while left <= right:
        mid = (left + right) // 2
        is_even = (right - mid) % 2 == 0

        if mid == 0 or left == right:
            return sorted_list[mid]

        if sorted_list[mid] == sorted_list[mid - 1]:
            if is_even:
                right = mid - 2
            else:
                left = mid + 1
        elif sorted_list[mid] == sorted_list[mid + 1]:
            if is_even:
                left = mid + 2
            else:
                right = mid - 1
    return None
