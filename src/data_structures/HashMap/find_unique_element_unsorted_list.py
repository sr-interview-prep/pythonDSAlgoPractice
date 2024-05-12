from typing import List


def find_unique_element_unsorted_list(input_list: List):
    hash_map = {}
    for element in input_list:
        if hash_map.get(element):
            hash_map[element] += 1
        else:
            hash_map[element] = 1

    for element in hash_map:
        if hash_map[element] == 1:
            return element
