def third_element_reordering(input_list):
    if not input_list:
        return []
    result = []
    index = 2  # Start with the 3rd element (0-based index 2)

    while input_list:
        index = index % len(input_list)
        result.append(input_list.pop(index))
        index += 2

    return result


if __name__ == "__main__":
    print(third_element_reordering(input_list=[1, 2, 3, 4, 5]))
