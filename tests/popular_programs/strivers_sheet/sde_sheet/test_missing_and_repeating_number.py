from popular_programs.strivers_sheet.sde_sheet.FindMissingAndRepeatingNumber.FindMissingAndRepeatingNumber import \
    FindMissingAndRepeatingNumber


def test_missing_and_repeating_number():
    # Test case 1
    input_array = [4, 3, 6, 2, 1, 1]
    expected_output = {"missing_number": 5, "repeating_number": 1}
    find_missing_and_repeating_number = FindMissingAndRepeatingNumber(input_array=input_array)
    result = find_missing_and_repeating_number.hashing_approach()
    assert result == expected_output

    result = find_missing_and_repeating_number.mathematical_approach()
    assert result == expected_output

    # Test case 2
    input_array = [1, 2, 2, 4, 5]
    expected_output = {"missing_number": 3, "repeating_number": 2}
    find_missing_and_repeating_number = FindMissingAndRepeatingNumber(input_array=input_array)
    result = find_missing_and_repeating_number.hashing_approach()
    assert result == expected_output

    result = find_missing_and_repeating_number.mathematical_approach()
    assert result == expected_output

    # Test case 3
    input_array = [1, 3, 3, 4, 5, 6]
    expected_output = {"missing_number": 2, "repeating_number": 3}
    find_missing_and_repeating_number = FindMissingAndRepeatingNumber(input_array=input_array)
    result = find_missing_and_repeating_number.hashing_approach()
    assert result == expected_output

    result = find_missing_and_repeating_number.mathematical_approach()
    assert result == expected_output

    # Test case 4
    input_array = [1, 1]
    expected_output = {"missing_number": 2, "repeating_number": 1}
    find_missing_and_repeating_number = FindMissingAndRepeatingNumber(input_array=input_array)
    result = find_missing_and_repeating_number.hashing_approach()
    assert result == expected_output

    result = find_missing_and_repeating_number.mathematical_approach()
    assert result == expected_output

    # Test case 5
    input_array = [2, 2]
    expected_output = {"missing_number": 1, "repeating_number": 2}
    find_missing_and_repeating_number = FindMissingAndRepeatingNumber(input_array=input_array)
    result = find_missing_and_repeating_number.hashing_approach()
    assert result == expected_output

    # Test case 6
    input_array = [3, 1, 3, 4, 2, 5, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                   28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
                   53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77,
                   78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 100]
    expected_output = {"missing_number": 99, "repeating_number": 3}
    find_missing_and_repeating_number = FindMissingAndRepeatingNumber(input_array=input_array)
    result = find_missing_and_repeating_number.hashing_approach()
    assert result == expected_output

    result = find_missing_and_repeating_number.mathematical_approach()
    assert result == expected_output
