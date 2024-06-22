from popular_programs.strivers_sheet.sde_sheet.SubsetSums.SubsetSums import SubsetSums


def test_sub_set_sums():
    subset_sums = SubsetSums()
    input_list = [1, 2, 3]
    expected_result = [6, 3, 4, 1, 5, 2, 3, 0]

    actual_result = subset_sums.helper_subset_sums(input_list=input_list)
    assert actual_result == expected_result
