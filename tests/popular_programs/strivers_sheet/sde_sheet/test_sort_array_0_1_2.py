from popular_programs.strivers_sheet.sde_sheet.sort_array_0_1_2.sort_array_0_1_2 import SortArray012


def test_sort_array_0_1_2():
    input_list = [1, 0, 2, 1, 0, 0, 2, 1]
    sort_array_0_1_2 = SortArray012(input_list=input_list)
    result = sort_array_0_1_2.count_approach()
    assert result == [0, 0, 0, 1, 1, 1, 2, 2]
