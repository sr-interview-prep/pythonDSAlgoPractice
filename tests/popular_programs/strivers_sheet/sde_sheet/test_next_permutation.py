from popular_programs.strivers_sheet.sde_sheet.NextPermutation.next_permutation import NextPermutation


def test_next_permutation():
    next_permutation = NextPermutation(input_list=[2, 1, 5, 4, 3, 0, 0])
    result = next_permutation.optimal_approach()
    assert result == [2, 3, 0, 0, 1, 4, 5]
