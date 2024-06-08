from popular_programs.strivers_sde_sheet.KadanesAlgo.kadanes_algo import KadanesAlgo


def test_kadanes_algo():
    arr = [-2, 1, -3, 4, -1, 2, 1, -5, 4]
    kadanes_algo = KadanesAlgo(arr=arr)
    assert kadanes_algo.brute_force() == 6
    assert kadanes_algo.better_approach() == 6
    assert kadanes_algo.optimal_approach_ka() == 6
    assert kadanes_algo.optimal_approach_with_sub_array() == (6, [4, -1, 2, 1])
