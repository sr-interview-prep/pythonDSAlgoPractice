from popular_programs.Recursion.GCD import GCD


def test_get_gcd():
    gcd = GCD()
    result = gcd.get_gcd(10, 6)
    '''
    Values are as follows:
    10,6
    6,4
    4,2
    2 - This is the result
    '''
    assert result == 2

    result = gcd.get_gcd(20, 10)
    assert result == 10
