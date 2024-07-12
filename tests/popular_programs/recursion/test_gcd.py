from popular_programs.Recursion.GCD import GCD


def test_get_gcd():
    gcd = GCD()
    result = gcd.get_gcd(10, 6)
    assert result == 2

    result = gcd.get_gcd(20, 10)
    assert result == 10
