from popular_programs.Recursion.Factorial import Factorial


def test_factorial():
    factorial = Factorial()
    result = factorial.get_factorial(num=5)
    assert result == 120

    result = factorial.get_factorial(num=4)
    assert result == 24

    result = factorial.get_factorial(num=6)
    assert result == 720

    result = factorial.get_factorial(num=0)
    assert result == 1

    result = factorial.get_factorial(num=1)
    assert result == 1

    result = factorial.get_factorial(num=-1)
    assert result == -1
