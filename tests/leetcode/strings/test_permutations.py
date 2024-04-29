from leetcode.strings.Permutations import Permutations


def test_permutations():
    permutations = Permutations()
    result = permutations.execute(s1="ab", s2="eidbaooo")
    assert result is True
    result = permutations.execute(s1="ab", s2="eidboaoo")
    assert result is False
