from leetcode.strings.Anagram import Anagram


def test_is_anagram():
    anagram = Anagram(base_str='anagram', test_str='nagaram')
    is_anagram = anagram.is_anagram()
    assert is_anagram is True

    anagram = Anagram(base_str='rat', test_str='car')
    is_anagram = anagram.is_anagram()
    assert is_anagram is False
