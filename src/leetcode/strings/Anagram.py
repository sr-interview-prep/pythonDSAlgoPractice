"""
Given two strings s and t, return true if t is an anagram of s, and false otherwise.

An Anagram is a word or phrase formed by rearranging the letters of a different word or phrase, typically using all the original letters exactly once.

 

Example 1:

Input: s = "anagram", t = "nagaram"
Output: true
Example 2:

Input: s = "rat", t = "car"
Output: false
"""


class Anagram:
    def __init__(self, base_str: str, test_str: str):
        self.base_str = test_str
        self.test_str = base_str

    def is_anagram(self) -> bool:
        if len(self.base_str) != len(self.test_str):
            return False
        hash_map = {}
        for i in self.base_str:
            if i not in hash_map:
                hash_map[i] = 1
            else:
                hash_map[i] += 1

        for i in self.test_str:
            if i not in hash_map:
                return False
            else:
                hash_map[i] -= 1
                if hash_map[i] == 0:
                    hash_map.pop(i)

        if not hash_map:
            return True


if __name__ == "__main__":
    anagram = Anagram("anagram", "nagaram")
    assert (anagram.is_anagram()) == True
    print("passed")
    anagram = Anagram("rat", "car")
    assert (anagram.is_anagram()) == False
    print("passed")

    #     def test_invalid_anagram(self):
    #         anagram = Anagram("rat", "car")
    #         self.assertFalse(anagram.is_anagram())
    #
    #     def test_empty_strings(self):
    #         anagram = Anagram("", "")
    #         self.assertTrue(anagram.is_anagram())
    #
    #     def test_different_lengths(self):
    #         anagram = Anagram("a", "ab")
    #         self.assertFalse(anagram.is_anagram())
    #
    #     def test_same_characters_different_counts(self):
    #         anagram = Anagram("aabbcc", "abc")
    #         self.assertFalse(anagram.is_anagram())
    #
    #
    # unittest.main()
