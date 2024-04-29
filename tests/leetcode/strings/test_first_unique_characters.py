from leetcode.strings.FirstUniqueCharacters import FirstUniqueCharacters


def test_first_unique_characters():
    first_unique_characters = FirstUniqueCharacters()

    result = first_unique_characters.execute(s="leetcode")
    assert result == 0

    result = first_unique_characters.execute(s="loveleetcode")
    assert result == 2

    result = first_unique_characters.execute(s="aabb")
    assert result == -1

    result = first_unique_characters.execute_alternate(s="leetcode")
    assert result == 0

    result = first_unique_characters.execute_alternate(s="loveleetcode")
    assert result == 2

    result = first_unique_characters.execute_alternate(s="aabb")
    assert result == -1
