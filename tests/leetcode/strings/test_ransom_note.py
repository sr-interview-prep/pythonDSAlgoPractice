from leetcode.strings.RansomNote import RansomNote


def test_ransom_note():
    ransom_note = RansomNote()
    result = ransom_note.execute(ransom_note="a", magazine="b")
    assert result is False

    result = ransom_note.execute(ransom_note="aa", magazine="ab")
    assert result is False

    result = ransom_note.execute(ransom_note="aa", magazine="aab")
    assert result is True
