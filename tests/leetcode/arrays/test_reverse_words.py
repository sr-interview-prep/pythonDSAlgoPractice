from leetcode.arrays.ReverseWords import ReverseWords


def test_reverse_words():
    reverse_words = ReverseWords()
    result = reverse_words.execute(s="Let's take LeetCode contest")
    assert result == "s'teL ekat edoCteeL tsetnoc"

    result = reverse_words.execute(s="God Ding")
    assert result == "doG gniD"
