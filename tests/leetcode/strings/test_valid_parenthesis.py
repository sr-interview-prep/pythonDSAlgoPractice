from leetcode.strings.ValidParenthesis import ValidParenthesis


def test_valid_parenthesis():
    valid_parenthesis = ValidParenthesis()

    result = valid_parenthesis.execute(s="()")
    assert result is True

    result = valid_parenthesis.execute(s="()[]{}")
    assert result is True

    result = valid_parenthesis.execute(s="(]")
    assert result is False
