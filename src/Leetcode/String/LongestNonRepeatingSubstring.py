class Solution:
    def __init__(self, s: str):
        self.s = s

    def length_of_longest_substring(self) -> int:
        """
        Finds the length of the longest substring without repeating characters.

        Algorithm:
        - Use a sliding window approach with two pointers: left and right.
        - Track unique characters in the current substring using a set.
        - Move the right pointer to expand the window until a repeating character is encountered.
        - Move the left pointer to shrink the window by removing the repeating character.
        - Update the longest length seen so far.

        Time Complexity: O(n)
            - The algorithm traverses the input string once with two pointers.
        Space Complexity: O(min(n, m))
            - The space used by the set `char_set` depends on the size of the charset (m) or the size of the string (n), whichever is smaller.
            - To be more crisp in actual to simply put the char_set only stores the unique values of the string
                - Corrected Space Complexity: O(len(unique(n))

        Returns:
        - Integer representing the length of the longest substring without repeating characters.
        """
        left = 0
        right = 0
        longest = 0
        char_set = set()  # Store unique characters in the current window
        while right < len(self.s):
            if self.s[right] not in char_set:
                char_set.add(self.s[right])
                longest = max(len(char_set), longest)
                right += 1
            else:
                char_set.remove(self.s[left])
                left += 1
        return longest

    def get_longest_substring(self) -> str:
        """
        Finds the longest substring without repeating characters.

        Algorithm:
        - Utilizes a sliding window approach with two pointers: left and right.
        - Maintains a set to track unique characters within the current substring.
        - Updates the starting index and length of the longest substring when a new maximum length is found.

        Time Complexity: O(n)
            - The algorithm traverses the input string once with two pointers.
        Space Complexity: O(min(n, m))
            - The space used by the set `char_set` depends on the size of the charset (m) or the size of the string (n), whichever is smaller.

        Returns:
        - String representing the longest substring without repeating characters.
        """
        left = 0
        right = 0
        longest = 0
        longest_start = 0  # Store the starting index of the longest substring
        char_set = set()  # Store unique characters in the current window
        while right < len(self.s):
            while self.s[right] in char_set:
                char_set.remove(self.s[left])
                left += 1
            char_set.add(self.s[right])
            if right - left + 1 > longest:
                longest = right - left + 1
                longest_start = left  # Update the starting index when we find a longer substring
            right += 1
        return self.s[longest_start:longest_start + longest]
