"""
Trie (Prefix Tree) Implementation

A Trie is a tree-like data structure used for efficient retrieval of keys in a dataset of strings.
Each node represents a character of a word. Useful for autocomplete, spell-check, and prefix-based search.

Supported operations:
- insert(word): Add a word to the trie
- search(word): Return True if the word exists in the trie
- starts_with(prefix): Return True if any word in the trie starts with the given prefix
"""

class TrieNode:
    def __init__(self):
        self.children = {}  # Dictionary mapping char to TrieNode
        self.is_end_of_word = False  # True if node represents end of a word

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word: str) -> None:
        """Insert a word into the trie."""
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True

    def search(self, word: str) -> bool:
        """Return True if the word is in the trie."""
        node = self.root
        for char in word:
            if char not in node.children:
                return False
            node = node.children[char]
        return node.is_end_of_word

    def starts_with(self, prefix: str) -> bool:
        """Return True if any word in the trie starts with the given prefix."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return False
            node = node.children[char]
        return True

if __name__ == "__main__":
    trie = Trie()
    trie.insert("apple")
    assert trie.search("apple") == True
    assert trie.search("app") == False
    assert trie.starts_with("app") == True
    trie.insert("app")
    assert trie.search("app") == True

    # Additional test cases
    trie2 = Trie()
    # Test empty trie
    assert trie2.search("") == False
    assert trie2.starts_with("") == True  # Every trie starts with empty prefix
    # Insert and search single character
    trie2.insert("a")
    assert trie2.search("a") == True
    assert trie2.starts_with("a") == True
    assert trie2.search("b") == False
    # Insert overlapping words
    trie2.insert("bat")
    trie2.insert("batch")
    assert trie2.search("bat") == True
    assert trie2.search("batch") == True
    assert trie2.starts_with("ba") == True
    assert trie2.starts_with("batc") == True
    assert trie2.starts_with("bath") == False

    print("All test cases passed!")
