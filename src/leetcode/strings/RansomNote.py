'''
Given two strings ransomNote and magazine, return true if ransomNote can be constructed from magazine and false otherwise.

Each letter in magazine can only be used once in ransomNote.

 

Example 1:

Input: ransomNote = "a", magazine = "b"
Output: false
Example 2:

Input: ransomNote = "aa", magazine = "ab"
Output: false
Example 3:

Input: ransomNote = "aa", magazine = "aab"
Output: true
'''

class Solution:
    def canConstruct(self, ransomNote: str, magazine: str) -> bool:
        
        frequency_m={}
        for i in magazine:
            if i not in frequency_m:
                frequency_m[i]=1
            else:
                frequency_m[i]=frequency_m[i]+1
                
        frequency_r={}
        for i in ransomNote:
            if i not in frequency_r:
                frequency_r[i]=1
            else:
                frequency_r[i]=frequency_r[i]+1
        
        for i in ransomNote:
            if i not in frequency_m:
                return False
            else:
                if frequency_m[i]<frequency_r[i]:
                    return False
        return True
        