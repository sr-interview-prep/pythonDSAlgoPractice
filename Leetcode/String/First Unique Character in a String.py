'''Given a string s, find the first non-repeating character in it and return its index. If it does not exist, return -1.

 

Example 1:

Input: s = "leetcode"
Output: 0
Example 2:

Input: s = "loveleetcode"
Output: 2
Example 3:

Input: s = "aabb"
Output: -1
'''

class Solution:
    def firstUniqChar(self, s: str) -> int:
        hashMap=set()
        hashMap2=set()
        for i in range(len(s)):
            if s[i] not in hashMap:
                hashMap.add(s[i])
            else:
                hashMap2.add(s[i])
        for i in range(len(s)):
            if s[i] not in hashMap2:
                return i
        return -1

#dictionary approach
class Solution:
	def firstUniqChar(self, s: str) -> int:

		frequecy = {}

		for i in range(len(s)):

			if s[i] not in frequecy:
				frequecy[s[i]] = 1
			else:
				frequecy[s[i]] = frequecy[s[i]] + 1

		for i in range(len(s)):
			if frequecy[s[i]]==1:
				return i

		return -1
            
            