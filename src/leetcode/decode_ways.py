def num_encodings(s):
	def dfs(i):
		if i==len(s):
			return 1
		if s[i]=='0':
			return 0
		# taking one character
		count=dfs(i+1)
		
		if i+1<len(s) and 10<=int(s[i:i+2])<=26:
			# taking 2 characters as the double is also a valid one
			count+=dfs(i+2)	
		return count
	return dfs(0)

assert num_encodings("12") == 2        # "AB" or "L"
assert num_encodings("226") == 3       # "BBF", "BZ", "VF"
assert num_encodings("06") == 0        # Invalid due to starting with '0'
assert num_encodings("10") == 1        # Only "J"
assert num_encodings("27") == 1        # Only "BG" (27 is not a valid double)
assert num_encodings("0") == 0         # Single 0 is invalid
assert num_encodings("") == 1          # Empty string has 1 way (base case)
assert num_encodings("11106") == 2     # "AAJF" and "KJF"
print("All Test cases passed")