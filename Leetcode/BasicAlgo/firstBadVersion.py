'''Suppose you have n versions [1, 2, ..., n] and you want to find out the 
first bad one, which causes all the following ones to be bad.'''
# The isBadVersion API is already defined for you.
# def isBadVersion(version: int) -> bool:

def firstBadVersion(n):
        left,right=1,n
        while left<=right:
            mid=(left+right)//2
            if isBadVersion(mid) and (not isBadVersion(mid-1)):
                return mid
            elif isBadVersion(mid):
                right=mid-1
            else:
                left=mid+1
        return n