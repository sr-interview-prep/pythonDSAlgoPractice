'''Given an integer numRows, return the first numRows of Pascal's triangle.

In Pascal's triangle, each number is the sum of the two numbers directly above it as shown:


 

Example 1:

Input: numRows = 5
Output: [[1],[1,1],[1,2,1],[1,3,3,1],[1,4,6,4,1]]
Example 2:

Input: numRows = 1
Output: [[1]]
'''
class Solution:
    def generate(self, numRows: int) -> List[List[int]]:
        res=[]
        curRow=1
        while curRow<=numRows:
            if curRow==1:
                res.append([1])
            elif curRow==2:
                res.append([1,1])
            else:
                mid=[1]
                j=0
                while j<curRow-2:
                    mid.append(res[-1][j]+res[-1][j+1])
                    j+=1
                mid.append(1)
                res.append(mid)
            curRow+=1
        return res
            