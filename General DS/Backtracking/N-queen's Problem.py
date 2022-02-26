'''
With brute force it is O(n!)
with back-tracking we can do in O(2^n)
'''

class QueensProblem:
    def __init__(self,n):
        self.n=n
        self.chess_table=[[None for i in range(n) for j in range(n)]]
    def print(self):
        print(self.chess_table)

q=QueensProblem(4)
q.print()
