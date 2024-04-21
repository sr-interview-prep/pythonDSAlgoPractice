'''
With brute force it is O(n!)
with back-tracking we can do in O(2^n)
'''

class QueensProblem:
    def __init__(self,n):
        self.n=n
        self.chess_table=[[None for i in range(n) for j in range(n)]]
    def solve_n_queens(self):
        if self.solve(0): 
            self.print_queens()
        else:
            print('There is no soltion for the problem')
    def solve(self, col_index):
        if col_index==self.n:
            return True
        
    def print(self):
        print(self.chess_table)

q=QueensProblem(4)
q.print()
