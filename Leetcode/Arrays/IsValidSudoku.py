class Solution:
    def isValidSudoku(self, board: List[List[str]]) -> bool:
        boardLength=len(board)
        # Horizontal Scan
        for i in range(boardLength):
            diction={}
            for j in range(boardLength):
                if board[i][j]==".": continue
                if board[i][j] in diction:
                    return False
                else:
                    diction[board[i][j]]=1
            
        # Vertical Scan
        for i in range(boardLength):
            diction={}
            for j in range(boardLength):
                if board[j][i]==".": continue
                if board[j][i] in diction:
                    return False
                else:
                    diction[board[j][i]]=1
            
        #3*3 small cube scan
        for i in range(0, boardLength, 3):
            for j in range(0, boardLength,3):
                diction={}
                for k in range(i,i+3):
                    for l in range(j,j+3):
                        if board[k][l]==".": continue
                        if board[k][l] in diction:
                            return False
                        else:
                            diction[board[k][l]]=1
                
        return True
                
