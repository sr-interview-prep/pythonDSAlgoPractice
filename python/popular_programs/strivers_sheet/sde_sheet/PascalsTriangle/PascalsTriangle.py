class PascalsTriangle:

    @staticmethod
    def get_n_c_r(n: int, r: int):
        if r > n:
            return -1
        numerator = 1
        for _ in range(r):
            numerator = numerator * n
            n -= 1
        denominator = 1
        counter = 1
        for _ in range(r):
            denominator = denominator * counter
            counter += 1
        return numerator / denominator

    def get_variation_1(self, row: int, col: int):
        return self.get_n_c_r(n=row - 1, r=col - 1)

    def get_variation_2(self, row_no: int):
        ls = []
        for col in range(1, row_no + 1):
            ls.append(self.get_variation_1(row=row_no, col=col))
        return ls

    def get_variation_3(self, no_of_rows: int):
        ls = []
        for i in range(no_of_rows):
            ls.append(self.get_variation_2(row_no=i + 1))
        return ls
