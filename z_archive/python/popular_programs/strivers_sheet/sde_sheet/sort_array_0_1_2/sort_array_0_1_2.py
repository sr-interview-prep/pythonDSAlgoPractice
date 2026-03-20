class SortArray012:
    def __init__(self, input_list: list):
        self.input_list = input_list

    def count_approach(self) -> list:
        count_0 = 0
        count_1 = 0
        count_2 = 0
        for num in self.input_list:
            if num == 0:
                count_0 += 1
            if num == 1:
                count_1 += 1
            if num == 2:
                count_2 += 1
        for i in range(count_0):
            self.input_list[i] = 0
        for i in range(count_0, count_0 + count_1):
            self.input_list[i] = 1
        for i in range(count_0 + count_1, len(self.input_list)):
            self.input_list[i] = 2

        return self.input_list
