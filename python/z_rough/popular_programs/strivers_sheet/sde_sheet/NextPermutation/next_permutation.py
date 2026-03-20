class NextPermutation:
    def __init__(self, input_list: list):
        self.input_list = input_list
        self.len = len(input_list)

    def optimal_approach(self):
        break_point_index = self.len - 1
        for i in range(self.len - 1, 0, -1):
            if self.input_list[i - 1] < self.input_list[i]:
                break_point_index = i - 1
                break
        for i in range(self.len - 1, 0, -1):
            if self.input_list[break_point_index] < self.input_list[i]:
                temp = self.input_list[break_point_index]
                self.input_list[break_point_index] = self.input_list[i]
                self.input_list[i] = temp
                break

        result = self.input_list[:break_point_index + 1] + self.input_list[break_point_index + 1:][::-1]
        return result
