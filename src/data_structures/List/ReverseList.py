class ReverseList:
    def __init__(self, input_list: list):
        self.input_list = input_list

    def built_in_method(self):
        return self.input_list[::-1]

    def linear_method(self):
        # Time complexity: O(N)
        result = []
        for i in range(len(self.input_list) - 1, -1, -1):
            result.append(self.input_list[i])
        return result
