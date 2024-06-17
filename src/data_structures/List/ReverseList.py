class ReverseList:
    def __init__(self, input_list: list):
        self.input_list = input_list

    def built_in_method(self):
        return self.input_list[::-1]

    def linear_method_basic(self):
        # Time complexity: O(N)
        # Space complexity: O(N)
        result = []
        for i in range(len(self.input_list) - 1, -1, -1):
            result.append(self.input_list[i])
        return result

    def linear_method_inplace(self):
        # Time complexity: O(N/2)
        # Space complexity: O(1)
        n = len(self.input_list)
        for i in range(n // 2):
            temp = self.input_list[i]
            self.input_list[i] = self.input_list[n - i - 1]
            self.input_list[n - i - 1] = temp
        return self.input_list
