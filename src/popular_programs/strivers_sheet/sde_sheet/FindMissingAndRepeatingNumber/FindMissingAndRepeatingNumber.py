class FindMissingAndRepeatingNumber:
    def __init__(self, input_array):
        self.input_array = input_array

    def dict_solution(self):
        repeating_number = -1
        missing_number = -1

        result: dict = {}
        for num in self.input_array:
            if result.get(num):
                result[num] += 1
            else:
                result[num] = 1
        index = 1
        for num in self.input_array:
            if result[num] == 2:
                repeating_number = num
            if not result.get(index):
                missing_number = index
            if missing_number != -1 and repeating_number != -1:
                break
            index += 1
        return {"missing_number": missing_number, "repeating_number": repeating_number}
