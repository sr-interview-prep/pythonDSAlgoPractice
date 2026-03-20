class FindMissingAndRepeatingNumber:
    def __init__(self, input_array):
        self.input_array = input_array

    def hashing_approach(self):
        """
        Time Complexity: O(2N)
        Space Complexity: O(1)
        """
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

    def mathematical_approach(self):
        """
        Time Complexity: O(N)
        Space Complexity: O(1)

        Sum(N) = Natural-Array
        Sum(N^2)= Natural-Array

        X-Y=diff_sum
        X^2-Y^2=diff_sum_square
        (X+Y)(X-Y)=diff_sum_square

        X+Y=diff_sum_square/diff_sum
        X-Y=diff_sum

        2X=(diff_sum_square/diff_sum)+diff_sum
        X=((diff_sum_square/diff_sum)+diff_sum)/2
        Y=X-diff_sum

        Solve and get X, Y
        X is the missing number
        Y is the repeating number


        """
        index = 1
        sum_array = 0
        sum_natural_numbers = 0
        sum_square_array = 0
        sum_square_natural_numbers = 0
        for num in self.input_array:
            sum_array = sum_array + num
            sum_natural_numbers = sum_natural_numbers + index

            sum_square_array = sum_square_array + (num ** 2)
            sum_square_natural_numbers = sum_square_natural_numbers + index ** 2
            index += 1

        diff_sum = sum_natural_numbers - sum_array
        diff_sum_square = sum_square_natural_numbers - sum_square_array

        missing_number = ((diff_sum_square / diff_sum) + diff_sum) / 2
        repeating_number = missing_number - diff_sum

        return {"missing_number": missing_number, "repeating_number": repeating_number}

    # def xor_gate_approach(self):
    # https://takeuforward.org/data-structure/find-the-repeating-and-missing-numbers/
    # Check Later
