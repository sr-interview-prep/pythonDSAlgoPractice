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

    # def mathematical_approach(self):
    '''
    Time Complexity: O(N)
    Space Complexity: O(1)
    
    Sum(N) = Natural-Array 
    Sum(N^2)= Natural-Array
    
    X-Y=val1
    X^2-Y^2=val2
    
    Solve and get X, Y
    X is the missing number
    Y is the repeating number
    
    
    '''

    # def xor_gate_approach(self):
    # https://takeuforward.org/data-structure/find-the-repeating-and-missing-numbers/
    # Check Later
