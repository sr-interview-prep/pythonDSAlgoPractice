class Factorial:

    def get_factorial(self, num) -> int:
        if num == 1 or num == 0:
            return 1
        if num < 0:
            return -1
        result = num * self.get_factorial(num - 1)
        return result
