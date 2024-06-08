# Euclidean algorithm

class GCD:
    def get_gcd(self, a: int, b: int) -> int:
        if a % b == 0:
            return b
        return self.get_gcd(b, a % b)
