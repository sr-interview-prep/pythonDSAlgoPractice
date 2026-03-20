def numberToBinary(number: int):
    if number == 0:
        return "0"
    binary_str = ""
    while number > 0:
        remainder = number % 2
        binary_str = str(remainder) + binary_str
        number = number // 2
    return int(binary_str)


print(numberToBinary(2))
print(numberToBinary(4))
print(numberToBinary(5))
print(numberToBinary(11))
