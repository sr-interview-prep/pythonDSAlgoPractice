def Factorial(num):
    if num == 1:
        return 1
    return num*Factorial(num-1)

print(Factorial(4))