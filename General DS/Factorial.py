def Factorial(num):
    if num == 1 or num==0:
        return 1
    if num <0:
        return None
    return num*Factorial(num-1)

print(Factorial(-1))