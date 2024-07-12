def closest_numbers(arr):
    dic = {}
    for i in range(0, len(arr)):
        for j in range(i + 1, len(arr)):
            dic[(i, j)] = abs(arr[j] - arr[i])

    min_value = 20000000000
    for k, v in dic.items():
        if v < min_value:
            min_value = v

    print(min_value, "min_value")

    for k, v in dic.items():
        if v == min_value:
            print(arr[k[0]], arr[k[1]])


closest_numbers(arr=[6, 2, 4, 10])
