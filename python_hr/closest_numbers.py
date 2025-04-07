def closest_pair(arr):
    arr.sort()
    min_diff = float('inf')
    result = ()
    for i in range(1, len(arr)):
        diff = arr[i] - arr[i - 1]
        if diff < min_diff:
            min_diff = diff
            result = (arr[i - 1], arr[i])
    return result


if __name__ == "__main__":
    print(closest_pair(arr=[6, 2, 4, 10]))
