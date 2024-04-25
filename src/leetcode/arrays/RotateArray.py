class RotateArray:
    @staticmethod
    def rotate_right(arr, d):
        arr1 = [0] * len(arr)
        for i in range(len(arr)):
            n = i + d
            if n < len(arr):
                arr1[n] = arr[i]
            else:
                while n >= len(arr):
                    n = n - len(arr)
                arr1[n] = arr[i]
        return arr1

    @staticmethod
    def rotate_left(arr, d):
        arr1 = [0] * len(arr)
        for i in range(len(arr)):
            n = i - d
            if n >= 0:
                arr1[n] = arr[i]
            else:
                while n < 0:
                    n = n + len(arr)
                arr1[n] = arr[i]
        return arr1
