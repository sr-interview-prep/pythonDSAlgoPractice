class Solution:
    def rotateRight(arr, d):
        arr1=[0]*len(arr)
        for i in range(len(arr)):
            n=i+d
            if n < len(arr):
                arr1[n]=arr[i]
            else:
                while n>=len(arr):
                    n=n-len(arr)
                arr1[n]=arr[i]
        return arr1
    def rotateLeft(arr, d):
        arr1=[0]*len(arr)
        for i in range(len(arr)):
            n=i-d
            if n >=0:
                arr1[n]=arr[i]
            else:
                while n<0:
                    n=n+len(arr)
                arr1[n]=arr[i]
        return arr1
# print(Solution.rotateRight([1,2,3,4,5,6,7],3))
print(Solution.rotateLeft([1,2,3,4,5,6,7],3))