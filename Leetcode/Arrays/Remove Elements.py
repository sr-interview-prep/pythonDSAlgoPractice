'''Remove a particular element and count the remaining elements in the list'''
def removeElement(nums, val): 
    i = 0
    for j in range(len(nums)):
        print(i,j)
        if nums[j] != val: 
            nums[i] = nums[j]
            i+=1
            print(nums)
        else:
            print(nums)
    return i
print(removeElement([0,1,2,2,3,0,4,2],2))