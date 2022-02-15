'''Remove a particular element and count the remaining elements in the list'''
def removeElement(nums, val): 
    i = 0
    for j in range(len(nums)):
        if nums[j] != val: 
            nums[i] = nums[j]
            i+=1
    print(nums)
    return i
print(removeElement([3,3,3,2,2,2,3,3,4,5,3,3,5,5],2))