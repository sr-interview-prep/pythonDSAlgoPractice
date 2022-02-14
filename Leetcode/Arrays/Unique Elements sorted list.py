# find the no. of unique elements in a sorted list
'''
Algo
two pointer approach
i, j
loop through j and when ith and jth element are not same:
increment i and replace ith element with jth element
this to ensure that the next of jth elements don't have dupes
'''
def uniqueElements(nums):
    if len(nums)==0:
        return 0
    i=0
    for j in range(1,len(nums)):
        print(i,j)
        if nums[i]!=nums[j]:
            i+=1
            nums[i]=nums[j]
            print(nums)    
        else:
            print(nums)
    print(nums)        
    return i+1
print(uniqueElements([1,1,1,2,2,2,3,3,4,4]))

