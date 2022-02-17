class Solution:
    def rotate( nums, k) :
        """
        Do not return anything, modify nums in-place instead.
        """
        if len(nums) > k:
            nums[:]=nums[-k:]+nums[:-k]
            print(nums)
        else:
            while(k>len(nums)):
                k=k-len(nums)
            nums[:]=nums[-k:]+nums[:-k]
        return nums
    
print(Solution.rotate([1,2,3,4,5,6,7],3))
                