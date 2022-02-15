class Solution:
    def containsDuplicate(self, nums: List[int]) -> bool:
        hashSet=set()
        for i in range(len(nums)):
            if nums[i] not in hashSet:
                hashSet.add(nums[i])
            else:
                return True
        return False
                