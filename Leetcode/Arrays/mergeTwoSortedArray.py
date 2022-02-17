class Solution(object):
    def merge(nums1, m, nums2, n):
        """
        :type nums1: List[int]
        :type m: int
        :type nums2: List[int]
        :type n: int
        :rtype: None Do not return anything, modify nums1 in-place instead.
        """
        i = m-1
        j = n-1
        index1 = m+n-1
        
        while i >= 0 and j >= 0:
            if nums1[i] > nums2[j]:
                nums1[index1] = nums1[i]
                i -= 1
                index1 -= 1
            
            elif nums2[j] > nums1[i]:
                nums1[index1] = nums2[j]
                j -= 1
                index1 -= 1
                
            else:
                nums1[index1] = nums1[i]
                nums1[index1-1] = nums2[j]
                i -= 1
                j -= 1
                index1 -= 2
                
        while j >= 0:
            nums1[index1] = nums2[j]
            j -= 1
            index1 -= 1
        return nums1

print(Solution.merge([1,2,3,0,0,0],3,[2,5,6],3))