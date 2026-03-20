'''# Definition for singly-linked list.
Given the head of a sorted linked list, delete all duplicates such that each element appears only once. Return the linked list sorted as well.

 

Example 1:


Input: head = [1,1,2]
Output: [1,2]
Example 2:


Input: head = [1,1,2,3,3]
Output: [1,2,3]
 '''

# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def deleteDuplicates(self, head: Optional[ListNode]) -> Optional[ListNode]:
        dmNode=ListNode(0)
        dmNode.next=head
        currNode=dmNode
        hashMap=set()
        while currNode.next:
            if currNode.next.val not in hashMap:
                hashMap.add(currNode.next.val)
                currNode=currNode.next
            else:
                currNode.next=currNode.next.next         
        
        return dmNode.next