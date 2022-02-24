# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def reverseList(self, head: Optional[ListNode]) -> Optional[ListNode]:
        currNode=head
        before=None
        while currNode:
            after=currNode.next
            currNode.next=before
            before=currNode
            currNode=after
        
        return before
        