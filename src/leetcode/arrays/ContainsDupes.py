from typing import List


class ContainsDupes:
    def __init__(self, nums: List[int]):
        self.nums = nums

    def contains_dupes(self) -> bool:
        has_set = set()
        for i in range(len(self.nums)):
            if self.nums[i] not in has_set:
                has_set.add(self.nums[i])
            else:
                return True
        return False
