def two_sum(nums: list, target: int) -> tuple:
    hash_map = {}
    for i in range(len(nums)):
        hash_map[nums[i]] = i
        if target - nums[i] in hash_map:
            return hash_map[target - nums[i]], i


if __name__ == "__main__":
    nums = [2, 7, 11, 15]
    target = 9
    assert two_sum(nums, target) == (0, 1)
    print("test passed")
