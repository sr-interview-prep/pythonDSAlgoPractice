class Permutations:
    @staticmethod
    def execute(s1: str, s2: str) -> bool:
        if len(s2) < len(s1):
            return False
        dic1, dic2, start = {}, {}, 0
        for i in range(len(s1)):
            if s1[i] in dic1:
                dic1[s1[i]] += 1
            else:
                dic1[s1[i]] = 1
            if s2[i] in dic2:
                dic2[s2[i]] += 1
            else:
                dic2[s2[i]] = 1
        if dic1 == dic2:
            return True
        for j in range(len(s1), len(s2)):
            # moving forward the window by removing starting elements and adding extra elements from s2.
            if dic2[s2[start]] > 1:
                dic2[s2[start]] -= 1
            else:
                dic2.pop(s2[start])
            start += 1
            if s2[j] in dic2:
                dic2[s2[j]] += 1
            else:
                dic2[s2[j]] = 1
            if dic1 == dic2:
                return True
        return False
