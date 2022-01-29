'''
Algorithm
1) Keep breaking the list in 2 until there are ind lists of ind elements of the original list
2) Take comb of two compare and make 2 element sorted list and repeat the process until final list
'''
'''O(n) is the space complexity as the list is broken into n chunks of lists
for breaking the list, it is log(n) 
for going through each element and combining it is n
Therefore, o(nlogn) is the time complexity'''
def merge(list1, list2):
    combined=[]
    i=0
    j=0
    while i<len(list1) and j< len(list2):
        if list1[i]<list2[j]:
            combined.append(list1[i])
            i+=1
        else:
            combined.append(list2[j])
            j+=1
    while(i<len(list1)):
        combined.append(list1[i])
        i+=1
    while(j<len(list2)):
        combined.append(list2[j])
        j+=1
    return combined

def merge_sort(my_list):
    if len(my_list)==1:
        return my_list
    mid=int(len(my_list)/2)
    left=my_list[:mid]
    right=my_list[mid:]
    return merge(merge_sort(left),merge_sort(right))

print(merge_sort([3,1,4,2]))