# If 0 needs to go from source to dest
# if not, go from source to mid and then from mid to dest

def hanoi(disk, source, middle, destination):
    if disk==0:
        print('disk %s from %s to %s' %(disk,source, destination))
        return
    hanoi(disk-1,source,destination, middle)
    print('disk %s from %s to %s' %(disk,source, destination))
    hanoi(disk-1,middle, source, destination)



