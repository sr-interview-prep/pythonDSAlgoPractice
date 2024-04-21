# If 0 needs to go from source to dest
# if not, go from source to mid and then from mid to dest

class TowersOfHanoi:
    def get_result(self, disk, source, middle, destination):
        results = []
        if disk == 0:
            result_string = 'disk %s from %s to %s' % (disk, source, destination)
            results.append(result_string)
            print(result_string)
            return results
        self.get_result(disk - 1, source, destination, middle)
        result_string = 'disk %s from %s to %s' % (disk, source, destination)
        results.append(result_string)
        print(result_string)
        self.get_result(disk - 1, middle, source, destination)
