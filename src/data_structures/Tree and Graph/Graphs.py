class Graph:
    def __init__(self):
        self.adj_list = {}

    def add_vertex(self, vertex):
        if vertex not in self.adj_list.keys():
            self.adj_list[vertex] = []
            return True
        return False

    def add_edge(self, v1, v2):
        if v1 in self.adj_list.keys() and v2 in self.adj_list.keys():
            self.adj_list[v1].append(v2)
            self.adj_list[v2].append(v1)
            return True
        return False

    def remove_edge(self, v1, v2):
        if v1 in self.adj_list.keys() and v2 in self.adj_list.keys():
            self.adj_list[v1].remove(v2)
            self.adj_list[v2].remove(v1)
            return True
        return False

    def remove_vertex(self, vertex):
        if vertex in self.adj_list.keys():
            for i in self.adj_list[vertex]:
                self.adj_list[i].remove(vertex)
            self.adj_list.pop(vertex)
            return True
        return False

    def print_graph(self):
        for k, v in self.adj_list.items():
            print(k, ' : ', v)


if __name__ == "__main__":
    g = Graph()
    g.add_vertex('1')
    g.add_vertex('2')
    g.add_vertex('3')
    g.add_edge('1', '2')
    g.add_edge('1', '3')
    g.print_graph()
    # g.remove_edge('1','2')
    g.remove_vertex('3')
    g.print_graph()
