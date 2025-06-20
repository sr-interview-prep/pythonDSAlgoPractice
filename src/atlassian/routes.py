## Extend this to take parameters


class Router:
    def __init__(self):
        self.routes={}
    
    def add_route(self, route, result):
        route_segments= tuple(segment for segment in route.strip('/').split('/'))
        if route_segments not in self.routes:
            self.routes[route_segments]=result
            return True
        return False
    
    def _is_match(self,route_segs,request_segs):
        if len(route_segs)!=len(request_segs):
            return False
        
        for route_seg, request_seg in zip(route_segs, request_segs):
            if route_seg!="*" and request_seg!=route_seg:
                return False
        return True
        
    
    def call_route(self, request):
        request_segments= tuple(segment for segment in request.strip('/').split('/'))
        if request_segments in self.routes:
            return self.routes[request_segments]
        
        for route_segments, result in self.routes.items():
            if self._is_match(route_segments, request_segments):
                return result
        return False 
    


router=Router()
print(router.add_route('/foo/bar','foo'))
print(router.add_route('/foo/*','dsadsa'))
print(router.call_route('/foo/bar'))
print(router.call_route('/foo/kan'))



        
        
        
    
            
        