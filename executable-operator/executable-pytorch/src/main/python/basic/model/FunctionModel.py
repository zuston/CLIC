class FunctionModel():
    def __init__(self, obj, functionmap):
        self.obj = obj
        self.functionmap = functionmap
        self.method = None

    def invoke(self, functionName, *args):
        try:
            self.method = getattr(self.obj, self.functionmap[functionName])
            return self.method(args)
        except Exception as e:
            print(e)
        return None

