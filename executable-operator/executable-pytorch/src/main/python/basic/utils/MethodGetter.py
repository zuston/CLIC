import inspect
# from basic.utils.ReflectUtil import ReflectUtil


class MethodGetter:
    def __init__(self, classToGetMethod):
        self.classToGetMethod = classToGetMethod
        self.methodList = []

    def getMethodList(self):
        for method, value in inspect.getmembers(self.classToGetMethod):
            if not method.startswith("_"):
                self.methodList.append(method)
        return self.methodList


if __name__ == "__main__":
    pass
    # print(MethodGetter(ReflectUtil).getMethodList())

