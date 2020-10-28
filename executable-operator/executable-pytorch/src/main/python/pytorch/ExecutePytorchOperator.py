import argparse
from basic.model.FunctionModel import FunctionModel
from basic.utils.ReflectUtil import ReflectUtil
from basic.model.ParamsModel import ParamsModel


class ExecutePytorchOperator():
    def __init__(self, udfpath, dagpath):
        self.udfpath = udfpath
        self.dagpath = dagpath

    def execute(self):
        functionModel = ReflectUtil.createInstanceAndMethodByPath(self.udfpath)
        try:
            inputArgs = ParamsModel(functionModel)

        except Exception as e:
            print(e)


print()

