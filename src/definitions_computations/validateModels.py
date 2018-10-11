from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator

class ValidateModels ():
    #Regression
    MSE = "mse" #Mean Squared Error
    RMSE = "rmse" #Root Mean Squared Error
    MAE = "mae" #Mean absolute error
    R2 = "r2" #Mean R^2^ metric

    LR = "lr"
    GLR = "glr"
    RFR = "rfr"

    #Classification
    ROC = "areaUnderROC"
    PR = "areaUnderPR"
    
    NB = "nb"
    LSVC = "lsvc"
    LRC = "lrc"

    def __init__ (self):
        self.metrics = {
            ValidateModels.LR : {
                ValidateModels.MSE : None,
                ValidateModels.RMSE : None,
                ValidateModels.MAE : None,
                ValidateModels.R2 : None
            },
            ValidateModels.GLR : {
                ValidateModels.MSE : None,
                ValidateModels.RMSE : None,
                ValidateModels.MAE : None,
                ValidateModels.R2 : None
            },
            ValidateModels.RFR : {
                ValidateModels.MSE : None,
                ValidateModels.RMSE : None,
                ValidateModels.MAE : None,
                ValidateModels.R2 : None
            },
            ValidateModels.NB : {
                ValidateModels.ROC : None,
                ValidateModels.PR : None,
            },
            ValidateModels.LSVC : {
                ValidateModels.ROC : None,
                ValidateModels.PR : None,
            },
            ValidateModels.LRC : {
                ValidateModels.ROC : None,
                ValidateModels.PR : None,
            }
        }
    
    def validate (self, df, model, modelName=None):
        predictions0 = model.transform(df)

        evaluator = RegressionEvaluator(metricName=ValidateModels.MSE)
        mse = evaluator.evaluate(predictions0)
        evaluator.setMetricName(ValidateModels.RMSE)
        rmse = evaluator.evaluate(predictions0)
        evaluator.setMetricName(ValidateModels.MAE)
        mae = evaluator.evaluate(predictions0)
        evaluator.setMetricName(ValidateModels.R2)
        r2 = evaluator.evaluate(predictions0)

        if modelName != None:
            self.metrics[modelName][ValidateModels.MSE] = mse
            self.metrics[modelName][ValidateModels.RMSE] = rmse
            self.metrics[modelName][ValidateModels.MAE] = mae
            self.metrics[modelName][ValidateModels.R2] = r2
        
        return {
            ValidateModels.MSE : mse,
            ValidateModels.RMSE : rmse,
            ValidateModels.MAE : mae,
            ValidateModels.R2 : r2
        }

    def validateClassification (self, df, model, modelName=None):
        predictions0 = model.transform(df)

        evaluator = BinaryClassificationEvaluator(metricName=ValidateModels.ROC)
        roc = evaluator.evaluate(predictions0)
        evaluator.setMetricName(ValidateModels.PR)
        pr = evaluator.evaluate(predictions0)

        if modelName != None:
            self.metrics[modelName][ValidateModels.ROC] = roc
            self.metrics[modelName][ValidateModels.PR] = pr
        
        return {
            ValidateModels.ROC : roc,
            ValidateModels.PR : pr
        }
    
    def getMetrics (self, modelName=None, metricName=None):
        if metricName != None and modelName != None:
            return self.metrics[modelName][metricName]
        
        elif metricName == None and modelName != None:
            return self.metrics[modelName]

        else:
            return self.metrics

    def isValidated (self, modelName):
        if modelName == ValidateModels.NB or modelName == ValidateModels.LSVC or modelName == ValidateModels.LRC:
            return self.metrics[modelName][ValidateModels.ROC] != None
        else:
            return self.metrics[modelName][ValidateModels.MSE] != None

