from SparkTool import SparkTool
from modelsSpark import ModelsSpark
import datetime
from pyspark.mllib.evaluation import RegressionMetrics



if __name__ == "__main__":
    modelType = SparkTool.CONST_TYPE_REGRESSION
    sparkTool = SparkTool(modelType, "test1", "clearTweet")
    modelSpark = ModelsSpark(sparkTool)

    
    if modelType == SparkTool.CONST_TYPE_CLASIFICATION:
        model1 = modelSpark.getOrCreateLRC()
        #model2 =
        #model3 =  
    else:
        print ("\nStart LR: {0}".format(datetime.datetime.now()))
        model1 = modelSpark.getOrCreateLR()
        print ("Stop LR: {0}".format(datetime.datetime.now()))

        print ("\nStart GLR: {0}".format(datetime.datetime.now()))
        model2 = modelSpark.getOrCreateGLR()
        print ("Stop GLR: {0}".format(datetime.datetime.now()))

        print ("\nStart RFR: {0}".format(datetime.datetime.now()))
        model3 = modelSpark.getOrCreateRFR()
        print ("Stop RFR: {0}".format(datetime.datetime.now()))
        

    result1 = modelSpark.predictionAndValidation(modelType == SparkTool.CONST_TYPE_REGRESSION, sparkTool.getTestDF(), model1)
    result2 = modelSpark.predictionAndValidation(modelType == SparkTool.CONST_TYPE_REGRESSION, sparkTool.getTestDF(), model2)
    result3 = modelSpark.predictionAndValidation(modelType == SparkTool.CONST_TYPE_REGRESSION, sparkTool.getTestDF(), model3)
    
    """
    metrics = RegressionMetrics(sparkTool.getTestDF().rdd.map(list))
    # Squared Error
    print("MSE = %s" % metrics.meanSquaredError)
    print("RMSE = %s" % metrics.rootMeanSquaredError)

    # R-squared
    print("R-squared = %s" % metrics.r2)

    # Mean absolute error
    print("MAE = %s" % metrics.meanAbsoluteError)

    # Explained variance
    print("Explained variance = %s" % metrics.explainedVariance)
    """
    print (result1[1])
    print (result2[1])
    print (result3[1])

    """for row in result1[0]:
        if (modelType == SparkTool.CONST_TYPE_REGRESSION and row.label > 0.1):
            print("label=%s -> LR-prediction=%s"
                % (row.label, row.prediction))"""

    """for row in result2[0]:
        if (row.label > 0.1):
            print("label=%s -> GLR-prediction=%s"
                % (row.label, row.prediction))

    
    for row in result3[0]:
        if (row.label > 0.1):
            print("label=%s -> RFR-prediction=%s"
                % (row.label, row.prediction))"""

    