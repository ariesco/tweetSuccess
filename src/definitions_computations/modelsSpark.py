from crossValidation import bestLinearReggresion, bestGeneralizedLR, bestRandomForestRegressor, bestNaivebayes, bestLinearSVC, bestLogisticRegression
from pyspark.ml.regression import LinearRegressionModel, GeneralizedLinearRegressionModel, RandomForestRegressionModel
from pyspark.ml.classification import NaiveBayesModel, LinearSVCModel, LogisticRegressionModel
from pyspark.ml.regression import LinearRegression

from validateModels import ValidateModels
from SparkTool import SparkTool

CONST_LR_FILE = "file:///D:/Data_TFM/code/models/PD/LinearRegressionPD"
CONST_GLR_FILE = "file:///D:/Data_TFM/code/models/PD/GeneralizedLinearRegressionPD"
CONST_RFR_FILE = "file:///D:/Data_TFM/code/models/PD/RandomForestRegressorPD" 

CONST_NB_FILE = "file:///D:/Data_TFM/code/models/TFM/NaiveBayes"
CONST_LSVC_FILE = "file:///D:/Data_TFM/code/models/TFM/LinearSupportVectorMachine"
CONST_RFC_FILE = "file:///D:/Data_TFM/code/models/TFM/RandomForestClassifier"
CONST_LRC_FILE = "file:///D:/Data_TFM/code/models/TFM/LogisticRegressionClassification"     


class ModelsSpark():

    def __init__ (self, sparkTool):
        self.sparkTool = sparkTool
        self.lrModel = None
        self.glrModel = None
        self.rfrModel = None

        self.nbModel = None
        self.lsvrModel = None
        self.rfcModel = None
        self.lrcModel = None
        self.LSVCModel = None
        self.LRCModel = None
        self.validations = ValidateModels ()

    def getOrCreateLRC (self):
        try:
            if self.LRCModel == None:
                self.LRCModel = LogisticRegressionModel.load(CONST_LRC_FILE)
        except:
            print ("Creating LinearSVC Model")
            self.LRCModel = self.createLRC()
        
        return self.LRCModel

    def createLRC (self):
        try:
            LRCModel = bestLogisticRegression(self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "areaUnderROC")
            self.validations.validateClassification(self.sparkTool.getTestDF(), LRCModel, ValidateModels.LRC)
        except:
            print("LinearSVC = None")
            LRCModel = None
        
        try:
            LRCModel.write().overwrite().save(CONST_LSVC_FILE)
        except :
            print("Error saving NB MODEL")

        return LRCModel


    def getOrCreateLSVC (self):
        try:
            if self.LSVCModel == None:
                self.LSVCModel = LinearSVCModel.load(CONST_LSVC_FILE)
        except:
            print ("Creating LinearSVC Model")
            self.LSVCModel = self.createLSVC()
        
        return self.LSVCModel

    def createLSVC (self):
        try:
            LSVCModel = bestLinearSVC(self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "areaUnderROC")
            self.validations.validateClassification(self.sparkTool.getTestDF(), LSVCModel, ValidateModels.LSVC)
        except:
            print("LinearSVC = None")
            LSVCModel = None
        
        try:
            LSVCModel.write().overwrite().save(CONST_LSVC_FILE)
        except :
            print("Error saving NB MODEL")

        return LSVCModel
        

    def getOrCreateNB (self):
        try:
            if self.nbModel == None:
                self.nbModel = NaiveBayesModel.load(CONST_NB_FILE)
        except:
            print ("Creating NB Model")
            self.nbModel = self.createNB()
        
        return self.nbModel

    def createNB (self):
        try:
            nbModel = bestNaivebayes(self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "areaUnderROC")
            self.validations.validateClassification(self.sparkTool.getTestDF(), nbModel, ValidateModels.NB)
        except:
            print("NB = None")
            nbModel = None
        
        try:
            nbModel.write().overwrite().save(CONST_NB_FILE)
        except :
            print("Error saving NB MODEL")
        
        return nbModel


    def getOrCreateLR (self):
        try:
            if self.lrModel == None:
                self.lrModel = LinearRegressionModel.load(CONST_LR_FILE)
        except :
            print("Creating LR Model")
            self.lrModel =  self.createLR ()
        
        return self.lrModel

    def createLR (self):
        try:
            lrModel = bestLinearReggresion (self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "mse")
            self.validations.validate(self.sparkTool.getTestDF(), lrModel, ValidateModels.LR)
        except:
            print("LR = None")
            lrModel = None

        try:
            lrModel.write().overwrite().save(CONST_LR_FILE)
        except :
            print("Error saving LR MODEL")
        
        return lrModel

    def getOrCreateGLR (self):
        try:
            if self.glrModel == None:
                self.glrModel = GeneralizedLinearRegressionModel.load(CONST_GLR_FILE)
        except :
            print("Creating GLR Model")
            self.glrModel = self.createGLR ()

        return self.glrModel

    def createGLR (self):
        try:
            glrModel = bestGeneralizedLR (self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "mse")
            self.validations.validate(self.sparkTool.getTestDF(), glrModel, ValidateModels.GLR)
        except:
            print("GLR = None")
            glrModel = None
        
        try:
            glrModel.write().overwrite().save(CONST_GLR_FILE)
        except:
            print("Error saving GLR MODEL")

        return glrModel

    def getOrCreateRFR (self):
        try:
            if self.rfrModel == None:
                self.rfrModel = RandomForestRegressionModel.load(CONST_RFR_FILE)
        except :
            print("Creating RFR Model")
            self.rfrModel = self.createRFR ()

        return self.rfrModel

    def createRFR (self):
        try:
            rfrModel = bestRandomForestRegressor (self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "mse")
            self.validations.validate(self.sparkTool.getTestDF(), rfrModel, ValidateModels.RFR)

        except :
            print("RFR = None")
            rfrModel = None
        
        try:
            rfrModel.write().overwrite().save(CONST_RFR_FILE)
        except :
            print("Error saving RFR MODEL")
        
        return rfrModel

    def lrValidated (self):
        return self.validations.isValidated(ValidateModels.LR)
    
    def glrValidated (self):
        return self.validations.isValidated(ValidateModels.GLR)

    def rfrValidated (self):
        return self.validations.isValidated(ValidateModels.RFR)

    def nbValidated (self):
        return self.validations.isValidated(ValidateModels.NB)

    def getBestRegression (self, metricname=ValidateModels.MSE):
        if not self.lrValidated():
            self.getOrCreateLR()
            self.validations.validate(self.sparkTool.getTestDF(), self.lrModel, ValidateModels.LR)

        if not self.glrValidated():
            self.getOrCreateGLR()
            self.validations.validate(self.sparkTool.getTestDF(), self.glrModel, ValidateModels.GLR)

        if not self.rfrValidated():
            self.getOrCreateRFR()
            self.validations.validate(self.sparkTool.getTestDF(), self.rfrModel, ValidateModels.RFR)

        bestModel = self.lrModel
        bestMetric = self.validations.getMetrics(ValidateModels.LR, metricname)

        if bestMetric > self.validations.getMetrics(ValidateModels.GLR, metricname):
            bestMetric = self.validations.getMetrics(ValidateModels.GLR, metricname)
            bestModel = self.glrModel

        if bestMetric > self.validations.getMetrics(ValidateModels.RFR, metricname):
            bestMetric = self.validations.getMetrics(ValidateModels.RFR, metricname)
            bestModel = self.rfrModel

        return bestModel

    
    def getBestClassification (self, metricname=ValidateModels.ROC):
        if not self.nbValidated():
            self.getOrCreateNB()
            self.validations.validateClassification(self.sparkTool.getTestDF(), self.nbModel, ValidateModels.NB)

        bestModel = self.nbModel
        bestMetric = self.validations.getMetrics(ValidateModels.NB, metricname)

        return bestModel
   
    def prediction (self, isRegression, dataDF=None, model=None):
        if dataDF == None:
           return None

        if model == None:
            if isRegression:
                model = self.getBestRegression()
            else:
                model = self.getBestClassification()

        predictions = model.transform(dataDF)
        return predictions.select("features", "label", "prediction").collect()


    def predictionAndValidation (self, isRegression, dataDF=None, model=None):
        if dataDF == None:
           return None

        if model == None:
            if isRegression:
                model = self.getBestRegression()
            else:
                model = self.getBestClassification()

        predictions = model.transform(dataDF)
        return [predictions.select("features", "label", "prediction").collect(), self.validations.validate(dataDF, model, None) if isRegression else self.validations.validateClassification(dataDF, model, None)]