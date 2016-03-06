from cubeservice import CubeService
from cubesetservice import CubeSetService

class Cubify:

    def __init__(self, dbName="cubify"):
        self.cubeService = CubeService(dbName)
        self.cubeSetService = CubeSetService(dbName)

    ### Cubes

    def createCubeFromCsv(self, csvFilePath, cubeName, cubeDisplayName=None):
        return self.cubeService.createCubeFromCsv(csvFilePath, cubeName, cubeDisplayName)

    def createCubeFromCube(self, fromCubeName, filter, toCubeName, toCubeDisplayName=None):
        return self.createCubeFromCube(fromCubeName, filter, toCubeName, toCubeDisplayName)

    def deleteCube(self, cubeName):
        self.cubeService.deleteCube(cubeName)

    def updateCubeDisplayName(self, cubeName, cubeDisplayName):
        self.cubeService.updateCubeDisplayName(cubeName, cubeDisplayName)

    def getCube(self, cubeName):
        return self.cubeService.getCube(cubeName)

    def queryCubeRows(self, cubeName, filter):
        return self.cubeService.queryCubeRows(cubeName, filter)

    def getCubeRows(self, cubeName):
        return self.cubeService.getCubeRows(cubeName)
        
    def exportCubeToCsv(self, cubeName,csvFilePath):
        return self.cubeService.exportCubeToCsv(cubeName, csvFilePath)

    def addColumn(self, cubeName, newColumnName, type, expression=None, func=None):
        self.cubeService.addColumn(cubeName, newColumnName, type, expression, func)

    def binCube(self, sourceCubeName, binnedCubeName, toBeBinned=None, hints={}):
        return self.cubeService.binCube(sourceCubeName, binnedCubeName, toBeBinned, hints)

    def autoRebinCube(self, sourceCubeName, binnedCubeName):
        return self.autoRebinCube(sourceCubeName, binnedCubeName)

    def binCubeCustom(self, binnings, sourceCubeName, binnedCubeName, binnedCubeDisplayName=None):
        return self.cubeService.binCubeCustom(binnings, sourceCubeName, binnedCubeName, binnedCubeDisplayName)

    def rebinCubeCustom(self, binnings, sourceCubeName, binnedCubeName):
        return self.cubeService.rebinCubeCustom(binnings, sourceCubeName, binnedCubeName)
  
    def aggregateCube(self, cubeName, aggs):
        return self.cubeService.aggregateCube(cubeName, aggs)     

    ####  CubeSets

    def createCubeSet (self, owner, cubeSetName, cubeSetDisplayName, csvFilePath, binnings=None, aggs=None):
        return self.cubeSetService.createCubeSet(owner, cubeSetName, cubeSetDisplayName, csvFilePath, binnings, aggs)

    def deleteCubeSet(self, cubeSetName):
        return self.cubeSetService.deleteCubeSet(cubeSetName)

    def getCubeSet(self, cubeSetName):
        return self.cubeSetService.getCubeSet(cubeSetName)

    def addRowsToSourceCube(self, cubeSetName, csvFilePath):
        return self.cubeSetService.addRowsToSourceCube(cubeSetName, csvFilePath)
        
    def removeRowsFromSourceCube(self, cubeSetName, filter):
        return self.cubeSetService.removeRowsFromSourceCube(cubeSetName, filter)

    def updateCubeSetDisplayName(self, cubeSetName, displayName):
        self.cubeSetService.updateCubeSetDisplayName(cubeSetName, displayName)

    def performBinning(self, cubeSetName, binnings):
        self.cubeSetService.performBinning(self, cubeSetName, binnings)

    def performAggregation(self, cubeSetName, aggs):
        self.cubeSetService.performAggregation(self, cubeSetName, aggs)

    def getSourceCubeRows(self, cubeSetName):
        return self.cubeSetService.getSourceCubeRows(cubeSetName)

    def getBinnedCubeRows(self, cubeSetName):
        return self.cubeSetService.getBinnedCubeRows(cubeSetName)

    def getAggregatedCubeRows(self, cubeSetName, aggName):
        return self.cubeSetService.getAggregatedCubeRows(cubeSetName, aggName)

    def exportSourceCubeToCsv(self, cubeSetName, csvFilePath):
        self.cubeSetService.exportSourceCubeToCsv(cubeSetName, csvFilePath)

    def exportBinnedCubeToCsv(self, cubeSetName, csvFilePath):
        self.cubeSetService.exportBinnedCubeToCsv(cubeSetName, csvFilePath)

    def exportAggCubeToCsv(self, cubeSetName, csvFilePath, aggName):
        self.cubeSetService.exportAggCubeToCsv(cubeSetName, csvFilePath, aggName)




 
       
