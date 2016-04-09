from cubeservice import CubeService
from cubesetservice import CubeSetService

class Cubify:

    def __init__(self, dbName="cubify"):
        self.cubeService = CubeService(dbName)
        self.cubeSetService = CubeSetService(dbName)

    ### Cubes

    def createCubeFromCsv(self, csvFilePath, cubeName):
        return self.cubeService.createCubeFromCsv(csvFilePath, cubeName)

    def createInMemoryCubeFromCsv(self, csvFilePath, cubeName):
        return self.cubeService.createInMemoryCubeFromCsv(csvFilePath, cubeName)

    def createCubeFromCube(self, fromCubeName, filter, toCubeName):
        return self.createCubeFromCube(fromCubeName, filter, toCubeName)

    def deleteCube(self, cubeName):
        self.cubeService.deleteCube(cubeName)

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

    def rebinCube(self, sourceCubeName, binnedCubeName):
        return self.rebinCube(sourceCubeName, binnedCubeName)

    def binCubeCustom(self, binnings, sourceCubeName, binnedCubeName):
        return self.cubeService.binCubeCustom(binnings, sourceCubeName, binnedCubeName)

    def rebinCubeCustom(self, binnings, sourceCubeName, binnedCubeName):
        return self.cubeService.rebinCubeCustom(binnings, sourceCubeName, binnedCubeName)

    def aggregateCube(self, cubeName, groupByDimensions, measures=None):
        return self.cubeService.aggregateCube(cubeName, groupByDimensions, measures)

    def aggregateCubeComplex(self, cubeName, groupByDimensionsList, measures=None):
        return self.cubeService.aggregateCubeComplex(cubeName, groupByDimensionsList, measures)

    def aggregateCubeCustom(self, cubeName, aggs):
        return self.cubeService.aggregateCubeCustom(cubeName, aggs)

    ####  CubeSets

    def createCubeSet(self, owner, cubeSetName, csvFilePath, binnings=None, aggs=None):
        return self.cubeSetService.createCubeSet(owner, cubeSetName, csvFilePath, binnings, aggs)

    def deleteCubeSet(self, cubeSetName):
        return self.cubeSetService.deleteCubeSet(cubeSetName)

    def getCubeSet(self, cubeSetName):
        return self.cubeSetService.getCubeSet(cubeSetName)

    def addRowsToSourceCube(self, cubeSetName, csvFilePath):
        return self.cubeSetService.addRowsToSourceCube(cubeSetName, csvFilePath)
        
    def removeRowsFromSourceCube(self, cubeSetName, filter):
        return self.cubeSetService.removeRowsFromSourceCube(cubeSetName, filter)

    def performBinning(self, cubeSetName, binnings):
        self.cubeSetService.performBinning(cubeSetName, binnings)

    def performAggregation(self, cubeSetName, dimensions):
        self.cubeSetService.performAggregation(cubeSetName, dimensions)

    def performAggregationCustom(self, cubeSetName, aggs):
        self.cubeSetService.performAggregationCustom(cubeSetName, aggs)

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




 
       
