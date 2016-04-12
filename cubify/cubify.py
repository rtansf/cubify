from cubeservice import CubeService
from cubesetservice import CubeSetService

class Cubify:

    def __init__(self, dbName="cubify"):
        self.cubeService = CubeService(dbName)
        self.cubeSetService = CubeSetService(dbName)

    ### Cubes

    def createCubeFromCsv(self, csvFilePath, cubeName, inMemory=False):
        return self.cubeService.createCubeFromCsv(csvFilePath, cubeName, inMemory)

    def createCubeFromCube(self, fromCube, filter, toCubeName):
        return self.cubeService.createCubeFromCube(fromCube, filter, toCubeName)

    def deleteCube(self, cubeName):
        self.cubeService.deleteCube(cubeName)

    def getCube(self, cubeName):
        return self.cubeService.getCube(cubeName)

    def queryCubeRows(self, cube, filter):
        return self.cubeService.queryCubeRows(cube, filter)

    def getCubeRows(self, cube):
        return self.cubeService.getCubeRows(cube)
        
    def exportCubeToCsv(self, cube, csvFilePath):
        return self.cubeService.exportCubeToCsv(cube, csvFilePath)

    def addColumn(self, cube, newColumnName, type, expression=None, func=None):
        self.cubeService.addColumn(cube, newColumnName, type, expression, func)

    def binCube(self, sourceCube, binnedCubeName, toBeBinned=None, hints={}):
        return self.cubeService.binCube(sourceCube, binnedCubeName, toBeBinned, hints)

    def rebinCube(self, sourceCube, binnedCubeName):
        return self.rebinCube(sourceCube, binnedCubeName)

    def binCubeCustom(self, binnings, sourceCube, binnedCubeName):
        return self.cubeService.binCubeCustom(binnings, sourceCube, binnedCubeName)

    def rebinCubeCustom(self, binnings, sourceCube, binnedCubeName):
        return self.cubeService.rebinCubeCustom(binnings, sourceCube, binnedCubeName)

    def aggregateCube(self, cube, groupByDimensions, measures=None):
        return self.cubeService.aggregateCube(cube, groupByDimensions, measures)

    def aggregateCubeCustom(self, cube, aggs):
        return self.cubeService.aggregateCubeCustom(cube, aggs)

    ####  CubeSets

    def createCubeSet(self, owner, cubeSetName, csvFilePath, binnings=None, aggs=None):
        return self.cubeSetService.createCubeSet(owner, cubeSetName, csvFilePath, binnings, aggs)

    def deleteCubeSet(self, cubeSetName):
        return self.cubeSetService.deleteCubeSet(cubeSetName)

    def getCubeSet(self, cubeSetName):
        return self.cubeSetService.getCubeSet(cubeSetName)

    def addRowsToSourceCube(self, cubeSet, csvFilePath):
        return self.cubeSetService.addRowsToSourceCube(cubeSet, csvFilePath)
        
    def removeRowsFromSourceCube(self, cubeSet, filter):
        return self.cubeSetService.removeRowsFromSourceCube(cubeSet, filter)

    def performBinning(self, cubeSet, binnings):
        return self.cubeSetService.performBinning(cubeSet, binnings)

    def performAggregation(self, cubeSetName, dimensions):
        return self.cubeSetService.performAggregation(cubeSetName, dimensions)

    def performAggregationCustom(self, cubeSetName, aggs):
        return self.cubeSetService.performAggregationCustom(cubeSetName, aggs)

    def getSourceCubeRows(self, cubeSet):
        return self.cubeSetService.getSourceCubeRows(cubeSet)

    def getBinnedCubeRows(self, cubeSet):
        return self.cubeSetService.getBinnedCubeRows(cubeSet)

    def getAggregatedCubeRows(self, cubeSet, aggName):
        return self.cubeSetService.getAggregatedCubeRows(cubeSet, aggName)

    def exportSourceCubeToCsv(self, cubeSet, csvFilePath):
        self.cubeSetService.exportSourceCubeToCsv(cubeSet, csvFilePath)

    def exportBinnedCubeToCsv(self, cubeSet, csvFilePath):
        self.cubeSetService.exportBinnedCubeToCsv(cubeSet, csvFilePath)

    def exportAggCubesToCsv(self, cubeSet, directoryPath):
        self.cubeSetService.exportAggCubesToCsv(cubeSet, directoryPath)

    def exportToCsv(self, cubeSet, directoryPath):
        self.cubeSetService.exportToCsv(cubeSet, directoryPath)


 
       
