from cubeservice import CubeService
from cubesetservice import CubeSetService

class Cubify:

    def __init__(self, dbName="cubify"):
        self.cubeService = CubeService(dbName)
        self.cubeSetService = CubeSetService(dbName)

    def createCubeFromCsv(self, csvFilePath, cubeName, cubeDisplayName=None):
        return self.cubeService.createCubeFromCsv(csvFilePath, cubeName, cubeDisplayName)

    def deleteCube(self, cubeName):
        self.cubeService.deleteCube(cubeName)

    def updateCubeDisplayName(self, cubeName, cubeDisplayName):
        self.cubeService.updateCubeDisplayName(cubeName, cubeDisplayName)

    def getCube(self, cubeName):
        return self.cubeService.getCube(cubeName)

    def queryCubeCells(self, cubeName, filter):
        return self.cubeService.queryCubeCells(cubeName, filter)

    def getCubeCells(self, cubeName):
        return self.cubeService.getCubeCells(cubeName)
        
    def exportCubeToCsv(self, cubeName,csvFilePath):
        return self.cubeService.exportCubeToCsv(cubeName, csvFilePath)

    def binCube(self, binnings, sourceCubeName, binnedCubeName, binnedCubeDisplayName=None):
        return self.cubeService.binCube(binnings, sourceCubeName, binnedCubeName, binnedCubeDisplayName)

    def rebinCube(self, binnings, sourceCubeName, binnedCubeName):
        return self.cubeService.rebinCube(binnings, sourceCubeName, binnedCubeName)
  
    def aggregateCube(self, cubeName, aggs):
        return self.cubeService.aggregateCube(cubeName, aggs)         
    
   
