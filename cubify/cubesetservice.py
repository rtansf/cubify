import csv
import re
import json
import sys
from pymongo import MongoClient
from pprint import pprint
from datetime import datetime, date
from timestring import Date, TimestringInvalid
from cubeservice import CubeService

class CubeSetService:

    def __init__(self, dbName):
        self.dbName = dbName
        client = MongoClient()
        self.db = client[dbName]
        self.cubeService = CubeService(dbName)

    #
    # Update an arbitrary field in cubeset
    #
    def __updateCubeSetProperty__(self, cubeSetName, update):
        self.db['cubeset'].update_one({ "name" : cubeSetName}, update)


    #
    # Get a cube set
    #
    def getCubeSet(self, cubeSetName):
        return self.db['cubeset'].find_one({ "name": cubeSetName})


    #
    # Create a cube set including binning and optionally aggregation
    #
    def createCubeSet(self, owner, cubeSetName, csvFilePath, binnings=None, aggs=None):

        # Make sure cubeSetName is unique
        existing = self.getCubeSet(cubeSetName)
        if existing != None:
            raise ValueError('Cube Set with ' + cubeSetName + ' already exists')

        sourceCubeName = cubeSetName + "_source"
        sourceCube = self.cubeService.createCubeFromCsv(csvFilePath, sourceCubeName)
        binnedCubeName = cubeSetName + "_binned"
        if binnings != None:
            binnedCube = self.cubeService.binCubeCustom(binnings, sourceCube, binnedCubeName)
        else:
            binnedCube = self.cubeService.binCube(sourceCube, binnedCubeName)

        if aggs != None:
                self.cubeService.aggregateCubeCustom(binnedCube, aggs)

        # Now save the cubeSet
        cubeSet = {}
        cubeSet['name'] = cubeSetName
        cubeSet['owner'] = owner
        cubeSet['csvFilePath'] = csvFilePath
        cubeSet['createdOn'] = datetime.utcnow()
        cubeSet['sourceCube'] = sourceCubeName
        cubeSet['binnedCube'] = binnedCubeName
        if aggs != None:
            aggCubeNames = []
            for agg in aggs:
                aggCubeNames.append(binnedCubeName + "_" + agg['name'])
            cubeSet['aggCubes'] = aggCubeNames
    
        self.db['cubeset'].insert_one(cubeSet)        
        
        return cubeSet

    #
    # Add rows to source cube
    #
    def addRowsToSourceCube(self, cubeSet, csvFilePath):
        if cubeSet == None:
            return

        existingSourceCube = self.cubeService.getCube(cubeSet['sourceCube'])
        self.cubeService.appendToCubeFromCsv(csvFilePath, existingSourceCube)

        #re-bin
        if 'binnedCube' in cubeSet:
            binnedCubeName = cubeSet['binnedCube']
            binnedCube = self.cubeService.getCube(binnedCubeName)
            self.cubeService.rebinCubeCustom(binnedCube['binnings'], existingSourceCube, cubeSet['binnedCube'])

            #re-aggregate
            if 'aggCubes' in cubeSet:
                for aggCubeName in cubeSet['aggCubes']:
                   aggCube = self.cubeService.getCube(aggCubeName)
                   aggs = []
                   aggs.append(aggCube['agg'])
                   self.cubeService.aggregateCubeCustom(binnedCube, aggs)

    #
    # Remove rows from source
    #
    def removeRowsFromSourceCube(self, cubeSet, filter):
        if cubeSet == None:
            return

        self.cubeService.deleteCubeRows(cubeSet['sourceCube'], filter)

        existingSourceCube = self.cubeService.getCube(cubeSet['sourceCube'])

        #re-bin
        if 'binnedCube' in cubeSet:
            binnedCubeName = cubeSet['binnedCube']
            binnedCube = self.cubeService.getCube(binnedCubeName)
            self.cubeService.rebinCubeCustom(binnedCube['binnings'], existingSourceCube, cubeSet['binnedCube'])

            #re-aggregate
            if 'aggCubes' in cubeSet:
                for aggCubeName in cubeSet['aggCubes']:
                   aggCube = self.cubeService.getCube(aggCubeName)
                   aggs = []
                   aggs.append(aggCube['agg'])
                   self.cubeService.aggregateCubeCustom(binnedCube, aggs)

    #
    # Delete a cube set
    #
    def deleteCubeSet(self, cubeSetName):

        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            return

        # Delete source, binned and agg cubes
        self.cubeService.deleteCube(existing['sourceCube'])
        if 'binnedCube' in existing:
            self.cubeService.deleteCube(existing['binnedCube'])
            if 'aggCubes' in existing:
                for aggCube in existing['aggCubes']:
                    self.cubeService.deleteCube(aggCube)
                   
        # Delete cubeset
        self.db['cubeset'].remove({ "name": cubeSetName })

    # 
    # Get source cube rows. Iterator to cube rows is returned.
    # 
    def getSourceCubeRows(self, cubeSet):
        if cubeSet == None:
            return []

        return self.cubeService.getCubeRowsForCube(cubeSet['sourceCube'])

    # 
    # Get binned cube rows. Iterator to cube rows is returned.
    # 
    def getBinnedCubeRows(self, cubeSet):
        if cubeSet == None:
            return []

        # Refresh the cube set
        cubeSet = self.getCubeSet(cubeSet['name'])

        if cubeSet['binnedCube'] != None:
            return self.cubeService.getCubeRowsForCube(cubeSet['binnedCube'])
        else:
            return []

    # 
    # Get aggregated cube rows. List of cube row is returned
    # 
    def getAggregatedCubeRows(self, cubeSet, aggName):
        if cubeSet == None:
            return []

        # Refresh the cube set
        cubeSet = self.getCubeSet(cubeSet['name'])

        if 'aggCubes' in cubeSet:
            if aggName == 'ALL':
                rows = []
                for aggCube in cubeSet['aggCubes']:
                    aggRows = self.cubeService.getCubeRowsForCube(aggCube)
                    for aggRow in aggRows:
                        rows.append(aggRow)
                return rows
            else:
               for aggCube in cubeSet['aggCubes']:
                  if cubeSet['binnedCube'] + "_" + aggName == aggCube:
                      return self.cubeService.getCubeRowsForCube(aggCube)

        return []

    #
    # Export source cube to csv.
    #
    def exportSourceCubeToCsv(self, cubeSet, csvFilePath):
        if cubeSet == None:
            return

        sourceCubeName = cubeSet['sourceCube']
        sourceCube = self.cubeService.getCube(sourceCubeName)
        self.cubeService.exportCubeToCsv(sourceCube, csvFilePath)

    #
    # Export binned cube to csv.
    #
    def exportBinnedCubeToCsv(self, cubeSet, csvFilePath):
        if cubeSet == None:
            return

        binnedCubeName = cubeSet['binnedCube']
        binnedCube = self.cubeService.getCube(binnedCubeName)
        self.cubeService.exportCubeToCsv(binnedCube, csvFilePath)
        
    #
    # Export agg cubes to csv.
    #
    def exportAggCubesToCsv(self, cubeSet, directoryPath):
        if cubeSet == None:
            return

        # Refresh the cube set
        cubeSet = self.getCubeSet(cubeSet['name'])

        if 'aggCubes' in cubeSet:
            for aggCubeName in cubeSet['aggCubes']:
                aggCube = self.cubeService.getCube(aggCubeName)
                csvFilePath = directoryPath + "/" + cubeSet['name'] + "_agg_" + aggCube['name'] + '.csv'
                self.cubeService.exportCubeToCsv(aggCube, csvFilePath)

    #
    # Export all component cubes of cube set to csv
    #
    def exportToCsv(self, cubeSet, directoryPath):
        if cubeSet == None:
            return
        cubeSetName = cubeSet['name']
        cubeSet = self.getCubeSet(cubeSetName) # Refresh

        self.exportSourceCubeToCsv(cubeSet, directoryPath + "/" + cubeSetName + "_source" + ".csv")
        self.exportSourceCubeToCsv(cubeSet, directoryPath + "/" + cubeSetName + "_binned" + ".csv")
        for aggCubeName in cubeSet['aggCubes']:
            aggCube = self.cubeService.getCube(aggCubeName)
            self.cubeService.exportCubeToCsv(aggCube, directoryPath + "/" + cubeSetName + "_agg_" + aggCube['name'] + '.csv')

    #
    # Perform binning on source cube
    #
    def performBinning(self, cubeSet, binnings):
        if cubeSet == None:
            return None

        sourceCube = self.cubeService.getCube(cubeSet['sourceCube'])

        # Are we rebinning?
        if cubeSet['binnedCube'] != None:
            binnedCubeName = cubeSet['binnedCube']
            if binnings != None:
                self.cubeService.rebinCubeCustom(binnings, sourceCube, cubeSet['binnedCube'])
            else:
                self.cubeService.rebinCube(sourceCube, cubeSet['binnedCube'])
        else:
            cubeSetName = cubeSet['name']
            binnedCubeName = cubeSetName + "_binned"
            if binnings != None:
                self.cubeService.binCubeCustom(binnings, sourceCube, binnedCubeName)
            else:
                self.cubeService.binCube(sourceCube, binnedCubeName, [])
            self.__updateCubeSetProperty__(cubeSetName, { "$set": {"binnedCube" : binnedCubeName}})

        return self.cubeService.getCube(binnedCubeName)

    #
    #  Given a list of dimensions, return a list of n lists of dimensions, each a unique combination of the original list
    #  For example, if the input is ['d1','d2','d3'] this returns a list of 3 lists [['d1','d2','d3'],['d1','d2'],['d1']]
    #
    def __getGroupByDimensionsList__(self, dimensions):
        result = []
        groupByList = []
        done = False
        while done==False:
            for d in dimensions:
               groupByList.append(d)
            result.append(groupByList)
            groupByList = []
            dimensions = dimensions[:-1]
            if len(dimensions) == 0:
               done = True
        return result

    # 
    # Aggregate the binned cube with an ordered list of dimensions.
    # For example if the dimensions list is ['d1', 'd2', 'd3'] the aggregation will be performed 3 times on the 
    # binned cube with group-by dimensions, ['d1', 'd2', 'd3'], ['d1', 'd2'], ['d1'] 
    #  
    def performAggregation(self, cubeSet, dimensions):
        if cubeSet == None:
            return []

        binnedCubeName = cubeSet['binnedCube']
        binnedCube = self.cubeService.getCube(binnedCubeName)
        groupByDimensionsList = self.__getGroupByDimensionsList__(dimensions)
        aggCubes = self.cubeService.aggregateCubeComplex(binnedCube, groupByDimensionsList)

        aggCubeNames = []
        for aggCube in aggCubes:
            aggCubeNames.append(aggCube['name'])

        cubeSet['aggCubes'] = aggCubeNames
        cubeSetName = cubeSet['name']
        self.__updateCubeSetProperty__(cubeSetName, { "$set": {"aggCubes" : aggCubeNames}})

        return aggCubes
       
    #
    # Perform one or more aggregations on binned cube using custom aggs.
    # The aggregated cubes are automatically saved and identified by aggName
    #
    def performAggregationCustom(self, cubeSet, aggs):
        if cubeSet == None:
            return []

        binnedCubeName = cubeSet['binnedCube']
        binnedCube = self.cubeService.getCube(binnedCubeName)
        aggCubes = self.cubeService.aggregateCubeCustom(binnedCube, aggs)

        aggCubeNames = []
        for agg in aggs:
              aggCubeNames.append(binnedCubeName + "_" + agg['name'])

        cubeSet['aggCubes'] = aggCubeNames
        cubeSetName = cubeSet['name']
        self.__updateCubeSetProperty__(cubeSetName, { "$set": {"aggCubes" : aggCubeNames}})
        
        return aggCubes
