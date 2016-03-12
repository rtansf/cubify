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
    def createCubeSet(self, owner, cubeSetName, cubeSetDisplayName, csvFilePath, binnings=None, aggs=None):

        # Make sure cubeSetName is unique
        existing = self.getCubeSet(cubeSetName)
        if existing != None:
            raise ValueError('Cube Set with ' + cubeSetName + ' already exists')

        sourceCubeName = cubeSetName + "_source"
        self.cubeService.createCubeFromCsv(csvFilePath, sourceCubeName, sourceCubeName)
        binnedCubeName = cubeSetName + "_binned"
        if binnings != None:
            self.cubeService.binCubeCustom(binnings, sourceCubeName, binnedCubeName, binnedCubeName)
        else:
            self.cubeService.binCube(sourceCubeName, binnedCubeName)

        if aggs != None:
                self.cubeService.aggregateCubeCustom(binnedCubeName, aggs)

        # Now save the cubeSet
        cubeSet = {}
        cubeSet['name'] = cubeSetName
        cubeSet['displayName'] = cubeSetDisplayName
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
    def addRowsToSourceCube(self, cubeSetName, csvFilePath):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        self.cubeService.appendToCubeFromCsv(csvFilePath, existing['sourceCube'])

        #re-bin
        if 'binnedCube' in existing:
            binnedCubeName = existing['binnedCube']
            binnedCube = self.cubeService.getCube(binnedCubeName)
            self.cubeService.rebinCubeCustom(binnedCube['binnings'], existing['sourceCube'], existing['binnedCube'])

            #re-aggregate
            if 'aggCubes' in existing:
                for aggCubeName in existing['aggCubes']:
                   aggCube = self.cubeService.getCube(aggCubeName)
                   aggs = []
                   aggs.append(aggCube['agg'])
                   self.cubeService.aggregateCubeCustom(binnedCubeName, aggs)

    #
    # Remove rows from source
    #
    def removeRowsFromSourceCube(self, cubeSetName, filter):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')
        self.cubeService.deleteCubeRows(existing['sourceCube'], filter)

        #re-bin
        if 'binnedCube' in existing:
            binnedCubeName = existing['binnedCube']
            binnedCube = self.cubeService.getCube(binnedCubeName)
            self.cubeService.rebinCubeCustom(binnedCube['binnings'], existing['sourceCube'], existing['binnedCube'])

            #re-aggregate
            if 'aggCubes' in existing:
                for aggCubeName in existing['aggCubes']:
                   aggCube = self.cubeService.getCube(aggCubeName)
                   aggs = []
                   aggs.append(aggCube['agg'])
                   self.cubeService.aggregateCubeCustom(binnedCubeName, aggs)

    #
    # Update cubeset display name
    #
    def updateCubeSetDisplayName(self, cubeSetName, displayName):
        self.__updateCubeSetProperty__(cubeSetName, { "$set": {"displayName" : displayName}})


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
    def getSourceCubeRows(self, cubeSetName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        return self.cubeService.getCubeRows(existing['sourceCube'])

    # 
    # Get binned cube rows. Iterator to cube rows is returned.
    # 
    def getBinnedCubeRows(self, cubeSetName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        if existing['binnedCube'] != None:
            return self.cubeService.getCubeRows(existing['binnedCube'])
        else:
            return None

    # 
    # Get aggregated cube rows. Iterator to cube rows is returned.
    # 
    def getAggregatedCubeRows(self, cubeSetName, aggName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        if 'aggCubes' in existing:
            for aggCube in existing['aggCubes']:
               if existing['binnedCube'] + "_" + aggName == aggCube:
                   return self.cubeService.getCubeRows(aggCube)

        return None

    #
    # Export source cube to csv.
    #
    def exportSourceCubeToCsv(self, cubeSetName, csvFilePath):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        self.cubeService.exportCubeToCsv(existing['sourceCube'], csvFilePath)

    #
    # Export binned cube to csv.
    #
    def exportBinnedCubeToCsv(self, cubeSetName, csvFilePath):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        self.cubeService.exportCubeToCsv(existing['binnedCube'], csvFilePath)
        
    #
    # Export agg cube to csv.
    #
    def exportAggCubeToCsv(self, cubeSetName, csvFilePath, aggName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        if 'aggCubes' in existing:
            for aggCube in existing['aggCubes']:
               if existing['binnedCube'] + "_" + aggName == aggCube:
                   self.cubeService.exportCubeToCsv(aggCube, csvFilePath)

    #
    # Perform binning on source cube
    #
    def performBinning(self, cubeSetName, binnings):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        # Are we rebinning?
        if existing['binnedCube'] != None:
            if binnings != None:
                self.cubeService.rebinCubeCustom(binnings, existing['sourceCube'], existing['binnedCube'])
            else:
                self.cubeService.autoRebinCube(existing['sourceCube'], existing['binnedCube'])
        else: 
            binnedCubeName = cubeSetName + "_binned"
            if binnings != None:
                self.cubeService.binCubeCustom(binnings, binnedCubeName, binnedCubeName)
            else:
                self.cubeService.binCube(binnedCubeName, binnedCubeName, [])
            self.__updateCubeSetProperty__(cubeSetName, { "$set": {"binnedCube" : binnedCubeName}})

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
    def performAggregation(self, cubeSetName, dimensions):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        binnedCubeName = existing['binnedCube']
        groupByDimensionsList = self.__getGroupByDimensionsList__(dimensions)
        aggCubes = self.cubeService.aggregateCubeComplex(binnedCubeName, groupByDimensionsList)

        aggCubeNames = []
        for aggCube in aggCubes:
            aggCubeNames.append(aggCube['name'])

        self.__updateCubeSetProperty__(cubeSetName, { "$set": {"aggCubes" : aggCubeNames}})

        return aggCubes
       
    #
    # Perform one or more aggregations on binned cube using custom aggs.
    # The aggregated cubes are automatically saved and identified by aggName
    #
    def performAggregationCustom(self, cubeSetName, aggs):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        binnedCubeName = existing['binnedCube']
        aggCubes = self.cubeService.aggregateCubeCustom(binnedCubeName, aggs)

        aggCubeNames = []
        for agg in aggs:
              aggCubeNames.append(binnedCubeName + "_" + agg['name'])
 
        self.__updateCubeSetProperty__(cubeSetName, { "$set": {"aggCubes" : aggCubeNames}})
        
        return aggCubes
