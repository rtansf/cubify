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
    # Create a cube set including optionally performing binning and aggregation
    #
    def createCubeSet(self, owner, cubeSetName, cubeSetDisplayName, csvFileName, binnings, aggs):

        # Make sure cubeSetName is unique
        existing = self.getCubeSet(cubeSetName)
        if existing != None:
            raise ValueError('Cube Set with ' + cubeSetName + ' already exists')

        sourceCubeName = cubeSetName + "_source"
        self.cubeService.createCubeFromCsv(csvFileName, sourceCubeName, sourceCubeName)
        if binnings != None:
            binnedCubeName = cubeSetName + "_binned"
            self.cubeService.binCube(binnings, sourceCubeName, binnedCubeName, binnedCubeName)
            if aggs != None:
                self.cubeService.aggregateCube(binnedCubeName, aggs)
        
        # Now save the cubeSet
        cubeSet = {}
        cubeSet['name'] = cubeSetName
        cubeSet['displayName'] = cubeSetDisplayName
        cubeSet['owner'] = owner
        cubeSet['csvFileName'] = csvFileName
        cubeSet['createdOn'] = datetime.utcnow()
        cubeSet['sourceCube'] = sourceCubeName
        if binnings != None:
            cubeSet['binnedCube'] = binnedCubeName
            if aggs != None:
                aggCubeNames = []
                for agg in aggs:
                    aggCubeNames.append(binnedCubeName + "_" + agg['name'])
                cubeSet['aggCubes'] = aggCubeNames
    
        self.db['cubeset'].insert_one(cubeSet);        

    #
    # Add cells to source cube
    #
    def addCellsToSourceCube(self, cubeSetName, csvFileName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        self.cubeService.appendToCubeFromCsv(csvFileName, existing['sourceCube'])

        #re-bin
        if 'binnedCube' in existing:
            binnedCubeName = existing['binnedCube']
            binnedCube = self.cubeService.getCube(binnedCubeName)
            self.cubeService.rebinCube(binnedCube['binnings'], existing['sourceCube'], existing['binnedCube'])

            #re-aggregate
            if 'aggCubes' in existing:
                for aggCubeName in existing['aggCubes']:
                   aggCube = self.cubeService.getCube(aggCubeName)
                   aggs = []
                   aggs.append(aggCube['agg'])
                   self.cubeService.aggregateCube(binnedCubeName, aggs)

    #
    # Remove cells from source
    #
    def removeCellsFromSourceCube(self, cubeSetName, filter):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')
        self.cubeService.deleteCubeCells(existing['sourceCube'], filter)

        #re-bin
        if 'binnedCube' in existing:
            binnedCubeName = existing['binnedCube']
            binnedCube = self.cubeService.getCube(binnedCubeName)
            self.cubeService.rebinCube(binnedCube['binnings'], existing['sourceCube'], existing['binnedCube'])

            #re-aggregate
            if 'aggCubes' in existing:
                for aggCubeName in existing['aggCubes']:
                   aggCube = self.cubeService.getCube(aggCubeName)
                   aggs = []
                   aggs.append(aggCube['agg'])
                   self.cubeService.aggregateCube(binnedCubeName, aggs)

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
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

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
    # Get source cube cells. Iterator to cube cells is returned.
    # 
    def getSourceCubeCells(self, cubeSetName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        return self.cubeService.getCubeCells(existing['sourceCube'])

    # 
    # Get binned cube cells. Iterator to cube cells is returned.
    # 
    def getBinnedCubeCells(self, cubeSetName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        if existing['binnedCube'] != None:
            return self.cubeService.getCubeCells(existing['binnedCube'])
        else:
            return None

    # 
    # Get aggregated cube cells. Iterator to cube cells is returned.
    # 
    def getAggregatedCubeCells(self, cubeSetName, aggName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        if 'aggCubes' in existing:
            for aggCube in existing['aggCubes']:
               if existing['binnedCube'] + "_" + aggName == aggCube:
                   return self.cubeService.getCubeCells(aggCube)

        return None

    #
    # Export source cube to csv.
    #
    def exportSourceCubeToCsv(self, cubeSetName, csvFileName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        self.cubeService.exportCubeToCsv(existing['sourceCube'], csvFileName)

    #
    # Export binned cube to csv.
    #
    def exportBinnedCubeToCsv(self, cubeSetName, csvFileName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        self.cubeService.exportCubeToCsv(existing['binnedCube'], csvFileName)
        
    #
    # Export agg cube to csv.
    #
    def exportAggCubeToCsv(self, cubeSetName, csvFileName, aggName):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        if 'aggCubes' in existing:
            for aggCube in existing['aggCubes']:
               if existing['binnedCube'] + "_" + aggName == aggCube:
                   self.cubeService.exportCubeToCsv(aggCube, csvFileName)

    #
    # Perform binning on source cube
    #
    def performBinning(self, cubeSetName, binnings):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')
        
        # Are we rebinning?
        if existing['binnedCube'] != None:
            self.cubeService.rebinCube(binnings, existing['sourceCube'], existing['binnedCube'])
        else: 
            binnedCubeName = cubeSetName + "_binned"
            self.cubeService.binCube(binnings, binnedCubeName, binnedCubeName)
            self.__updateCubeSetProperty__(cubeSetName, { "$set": {"binnedCube" : binnedCubeName}})

    #
    # Perform one or more aggregations on binned cube. The aggregated cubes are automatically saved and identified by aggName
    #
    def performAggregation(self, cubeSetName, aggs):
        existing = self.getCubeSet(cubeSetName)
        if existing == None:
            raise ValueError('Cube Set with ' + cubeSetName + ' does not exist')

        binnedCubeName = existing['binnedCube']
        self.cubeService.aggregateCube(binnedCubeName, aggs)

        aggCubeNames = []
        for agg in aggs:
              aggCubeNames.append(binnedCubeName + "_" + agg['name'])
 
        self.__updateCubeSetProperty__(cubeSetName, { "$set": {"aggCubes" : aggCubeNames}})
        



