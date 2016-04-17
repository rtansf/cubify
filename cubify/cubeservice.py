import csv
import re
import json
import sys
import math
import numpy as np
import pymongo
from copy import deepcopy
from pymongo import MongoClient
from datetime import datetime
from time import strptime
from timestring import Date
from simpleeval import simple_eval

class CubeService:
    def __init__(self, dbName="cubify"):
        self.dbName = dbName
        client = MongoClient()
        self.db = client[dbName]
        self.inMemoryCubes = {}
        self.inMemoryCubeRows = {}

    def __is_number__(self, s):
        try:
            float(s)  # for int, long and float
        except ValueError:
            return False
        return True

    def __cleanStringValue__(self, s):
        s = s.replace(".", "")             
        return s

    def __getDateFormat__(self, s):

        formats = ['%Y-%m-%d %H:%M:%S',
                   '%Y-%m-%dT%H:%M:%S',
                   '%Y-%m-%d',
                   '%y-%m-%d',
                   '%Y/%m/%d',
                   '%y/%m/%d'
                   ]

        for format in formats:
            try:
               d = strptime(s, format )
               return format
            except ValueError:
               pass # Try next format

        return None

    def __is_date__(self, s):
        if self.__getDateFormat__(s) != None:
            return True
        else:
            return False

    def __cleanFieldName(self, s):
        if s.startswith('S:') or s.startswith('N:') or s.startswith('D:'):
            return s[2:]
        else:
            return s

    def __addToDistincts__(self, distincts, fieldName, value):
        if fieldName not in distincts:
            field = {}
            field[value] = 1
            distincts[fieldName] = field
        else:
            field = distincts[fieldName]
            if value not in field:
                field[value] = 1
            else:
                field[value] += 1

    def __getFields__(self, csvFilePath):
        result = {}
        result['rawFieldNames'] = []
        result['fieldNames'] = []
        fieldTypes = {}
        dateFormats = {}

        with open(csvFilePath) as csvfile:
            reader = csv.DictReader(csvfile)
            rawFieldNames = reader.fieldnames
            for rawFieldName in rawFieldNames:
               result['rawFieldNames'].append(rawFieldName)
               cleanedFieldName = self.__cleanFieldName(rawFieldName)
               result['fieldNames'].append(cleanedFieldName)
               if rawFieldName.startswith('S:'):
                  fieldTypes[cleanedFieldName] = 'string'
               elif rawFieldName.startswith('N:'):
                  fieldTypes[cleanedFieldName] = 'number'
               elif rawFieldName.startswith('D:'):
                  fieldTypes[cleanedFieldName] = 'date'
               else:
                  fieldTypes[cleanedFieldName] = 'unknown'

            for row in reader:
                for rawFieldName in rawFieldNames:
                    cleanedFieldName = self.__cleanFieldName(rawFieldName)
                    value = row[rawFieldName]
                    if fieldTypes[cleanedFieldName] == 'unknown':
                        if self.__is_number__(value):
                            fieldTypes[cleanedFieldName] = 'number'
                        elif self.__is_date__(value):
                            fieldTypes[cleanedFieldName] = 'date'
                            dateFormats[cleanedFieldName] = self.__getDateFormat__(value)
                        else: 
                            fieldTypes[cleanedFieldName] = 'string'
                break

        result['fieldTypes'] = fieldTypes
        result['dateFormats'] = dateFormats
        return result

    #
    #  Get number of rows in cube
    #
    def __getCubeRowCount__(self, cubeName):
        cubeRows = self.getCubeRowsForCube(cubeName)

        if isinstance(cubeRows, list):
            return len(cubeRows)
        else:
            return cubeRows.count()

    #
    #  Create cube rows from csv
    #
    def createCubeRowsFromCsv(self, csvFilePath):

        cubeRows = []
        distincts = {}

        fields = self.__getFields__(csvFilePath)
        fieldTypes = fields['fieldTypes']
        fieldNames = fields['fieldNames']
        rawFieldNames = fields['rawFieldNames']

        numFields = len(fieldNames)

        with open(csvFilePath) as csvfile:
            reader = csv.DictReader(csvfile)
            num = 1
            for row in reader:
                if len(row) != numFields:
                    print "Number of fields in row are incorrect. Skipping: ", row
                    continue

                cubeRow = {"id": num, "dimensionKey": "", "dimensions": {}, "measures": {}, "dates": {}}
                num += 1

                for rawFieldName, fieldName in map(None, rawFieldNames, fieldNames):
                    value = row[rawFieldName]
                    fieldType = fieldTypes[fieldName]

                    # Check for null or empty value and handle appropriately
                    if value == None or value == '':
                        if fieldType == 'string':
                            value = ''
                        elif fieldType == 'number':
                            value = 0
                        elif fieldType == 'date':
                            value = '1970-01-01'
                        else:
                            value = ''

                    if fieldType == 'number':
                        if self.__is_number__(value):
                            cubeRow['measures'][fieldName] = float(value)
                        else: 
                            cubeRow['measures'][fieldName] = 0.0

                    elif fieldType == 'date':
                        # Treat date value as dimension
                        try:
                           d = Date(value)
                        except ValueError:
                           print "Invalid date: " + value + " Replaced with 1990-01-01"
                           d = Date('1990-01-01')
                        date = datetime(d.year, d.month, d.day)
                        cubeRow['dates'][fieldName] = date

                        # Process distincts
                        self.__addToDistincts__(distincts, fieldName, value)

                    else:  # This is a string value
                        # Treat value as dimension
                        # Clean the value
                        value = self.__cleanStringValue__(value)
                        cubeRow['dimensions'][fieldName] = value

                        # Process distincts
                        self.__addToDistincts__(distincts, fieldName, value)

                dimensionKey = ''
                for dimension in sorted(cubeRow['dimensions']):
                    dimensionKey += '#' + dimension + ":" + cubeRow['dimensions'][dimension]
                for dateName in sorted(cubeRow['dates']):
                    dimensionKey += '#' + dateName + ":" + str(cubeRow['dates'][dateName])[:10]
                cubeRow['dimensionKey'] = dimensionKey

                # Add this row to the list
                cubeRows.append(cubeRow)

            stats = self.getStats(cubeRows)
           
        return {'cubeRows': cubeRows, 'distincts': distincts, 'stats': stats}

    #
    # Create a cube from csv file. Returns the new cube
    #
    def createCubeFromCsv(self, csvFilePath, cubeName, inMemory=False):
        result = self.createCubeRowsFromCsv(csvFilePath)
        self.createCube('source', cubeName, result['cubeRows'], result['distincts'], result['stats'], None, None, inMemory)
        return self.getCube(cubeName)

    #
    # Create a cube by applying a filter on another cube
    #
    def createCubeFromCube(self, fromCube, filter, toCubeName):

        if fromCube == None:
            return None

        fromCubeName = fromCube['name']
        if fromCubeName in self.inMemoryCubes:
            raise ValueError("Cannot create cube from an in-memory cube. This feature is not yet supoorted.")
        cubeRows = self.queryCubeRows(fromCube, filter)
        if self.__getCubeRowCount__(fromCubeName) == 0:
            raise ValueError("Cube not created because number of rows returned by filter = 0")
            return None

        # TODO - see about not bringing all  cubeRows into memory for insertion
        rows = []
        distincts = {}

        # Compute the distincts
        for cubeRow in cubeRows:
            rows.append(cubeRow)
            for dimension in cubeRow['dimensions']:
                value = cubeRow['dimensions'][dimension]
                self.__addToDistincts__(distincts, dimension, value)


        # Compute stats
        stats = self.getStats(rows)

        # Create the cube
        self.createCube('source', toCubeName, rows, distincts, stats, None, None)

        return self.getCube(toCubeName)


    #
    # Append to cube from csv file
    #
    # Returns cube
    #
    def appendToCubeFromCsv(self, csvFilePath, cube):
        if cube == None:
            return
        cubeName = cube['name']

        inMemory = cubeName in self.inMemoryCubes

        result = self.createCubeRowsFromCsv(csvFilePath)

        # Adjust ids # TODO use max(id) from cube instead of numCurrentCubeRows
        numCurrentCubeRows = self.__getCubeRowCount__(cubeName)

        cubeRows = result['cubeRows']
        id = numCurrentCubeRows + 1
        for cubeRow in cubeRows:
            cubeRow['id'] = id
            id += 1

        # Save the cube rows
        if not inMemory:
            self.db[cubeName].insert_many(cubeRows)

        # Merge the distincts
        existingDistincts = cube['distincts']
        dcs = result['distincts']
        for dc in dcs:
            if dc not in existingDistincts:
                existingDistincts.append(dc)
        cube['distincts'] = existingDistincts
        if not inMemory:
            self.__updateCubeProperty__(cubeName, { "$set": {"distincts" : existingDistincts}})

        # Redo the stats
        allCubeRows = self.getCubeRowsForCube(cubeName)
        stats = self.getStats(allCubeRows)
        cube['stats'] = stats

        if not inMemory:
            self.__updateCubeProperty__(cubeName, { "$set": {"stats" : stats}})

        return cube

    #
    # Delete cube rows from a cube
    #
    def deleteCubeRows(self, cubeName, filter):
        cube = self.getCube(cubeName)
        if (cube == None):
            raise ValueError("Cube does not exist:" + cubeName)

        self.db[cubeName].remove(filter)

        allCubeRows = self.getCubeRowsForCube(cubeName)

        # Redo distincts
        distincts =  {}
        for row in allCubeRows:
            for k, v in row['dates'].items():
                self.__addToDistincts__(distincts, k, str(v))
            for k, v in row['dimensions'].items():
                self.__addToDistincts__(distincts, k, v)

        cube['distincts'] = distincts
        self.db['cube'].update_one({ "name" : cubeName}, { '$set' : { "distincts" : distincts}})

        # Redo stats
        stats = self.getStats(allCubeRows)
        self.__updateCubeProperty__(cubeName, { "$set": {"stats" : stats}})

    #
    # Create cube
    #
    def createCube(self, type, cubeName, cubeRows, distincts, stats, binnings, agg, inMemory=False):

        cube = {}
        cube['type'] = type
        cube['name'] = cubeName
        cube['distincts'] = distincts
        cube['stats'] = stats
        cube['binnings'] = binnings
        cube['agg'] = agg
        cube['createdOn'] = datetime.utcnow()

        # TODO make sure cubeName is unique
        #cube = self.getCube(cubeName)
        #if cube != None:
        #    raise RuntimeError('createCube failed. Cube already exists: ' + cubeName)

        if inMemory:
            self.inMemoryCubes[cubeName] = cube 
            self.inMemoryCubeRows[cubeName] = cubeRows
        else:   
            self.db['cube'].insert_one(cube)
            # Save the cube rows
            self.db[cubeName].insert_many(cubeRows)

    #
    # Delete cube
    #
    def deleteCube(self, cubeName):
        if cubeName in self.inMemoryCubes:
            del(self.inMemoryCubes[cubeName])
            del(self.inMemoryCubeRows[cubeName])
        else:
            self.db[cubeName].drop()
            self.db['cube'].remove({ "name": cubeName })


    #
    # Update an arbitrary field in cube
    #
    def __updateCubeProperty__(self, cubeName, update):
        # Handle inmemory cubes
        if cubeName not in self.inMemoryCubes:
            self.db['cube'].update_one({ "name" : cubeName}, update)

    #
    # Update an arbitrary field in a cube row
    #
    def __updateCubeRowProperty__(self, cubeName, _id, update):
        # Handle inmemory cubes
        if cubeName not in self.inMemoryCubes:
            self.db[cubeName].update_one({ "_id" : _id}, update)

    #
    #  Get cube
    #
    def getCube(self, cubeName):
        if cubeName in self.inMemoryCubes:
            return self.inMemoryCubes[cubeName]
        else:
            return self.db['cube'].find_one({ "name": cubeName })

    #
    #  Query cube rows
    #
    def queryCubeRows(self, cube, filter):
        if cube == None:
            return []
        cubeName = cube['name']
        return self.db[cubeName].find(filter)

    #
    #  Get all cube rows for a cubeName
    #
    def getCubeRowsForCube(self, cubeName):
        if cubeName in self.inMemoryCubeRows:
            return self.inMemoryCubeRows[cubeName]
        else:
            cube = self.getCube(cubeName)
            return self.queryCubeRows(cube, {}).sort("dimensionKey", pymongo.ASCENDING)

    #
    #  Get all cube rows for a cube
    #
    def getCubeRows(self, cube):
        if cube == None:
            return []
        cubeName = cube['name']
        return self.getCubeRowsForCube(cubeName)

    #
    # Compute stats on cube
    #
    def getStats(self, cubeRows):

        stats = {}
        measureValues = {}
        for row in cubeRows:
            for k, v in row['measures'].items():
                # print k,' -> ', v
                if k not in stats:
                    stats[k] = {"total": 0, "mean": 0, "median": 0, "std": 0, "min": 0, "max": 0}
                if k not in measureValues:
                    measureValues[k] = []
                measureValues[k].append(v)

                # measure = stats[k]
                # measure["total"] += v

        for k, v in measureValues.items():
            varray = np.array(v)
            stats[k]['total'] = np.sum(varray)
            stats[k]['mean'] = np.mean(varray)
            stats[k]['median'] = np.median(varray)
            stats[k]['std'] = np.std(varray)
            stats[k]['min'] = np.amin(varray)
            stats[k]['max'] = np.amax(varray)

        return stats

    def __getNumericBinLabel__(self, v, nb):
        bins = nb['bins']
        for bin in bins:
            if bin['min'] <= v and v <= bin['max']:
                return bin['label']
        return nb['fallbackLabel']

    def __getStringBinLabel__(self, v, sb):
        bins = sb['bins']
        for bin in bins:
            if bin['value'] == v:
                return bin['label']
        return sb['fallbackLabel']

    # TODO Room for optimization here
    def __getDateBinLabel__(self, v, db):
        d = Date(v)
        if 'period' in db:
           period = db['period']
           zeroFill = ''
           if period == 'weekly':
               dt = datetime(d.year, d.month, d.day)
               isoCalendar = dt.isocalendar()
               yearNum = isoCalendar[0]
               weekNum = isoCalendar[1]
               if weekNum < 10:
                   zeroFill = '0'
               return str(yearNum) + zeroFill + str(weekNum)
           elif period == 'monthly':
               if d.month < 10:
                   zeroFill = '0'
               return str(d.year) + zeroFill + str(d.month)
           elif period == 'yearly':
               return str(d.year)
           else:
               return "Unknown"
        else:
           bins = db['bins']
           for bin in bins:
               minD = Date(bin['min'])
               maxD = Date(bin['max'])
               if d.year >= minD.year and d.month >= minD.month and d.day >= minD.day and d.year <= maxD.year and d.month <= maxD.month and d.day <= maxD.day:
                 return bin['label']
           return db['fallbackLabel']

    #
    # Perform binning
    #  
    def __performBinning__(self, binnings, cubeRow, distincts):

        binnedCubeRow = deepcopy(cubeRow)

        for binning in binnings:

            sourceField = binning['sourceField']
            outputField = binning['outputField']['name']

            if binning['type'] == 'range':
                if sourceField not in cubeRow['measures']:
                    continue
                value = cubeRow['measures'][sourceField]
                label = self.__getNumericBinLabel__(value, binning)
                binnedCubeRow['dimensions'][outputField] = label

            elif binning['type'] == 'enum':
                if sourceField not in cubeRow['dimensions']:
                    continue
                value = cubeRow['dimensions'][sourceField]
                label = self.__getStringBinLabel__(value, binning)
                binnedCubeRow['dimensions'][outputField] = label

            elif binning['type'] == 'date':
                if sourceField not in cubeRow['dates']:
                    continue
                value = cubeRow['dates'][sourceField]
                label = self.__getDateBinLabel__(value, binning)
                binnedCubeRow['dimensions'][outputField] = label

        dimensionKey = ''
        for dimension in sorted(binnedCubeRow['dimensions']):
            self.__addToDistincts__(distincts, dimension, binnedCubeRow['dimensions'][dimension])
            dimensionKey += '#' + dimension + ":" + binnedCubeRow['dimensions'][dimension]
        for dateName in sorted(binnedCubeRow['dates']):
            dimensionKey += '#' + dateName + ":" + str(binnedCubeRow['dates'][dateName])[:10]
        binnedCubeRow['dimensionKey'] = dimensionKey

        return binnedCubeRow

    #
    # Determines if measure is date
    #
    def __isMeasureDate__(self, cubeName, measure):
       # Get a cube row
       cubeRows = self.getCubeRowsForCube(cubeName)
       cubeRow = cubeRows[0];
       for dateCol in cubeRow['dates']:
           if dateCol == measure:
               return True
 
       return False

    #
    # Generate the binning definitions for measures to be binned
    #
    def __generateBinnings__(self, sourceCubeName, measuresToBeBinned, hints):
        binnings = []
        cube = self.getCube(sourceCubeName)
        stats = cube['stats']
        for measure in measuresToBeBinned:
            if self.__isMeasureDate__(sourceCubeName, measure):
               binning = {}
               binnings.append(binning)
               binning['binningName'] = measure + "Binning"
               binning['outputField'] = {}
               binning['outputField']['name'] = measure + "Bin"
               binning['outputField']['displayName'] = measure + " Bin"
               binning['sourceField'] = measure
               binning['type'] = 'date'
               binning['fallbackLabel'] = 'OutsideRange'
               if measure in hints:
                   period = hints[measure]
                   if period == 'weekly' or period == 'monhtly' or period == 'yearly':
                       binning['period'] = period
                   else:
                       binning['period'] = 'monthly' #default
               else:
                   binning['period'] = 'monthly' #default

            else:
               minV = stats[measure]['min']
               minV = math.floor(minV)
               iMinv = int(minV)

               maxV = stats[measure]['max']
               maxV = math.ceil(maxV)
               iMaxv = int(maxV)

               size =  self.__getCubeRowCount__(sourceCubeName)

               # Rice formula
               binSize = 2 * math.pow(size, 0.33)
               binWidth = (iMaxv - iMinv) / binSize
               iBinSize = int(round(binSize))
               iBinWidth = int(round(binWidth))

               binning = {}
               binnings.append(binning)
               binning['binningName'] = measure + "Binning"
               binning['outputField'] = {}
               binning['outputField']['name'] = measure + "Bin"
               binning['outputField']['displayName'] = measure + " Bin"
               binning['sourceField'] = measure
               binning['type'] = 'range'
               binning['bins'] = []
               binning['fallbackLabel'] = 'OutsideRange'

               lo = iMinv
               for i in range(0, iBinSize):
                   hi = lo + iBinWidth
                   if i == iBinSize-1 and hi < iMaxv:
                      hi = iMaxv
                   bin = { "min" : lo, "max" : hi, "label" : str(lo) + "-" + str(hi) }
                   binning['bins'].append(bin) 
                   lo += iBinWidth

        return binnings

    #
    #  Get all measures for binning (a measure is a numeric or date type)
    #
    def __getAllMeasuresForBinning__(self, cubeName):
        measuresToBeBinned = []
        cubeRows = self.getCubeRowsForCube(cubeName)
        cubeRow = cubeRows[0];
        for measure in cubeRow['measures']:
            measuresToBeBinned.append(measure)
        for date in cubeRow['dates']:
            measuresToBeBinned.append(date)
        return measuresToBeBinned

    #
    # Automatically bin a cube
    #
    def binCube(self, sourceCube, binnedCubeName, measuresToBeBinned=None, hints={}):
        if sourceCube == None:
            return

        sourceCubeName = sourceCube['name']
        if measuresToBeBinned == None:
            measuresToBeBinned = self.__getAllMeasuresForBinning__(sourceCubeName)

        binnings = self.__generateBinnings__(sourceCubeName, measuresToBeBinned, hints)
        return self.binCubeCustom(binnings, sourceCube, binnedCubeName)

    #
    # Automatically re-bin a cube
    #
    def rebinCube(self, sourceCube, binnedCubeName):

        if sourceCube == None:
            return None

        sourceCubeName = sourceCube['name']
        measuresToBeBinned = []

        # Get existing binning definition, if any - and get the measures to be binned from this
        cube = self.getCube(sourceCubeName)
        if 'binnings' in cube and cube['binnings'] != None:
            binnings = cube['binnings']
            for binning in binnings:
                if binning['type'] == 'range' or binning['type'] == 'date':
                    measuresToBeBinned.append(binning['sourceField'])
        else:
            # If no existing binning definition, generate the binnings automatically for all measures
            measuresToBeBinned = self.__getAllMeasuresForBinning__(sourceCubeName)

        binnings = self.__generateBinnings__(sourceCubeName, measuresToBeBinned, {})
        return self.rebinCubeCustom(binnings, cube, binnedCubeName)

    #
    # Bin cube with custom binnings  - Returns the binned cube
    #
    def binCubeCustom(self, binnings, sourceCube, binnedCubeName):
        if sourceCube == None:
            return None

        sourceCubeName = sourceCube['name']
        binnedCubeRows = []
        cubeRows = self.getCubeRowsForCube(sourceCubeName)
        distincts = {}
        for cubeRow in cubeRows:
            #print cubeRow
            binnedCubeRow = self.__performBinning__(binnings, cubeRow, distincts)
            binnedCubeRows.append(binnedCubeRow)

        stats = self.getStats(binnedCubeRows)
        inMemory = False
        if sourceCubeName in self.inMemoryCubes:
            inMemory = True
        self.createCube('binned', binnedCubeName, binnedCubeRows, distincts, stats, binnings, None, inMemory)

        if not inMemory:
            self.__updateCubeProperty__(binnedCubeName, { "$set": {"lastBinnedOn" : datetime.utcnow()}})

        return self.getCube(binnedCubeName)

    #
    # Re-bin cube using custom binnings
    #
    def rebinCubeCustom(self, binnings, sourceCube, binnedCubeName):
        if sourceCube == None:
            return None

        sourceCubeName = sourceCube['name']
        binnedCubeRows = []
        cubeRows = self.getCubeRowsForCube(sourceCubeName)
        distincts = {}
        for cubeRow in cubeRows:
            #print cubeRow
            binnedCubeRow = self.__performBinning__(binnings, cubeRow, distincts)
            binnedCubeRows.append(binnedCubeRow)
        stats = self.getStats(binnedCubeRows)

        # Drop old cube row collection
        self.db[binnedCubeName].drop()

        # Recreate the new cube row collection
        self.db[binnedCubeName].insert_many(binnedCubeRows)

        # Update the binned cube
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"binningsUpdatedOn" : datetime.utcnow()}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"lastBinnedOn" : datetime.utcnow()}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"binnings" : binnings}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"stats" : stats}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"distincts" : distincts}})

        return self.getCube(binnedCubeName)

    #
    # Get agg name
    #
    def __getAggName__(self, dimensions):
        aggName = ''
        for dimension in dimensions:
            if aggName == '':
                aggName = dimension
            else:
                aggName = aggName + '-' + dimension
        return aggName

    #
    # Generate aggregation definitions
    #
    def __generateAggsOld__(self, cubeName, groupByDimensionsList, measures):
        aggs = []
        for groupByDimensions in groupByDimensionsList:
            agg = {}
            aggName = self.__getAggName__(groupByDimensions)
            agg['name'] = aggName
            agg['dimensions'] = groupByDimensions
            measuresList = []
            for measure in measures:
                m = {}
                m['outputField'] = { 'name' : "Average_" + measure,  'displayName' : 'Average ' + measure }
                m['formula'] = { 'numerator' : '{"$avg": "$measures.' + measure + '"}', 'denominator' : ''}
                measuresList.append(m)
                m = {}
                m['outputField'] = { 'name' : "Total_" + measure,  'displayName' : 'Total ' + measure }
                m['formula'] = { 'numerator' : '{"$sum": "$measures.' + measure + '"}', 'denominator' : ''}
                measuresList.append(m)
            agg['measures'] = measuresList
            aggs.append(agg)
        return aggs

    #
    # Generate aggregation definitions
    #
    def __generateAggs__(self, cubeName, groupByDimensionsList, measures):
        aggs = []
        for groupByDimensions in groupByDimensionsList:
            agg = {}
            aggName = self.__getAggName__(groupByDimensions)
            agg['name'] = aggName
            agg['dimensions'] = groupByDimensions
            measuresList = []
            for measure in measures:
                m = {}
                m['outputField'] = { 'name' : "Average_" + measure,  'displayName' : 'Average ' + measure }
                m['formula'] = { 'numerator' : { 'aggOperator': 'avg', 'expression': measure }, 'denominator' : {}}
                measuresList.append(m)
                m = {}
                m['outputField'] = { 'name' : "Total_" + measure,  'displayName' : 'Total ' + measure }
                m['formula'] = { 'numerator' : { 'aggOperator': 'sum', 'expression': measure }, 'denominator' : {}}
                measuresList.append(m)
            agg['measures'] = measuresList
            aggs.append(agg)
        return aggs

    #
    #  Aggregate a cube on a single group-by dimensions list
    #
    def aggregateCubeOld(self, cubeName, groupByDimensions, measures=None):
        aggCubes = self.aggregateCubeComplexOld(cubeName, [groupByDimensions], measures)
        return aggCubes[0]

    #
    # Aggregate cube with multiple group-by dimensions list
    #
    def aggregateCubeComplexOld(self, cubeName, groupByDimensionsList, measures=None):
        if measures == None:
            allMeasures = []
            cubeRows = self.getCubeRowsForCube(cubeName)
            cubeRow = cubeRows[0];
            for measure in cubeRow['measures']:
                allMeasures.append(measure)
            aggs = self.__generateAggsOld__(cubeName, groupByDimensionsList, allMeasures)
        else:
            aggs = self.__generateAggsOld__(cubeName, groupByDimensionsList, measures)

        return self.aggregateCubeCustomOld(cubeName, aggs)

    #
    #  Aggregate a cube on a single group-by dimensions list
    #
    def aggregateCube(self, cube, groupByDimensions, measures=None):
        aggCubes = self.aggregateCubeComplex(cube, [groupByDimensions], measures)
        return aggCubes[0]

    #
    # Aggregate cube with multiple group-by dimensions list
    #
    def aggregateCubeComplex(self, cube, groupByDimensionsList, measures=None):
        if cube == None:
            return []
        cubeName = cube['name']
        if measures == None:
            allMeasures = []
            cubeRows = self.getCubeRowsForCube(cubeName)
            cubeRow = cubeRows[0];
            for measure in cubeRow['measures']:
                allMeasures.append(measure)
            aggs = self.__generateAggs__(cubeName, groupByDimensionsList, allMeasures)
        else:
            aggs = self.__generateAggs__(cubeName, groupByDimensionsList, measures)

        return self.aggregateCubeCustom(cube, aggs)

    #
    #  Is a group-by dimension a date?
    #
    def __isDateDimension__(self, cubeName, dimension):
        cubeRows = self.getCubeRowsForCube(cubeName)
        cubeRow = cubeRows[0]
        for date in cubeRow['dates']:
            if dimension == date:
                return True
        return False

    def __getExpressionValue__(self, dict, expression):
        expressionValue = simple_eval(expression, names=dict)
        return expressionValue

    def __flatten__(self, cubeRow):
        dict = {}
        for dim in cubeRow['dimensions']:
            dict[dim] = cubeRow['dimensions'][dim]
        for date in cubeRow['dates']:
            dict[date] = cubeRow['dates'][date]
        for measure in cubeRow['measures']:
            dict[measure] = cubeRow['measures'][measure]
        return dict

    #
    # Aggregate cube using custom aggregation definitions
    #
    def aggregateCubeCustom(self, cube, aggs):
        if cube == None:
            return []

        cubeName = cube['name']
        cubeRows = self.getCubeRowsForCube(cubeName)
        results = {}
        nameToAggMap = {}

        for cubeRow in cubeRows:
            for agg in aggs:
                aggName = agg['name']
                nameToAggMap[aggName] = agg
                if aggName not in results:
                    results[aggName] = {}

                result = results[aggName]

                groupByDims = agg['dimensions']
                aggDimKey = ''
                for groupByDim in groupByDims:
                    aggDimKey = aggDimKey + '#'
                    aggDimKey = aggDimKey + groupByDim
                    aggDimKey = aggDimKey + ':'
                    if groupByDim in cubeRow['dimensions']:
                        aggDimKey = aggDimKey + cubeRow['dimensions'][groupByDim]
                    elif groupByDim in cubeRow['dates']:
                        dateDimVal = cubeRow['dates'][groupByDim]
                        aggDimKey = aggDimKey + str(dateDimVal)[:10]
                    # TODO else throw exception

                if (aggDimKey not in result):
                    accums = {}
                    result[aggDimKey] = accums

                for measure in agg['measures']:
                    outputField = measure['outputField']
                    outputFieldName = outputField['name']
                    formula = measure['formula']

                    numerator = formula['numerator']
                    numAggOperator = numerator['aggOperator']
                    numExpression = numerator['expression']
                    numExpressionValue = self.__getExpressionValue__(self.__flatten__(cubeRow), numExpression)

                    denominator = formula['denominator']
                    if denominator:
                        denAggOperator = denominator['aggOperator']
                        denExpression = denominator['expression']
                        denExpressionValue = self.__getExpressionValue__(self.__flatten__(cubeRow), denExpression)

                    accums = result[aggDimKey]
                    if outputFieldName in accums:
                        accum = accums[outputFieldName]
                        accumNum = accum['numerator']
                        accumNum['count'] = accumNum['count'] + 1
                        accumNum['sum'] = accumNum['sum'] + numExpressionValue
                        accumNum['avg'] = accumNum['sum'] / accumNum['count']
                        if numExpressionValue < accumNum['min']:
                            accumNum['min'] = numExpressionValue
                        if numExpressionValue > accumNum['max']:
                            accumNum['max'] = numExpressionValue
               
                        if denominator:
                            accumDen = accum['denominator']
                            accumDen['count'] = accumDen['count'] + 1
                            accumDen['sum'] = accumDen['sum'] + denExpressionValue
                            accumDen['avg'] = accumDen['sum'] / accumDen['count']
                            if denExpressionValue < accumDen['min']:
                                accumDen['min'] = denExpressionValue
                            if denExpressionValue > accumDen['max']:
                                accumDen['max'] = denExpressionValue
                    else:
                        accum = {}
                        accums[outputFieldName] = accum        
                        accumNum = {}
                        accum['numerator'] = accumNum
                        accumNum['operator'] = numAggOperator
                        accumNum['count'] = 1
                        accumNum['sum'] = numExpressionValue
                        accumNum['avg'] = numExpressionValue
                        accumNum['min'] = numExpressionValue
                        accumNum['max'] = numExpressionValue
                        if denominator:
                            accumDen = {}
                            accum['denominator'] = accumDen
                            accumDen['operator'] = denAggOperator
                            accumDen['count'] = 1
                            accumDen['sum'] = denExpressionValue
                            accumDen['avg'] = denExpressionValue
                            accumDen['min'] = denExpressionValue
                            accumDen['max'] = denExpressionValue

        # Now process results
        resultCubes = []

        for aggName in results:
            result = results[aggName]
            agg = nameToAggMap[aggName]

            #print aggName

            aggCubeRows = []
            distincts = {}
            aggCubeRowId = 0

            for dimKey in result:
                #print dimKey

                aggCubeRowId += 1
                aggCubeRow = { 'dimensions': {}, 'dates': {}, 'measures': {}, "id": aggCubeRowId }
                aggCubeRows.append(aggCubeRow)

                dims = dimKey.split('#')
                for dim in dims:
                    if len(dim) > 0:
                        dimToks = dim.split(':')
                        dimName = dimToks[0]
                        dimName = dimName.encode('ascii', 'ignore')
                        dimValue = dimToks[1]
                        dimValue = dimValue.encode('ascii', 'ignore')
                        aggCubeRow['dimensions'][dimName] = dimValue

                outputFields = result[dimKey]
                for outputFieldName in outputFields:
                    outputFieldName = outputFieldName.encode('ascii', 'ignore')
                    #print outputFieldName

                    numOutput = outputFields[outputFieldName]['numerator']
                    numOperator = numOutput['operator']
                    numMeasureValue = 0
                    if numOperator == 'sum':
                        numMeasureValue = numOutput['sum']
                    elif numOperator == 'avg':
                        numMeasureValue = numOutput['avg']
                    elif numOperator == 'min':
                        numMeasureValue = numOutput['min']
                    elif numOperator == 'max':
                        numMeasureValue = numOutput['max']
                    finalMeasureValue = numMeasureValue

                    if 'denominator' in outputFields[outputFieldName]:
                        denOutput = outputFields[outputFieldName]['denominator']
                        #print "   ", "denominator", denOutput['operator']
                        denOperator = denOutput['operator']
                        if denOperator == 'sum':
                            denMeasureValue = denOutput['sum']
                        elif denOperator == 'avg':
                            denMeasureValue = denOutput['avg']
                        elif denOperator == 'min':
                            denMeasureValue = denOutput['min']
                        elif denOperator == 'max':
                            denMeasureValue = denOutput['max']
                        if denMeasureValue > 0:
                            finalMeasureValue = numMeasureValue / denMeasureValue

                    # Useful for debugging
                    #print "   ", "numerator", numOutput['operator']
                    #print "    Value", finalMeasureValue
                    #print "    Count", numOutput['count']

                    aggCubeRow['measures'][outputFieldName] = finalMeasureValue
                    aggCubeRow['measures']['Count'] = numOutput['count']

                    # Re-create dimensionkey - N.B. need to preserve the order of the agg dimensions
                    groupByDims = agg['dimensions']
                    dimensionKey = ''
                    for groupByDim in groupByDims:
                        dimensionKey += '#' + groupByDim + ':' + aggCubeRow['dimensions'][groupByDim]
                        self.__addToDistincts__(distincts, groupByDim, aggCubeRow['dimensions'][groupByDim])

                    aggCubeRow['dimensionKey'] = dimensionKey

            # Create the aggregated cube    
            aggCubeName = cubeName + "_" + aggName
            existingAggCube = self.getCube(aggCubeName)
            stats = self.getStats(aggCubeRows)

            # Does agg cube already exist? If so delete it and recreate the agg cube
            if existingAggCube != None:
                self.deleteCube(aggCubeName)

            inMemory = False
            if cubeName in self.inMemoryCubes:
                inMemory = True
            self.createCube('agg', aggCubeName, aggCubeRows, distincts, stats, None, agg, inMemory)
            resultCubes.append(self.getCube(aggCubeName))
    
        return resultCubes        

    #
    # Aggregate cube using custom aggregation definitions
    #
    def aggregateCubeCustomOld(self, cubeName, aggs):
        resultCubes = []
        for agg in aggs:
            aggName = agg['name']
            aggCubeRows = []
            distincts = {}
            pipeline = []
            groupPipelineItem = {}
            pipeline.append(groupPipelineItem)
            idContent = {}
            group = {}
            groupPipelineItem['$group'] = group
            group['_id'] = idContent
            group['count'] = { '$sum' : 1 }
            for dimension in agg['dimensions']:
                if self.__isDateDimension__(cubeName, dimension):
                    idContent[dimension] = '$dates.' + dimension
                else:
                    idContent[dimension] = '$dimensions.' + dimension

            hasProjection = False
            project = {}

            for measure in agg['measures']:
                measureName = measure['outputField']['name']

                # Handle the case where there is a denominator
                if measure['formula']['denominator'] != "":
                    formulaNumerator = measure['formula']['numerator']
                    formulaNumerator = formulaNumerator.replace("'", '"')
                    formulaDenominator = measure['formula']['denominator']
                    formulaDenominator = formulaDenominator.replace("'", '"')
                    group['numerator' + measureName] = json.loads(formulaNumerator)
                    group['denominator' + measureName] = json.loads(formulaDenominator)
                    result = {}
                    project[measureName] = result
                    projectPipelineItem = {}
                    pipeline.append(projectPipelineItem)
                    projectPipelineItem['$project'] = project
                    result['$divide'] = ["$numerator"+measureName, "$denominator"+measureName]
                    hasProjection = True
                else:
                    # Put numerator in group
                    formulaNumerator = measure['formula']['numerator']
                    formulaNumerator = formulaNumerator.replace("'", '"')
                    group[measureName] = json.loads(formulaNumerator)

            # Need to include other grouped items in the projection -- TODO needs reworking.
            if hasProjection :
                for gname in group:
                    if gname != '_id' and gname.startswith('numerator') == False and gname.startswith('denominator') == False:
                         project[gname] = '$'+gname

            aggResult = self.db[cubeName].aggregate(pipeline)
            aggResultList = list(aggResult)

            num = 1
            for aggResult in aggResultList:
                cubeRow = { "id": num, "dimensionKey": "", "dimensions": {}, "measures": {}, "dates": {} }
                num += 1
                for k, v in aggResult.items():
                   if k == '_id':
                      for dimName, dimValue in v.items():
                          if self.__is_date__(str(dimValue)):
                             cubeRow['dates'][dimName] = str(dimValue)
                          else:
                             cubeRow['dimensions'][dimName] = dimValue
                          self.__addToDistincts__(distincts, dimName, str(dimValue))
                   elif k == 'count':
                       cubeRow['measures']['Count'] = v
                   else:
                       cubeRow['measures'][k] = v
                   dimensionKey = ''
                   for dim in sorted(cubeRow['dimensions']):
                       dimensionKey += '#' + dim + ':' + cubeRow['dimensions'][dim]
                   for datedim in sorted(cubeRow['dates']):
                       dimensionKey += '#' + datedim + ':' + cubeRow['dates'][datedim]
                   cubeRow['dimensionKey'] = dimensionKey

                aggCubeRows.append(cubeRow)

            aggCubeName = cubeName + "_" + aggName
            existingAggCube = self.getCube(aggCubeName)
            stats = self.getStats(aggCubeRows)

            # Does agg cube already exist? If so delete it and recreate the agg cube
            if existingAggCube != None:
                self.deleteCube(aggCubeName)

            inMemory = False
            if cubeName in self.inMemoryCubes:
                inMemory = True
            self.createCube('agg', aggCubeName, aggCubeRows, distincts, stats, None, agg, inMemory)
            resultCubes.append(self.getCube(aggCubeName))

        return resultCubes

    def addColumn(self, cube, newColumnName, type, expression=None, func=None):
        if type != 'numeric' and type != 'string':
            raise ValueError("type must be one of numeric, string")

        if expression == None and func == None:
            raise ValueError("You must supply an expression or function")

        if (expression != None):
            if type  == 'numeric':
               expression = expression.replace('$', "cubeRow['measures']")
            elif type == 'string':
               expression = expression.replace('$', "cubeRow['dimensions']")

        if (cube == None):
            return

        cubeName = cube['name']
        cubeRows = self.getCubeRowsForCube(cubeName)
        tempMap = {}

        for cubeRow in cubeRows:
            try:
                if expression != None:
                    value = eval(expression)
                else:
                    value = func(cubeRow)
                cubeId = cubeRow['id']
                tempMap[cubeId] = value
            except SyntaxError:
                raise SyntaxError("Expression contains syntax error.")

        cubeRows = self.getCubeRowsForCube(cubeName)
        for cubeRow in cubeRows:
            cubeId = cubeRow['id']
            value = tempMap[cubeId]
            if (type == 'numeric'):
                cubeRow['measures'][newColumnName] = value
                measureUpdateField = 'measures.' + newColumnName
                self.__updateCubeRowProperty__(cubeName, cubeRow['_id'], { "$set": {measureUpdateField : value}})
            elif (type == 'string'):
                cubeRow['dimensions'][newColumnName] = value
                dimensionUpdateField = 'dimensions.' + newColumnName
                self.__updateCubeRowProperty__(cubeName, cubeRow['_id'], { "$set": {dimensionUpdateField : value}})
                # Reconstruct dimension key
                dimensionKey = self.__reconstructDimensionKey__(cubeRow['dimensions'], cubeRow['dates'])
                self.__updateCubeRowProperty__(cubeName, cubeRow['_id'], { "$set": {"dimensionKey": dimensionKey}})
                self.__addToDistincts__(cube['distincts'], newColumnName, value)

        if type == 'numeric':
            # Recompute stats
            cubeRows = self.getCubeRowsForCube(cubeName)
            stats = self.getStats(cubeRows)
            self.__updateCubeProperty__(cubeName, { "$set": {"stats" : stats}})
        elif type == 'string':
            self.__updateCubeProperty__(cubeName, { "$set": {"distincts" : cube['distincts']}})


    def __reconstructDimensionKey__(self, dimensions, dates):
        dimensionKey = ''
        for dimension in sorted(dimensions):
            dimensionKey += '#' + dimension + ":" + dimensions[dimension]
        for dateName in sorted(dates):
            dimensionKey += '#' + dateName + ":" + str(dates[dateName])[:10]
        return dimensionKey

    #
    # Export cube rows to csv
    #
    def exportCubeToCsv(self, cube, csvFilePath):

        if (cube == None):
            raise ValueError("Cube is null")

        cubeName = cube['name']

        csvfile = open(csvFilePath, 'w')
        cubeRows = self.getCubeRowsForCube(cubeName)
        fieldNames = []
        for cubeRow in cubeRows:
            if len(fieldNames) == 0:
                for dimName in sorted(cubeRow['dimensions']):
                   fieldNames.append('S:'+dimName)
                for dateName in sorted(cubeRow['dates']):
                   fieldNames.append('D:'+dateName)
                for measure in sorted(cubeRow['measures']):
                   fieldNames.append('N:'+measure)

                writer = csv.DictWriter(csvfile, fieldnames=fieldNames)
                writer.writeheader()
             
            row = {}
            for dimName in cubeRow['dimensions']:
                value = cubeRow['dimensions'][dimName]
                rawDimName = 'S:'+dimName
                row[rawDimName] = value
            for dateName in cubeRow['dates']:
                value = cubeRow['dates'][dateName]
                rawDateName = 'D:'+dateName
                row[rawDateName] = value
            for measure in cubeRow['measures']:
                value = cubeRow['measures'][measure]
                rawMeasureName = 'N:'+measure
                row[rawMeasureName] = value

            writer.writerow(row)


     
