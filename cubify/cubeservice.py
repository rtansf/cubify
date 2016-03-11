import csv
import re
import json
import sys
import math
import numpy as np
from copy import deepcopy
from pymongo import MongoClient
from datetime import datetime
from time import strptime
from timestring import Date, TimestringInvalid

class CubeService:
    def __init__(self, dbName="cubify"):
        self.dbName = dbName
        client = MongoClient()
        self.db = client[dbName]

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
    def createCubeFromCsv(self, csvFilePath, cubeName, cubeDisplayName=None):
        if cubeDisplayName == None:
            cubeDisplayName = cubeName
        result = self.createCubeRowsFromCsv(csvFilePath)
        self.createCube('source', cubeName, cubeDisplayName, result['cubeRows'], result['distincts'], result['stats'], None, None)
        return self.getCube(cubeName)

    #
    # Create a cube by applying a filter on another cube
    #
    def createCubeFromCube(self, fromCubeName, filter, toCubeName, toCubeDisplayName=None):
        if toCubeDisplayName == None:
            toCubeDisplayName = toCubeName
        fromCube = self.getCube(fromCubeName)
        if fromCube == None:
            raise ValueError("Cube does not exist: " + fromCubeName)
        cubeRows = self.queryCubeRows(fromCubeName, filter)
        if cubeRows.count() == 0:
            raise ValueError("Cube not created because number of rows returned by filter = 0")
            return

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
        self.createCube('source',toCubeName, toCubeDisplayName, rows, distincts, stats, None, None)


    #
    # Append to cube from csv file
    #
    def appendToCubeFromCsv(self, csvFilePath, cubeName):
        cube = self.getCube(cubeName)
        if (cube == None):
            raise ValueError("Cube does not exist: " + cubeName)

        result = self.createCubeRowsFromCsv(csvFilePath)

        # Adjust ids # TODO use max(id) from cube instead of numCurrentCubeRows
        currentCubeRows = self.getCubeRows(cubeName)
        numCurrentCubeRows = currentCubeRows.count()
        cubeRows = result['cubeRows']
        id = numCurrentCubeRows + 1
        for cubeRow in cubeRows:
            cubeRow['id'] = id
            id += 1

        # Save the cube rows
        self.db[cubeName].insert_many(cubeRows)

        # Merge the distincts
        existingDistincts = cube['distincts']
        dcs = result['distincts']
        for dc in dcs:
            if dc not in existingDistincts:
                existingDistincts.append(dc)
        self.__updateCubeProperty__(cubeName, { "$set": {"distincts" : existingDistincts}})

        # Redo the stats
        allCubeRows = self.getCubeRows(cubeName)
        stats = self.getStats(allCubeRows)
        self.__updateCubeProperty__(cubeName, { "$set": {"stats" : stats}})

    #
    # Delete cube rows from a cube
    #
    def deleteCubeRows(self, cubeName, filter):
        cube = self.getCube(cubeName)
        if (cube == None):
            raise ValueError("Cube does not exist:" + cubeName)

        self.db[cubeName].remove(filter)

        allCubeRows = self.getCubeRows(cubeName)

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
    def createCube(self, type, cubeName, cubeDisplayName, cubeRows, distincts, stats, binnings, agg):

        cube = {}
        cube['type'] = type
        cube['name'] = cubeName
        cube['displayName'] = cubeDisplayName
        cube['distincts'] = distincts
        cube['stats'] = stats
        cube['binnings'] = binnings
        cube['agg'] = agg
        cube['createdOn'] = datetime.utcnow()

        # TODO make sure cubeName is unique
        #cube = self.getCube(cubeName)
        #if cube != None:
        #    raise RuntimeError('createCube failed. Cube already exists: ' + cubeName)

        self.db['cube'].insert_one(cube)

        # Save the cube rows
        self.db[cubeName].insert_many(cubeRows)

    #
    # Delete cube
    #
    def deleteCube(self, cubeName):
        self.db[cubeName].drop()
        self.db['cube'].remove({ "name": cubeName })

    #
    # Update cube display name
    #
    def updateCubeDisplayName(self, cubeName, displayName):
        self.__updateCubeProperty__(cubeName, { "$set": {"displayName" : displayName}})

    #
    # Update an arbitrary field in cube
    #
    def __updateCubeProperty__(self, cubeName, update):
        self.db['cube'].update_one({ "name" : cubeName}, update)

    #
    # Update an arbitrary field in a cube row
    #
    def __updateCubeRowProperty__(self, cubeName, _id, update):
        self.db[cubeName].update_one({ "_id" : _id}, update)

    #
    #  Get cube
    #
    def getCube(self, cubeName):
        return self.db['cube'].find_one({ "name": cubeName })

    #
    #  Query cube rows
    #
    def queryCubeRows(self, cubeName, filter):
        return self.db[cubeName].find(filter)

    #
    #  Get all cube rows
    #
    def getCubeRows(self, cubeName):
        return self.queryCubeRows(cubeName, {})


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
       cubeRows = self.getCubeRows(cubeName)
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

               size =  self.getCubeRows(sourceCubeName).count()

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
        cubeRows = self.getCubeRows(cubeName)
        cubeRow = cubeRows[0];
        for measure in cubeRow['measures']:
            measuresToBeBinned.append(measure)
        for date in cubeRow['dates']:
            measuresToBeBinned.append(date)
        return measuresToBeBinned

    #
    # Automatically bin a cube
    #
    def binCube(self, sourceCubeName, binnedCubeName, measuresToBeBinned=None, hints={}):
        if measuresToBeBinned == None:
            measuresToBeBinned = self.__getAllMeasuresForBinning__(sourceCubeName)

        binnings = self.__generateBinnings__(sourceCubeName, measuresToBeBinned, hints)
        return self.binCubeCustom(binnings, sourceCubeName, binnedCubeName)

    #
    # Automatically re-bin a cube
    #
    def autoRebinCube(self, sourceCubeName, binnedCubeName):

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
        return self.rebinCubeCustom(binnings, sourceCubeName, binnedCubeName)

    #
    # Bin cube with custom binnings  - Returns the binned cube
    #
    def binCubeCustom(self, binnings, sourceCubeName, binnedCubeName, binnedCubeDisplayName=None):

        if binnedCubeDisplayName == None:
            binnedCubeDisplayName = binnedCubeName
        binnedCubeRows = []
        cubeRows = self.getCubeRows(sourceCubeName)
        distincts = {}
        for cubeRow in cubeRows:
            #print cubeRow
            binnedCubeRow = self.__performBinning__(binnings, cubeRow, distincts)
            binnedCubeRows.append(binnedCubeRow)

        stats = self.getStats(binnedCubeRows)
        self.createCube('binned', binnedCubeName, binnedCubeDisplayName, binnedCubeRows, distincts, stats, binnings, None)
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"lastBinnedOn" : datetime.utcnow()}})

        return self.getCube(binnedCubeName)

    #
    # Re-bin cube using custom binnings
    #
    def rebinCubeCustom(self, binnings, sourceCubeName, binnedCubeName):
        binnedCubeRows = []
        cubeRows = self.getCubeRows(sourceCubeName)
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
    #  Aggregate a cube on a single group-by dimensions list
    #
    def aggregateCube(self, cubeName, groupByDimensions, measures=None):
        aggCubes = self.aggregateCubeComplex(cubeName, [groupByDimensions], measures)
        return aggCubes[0]

    #
    # Aggregate cube with multiple group-by dimensions list
    #
    def aggregateCubeComplex(self, cubeName, groupByDimensionsList, measures=None):
        if measures == None:
            allMeasures = []
            cubeRows = self.getCubeRows(cubeName)
            cubeRow = cubeRows[0];
            for measure in cubeRow['measures']:
                allMeasures.append(measure)
            aggs = self.__generateAggs__(cubeName, groupByDimensionsList, allMeasures)
        else:
            aggs = self.__generateAggs__(cubeName, groupByDimensionsList, measures)

        return self.aggregateCubeCustom(cubeName, aggs)

    #
    #  Is a group-by dimension a date?
    #
    def __isDateDimension__(self, cubeName, dimension):
        cubeRows = self.getCubeRows(cubeName)
        cubeRow = cubeRows[0]
        for date in cubeRow['dates']:
            if dimension == date:
                return True
        return False

    #
    # Aggregate cube using custom aggregation definitions
    #
    def aggregateCubeCustom(self, cubeName, aggs):
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

            # Does agg cube already exist? If so delete it and reccreate the agg cube
            if existingAggCube != None:
                self.deleteCube(aggCubeName)

            self.createCube('agg', aggCubeName, aggCubeName, aggCubeRows, distincts, stats, None, agg)
            resultCubes.append(self.getCube(aggCubeName))

        return resultCubes

    def addColumn(self, cubeName, newColumnName, type, expression=None, func=None):
        if type != 'numeric' and type != 'string':
            raise ValueError("type must be one of numeric, string")

        if expression == None and func == None:
            raise ValueError("You must supply an expression or function")

        if (expression != None):
            if type  == 'numeric':
               expression = expression.replace('$', "cubeRow['measures']")
            elif type == 'string':
               expression = expression.replace('$', "cubeRow['dimensions']")

        cube = self.getCube(cubeName)
        if (cube == None):
           raise ValueError("Cube does not exist:" + cubeName)

        cubeRows = self.getCubeRows(cubeName)
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

        cubeRows = self.getCubeRows(cubeName)
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
            cubeRows = self.getCubeRows(cubeName)
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
    def exportCubeToCsv(self, cubeName,csvFilePath):

        cube = self.getCube(cubeName)
        if (cube == None):
            raise ValueError("Cube does not exist:" + cubeName)

        csvfile = open(csvFilePath, 'w')
        cubeRows = self.getCubeRows(cubeName)
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


     
