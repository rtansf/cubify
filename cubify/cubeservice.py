import csv
import re
import json
import sys
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
    #  Create cube cells from csv
    #
    def createCubeCellsFromCsv(self, csvFilePath):

        cubeCells = []
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

                cubeCell = {"id": num, "dimensionKey": "", "dimensions": {}, "measures": {}, "dates": {}}
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
                            cubeCell['measures'][fieldName] = float(value)
                        else: 
                            cubeCell['measures'][fieldName] = 0.0

                    elif fieldType == 'date':
                        # Treat date value as dimension
                        d = Date(value)
                        date = datetime(d.year, d.month, d.day)
                        cubeCell['dates'][fieldName] = date

                        # Process distincts
                        self.__addToDistincts__(distincts, fieldName, value)

                    else:  # This is a string value
                        # Treat value as dimension
                        # Clean the value
                        value = self.__cleanStringValue__(value)
                        cubeCell['dimensions'][fieldName] = value

                        # Process distincts
                        self.__addToDistincts__(distincts, fieldName, value)

                dimensionKey = ''
                for dimension in sorted(cubeCell['dimensions']):
                    dimensionKey += '#' + dimension + ":" + cubeCell['dimensions'][dimension]
                for dateName in sorted(cubeCell['dates']):
                    dimensionKey += '#' + dateName + ":" + str(cubeCell['dates'][dateName])[:10]
                cubeCell['dimensionKey'] = dimensionKey

                # Add this cell to the list
                cubeCells.append(cubeCell)

            stats = self.getStats(cubeCells)
           
        return {'cubeCells': cubeCells, 'distincts': distincts, 'stats': stats}

    #
    # Create a cube from csv file. Returns the new cube
    #
    def createCubeFromCsv(self, csvFilePath, cubeName, cubeDisplayName=None):
        if cubeDisplayName == None:
            cubeDisplayName = cubeName
        result = self.createCubeCellsFromCsv(csvFilePath)
        self.createCube('source', cubeName, cubeDisplayName, result['cubeCells'], result['distincts'], result['stats'], None, None)
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
        cubeCells = self.queryCubeCells(fromCubeName, filter)
        if cubeCells.count() == 0:
            raise ValueError("Cube not created because number of cells returned by filter = 0")
            return

        # TODO - see about not bringing all  cubeCells into memory for insertion
        cells = []
        distincts = {}

        # Compute the distincts
        for cubeCell in cubeCells:
            cells.append(cubeCell)
            for dimension in cubeCell['dimensions']:
                value = cubeCell['dimensions'][dimension]
                self.__addToDistincts__(distincts, dimension, value)


        # Compute stats
        stats = self.getStats(cells)

        # Create the cube
        self.createCube('source',toCubeName, toCubeDisplayName, cells, distincts, stats, None, None)


    #
    # Append to cube from csv file
    #
    def appendToCubeFromCsv(self, csvFilePath, cubeName):
        cube = self.getCube(cubeName)
        if (cube == None):
            raise ValueError("Cube does not exist: " + cubeName)

        result = self.createCubeCellsFromCsv(csvFilePath)

        # Adjust ids # TODO use max(id) from cube instead of numCurrentCubeCells
        currentCubeCells = self.getCubeCells(cubeName)
        numCurrentCubeCells = currentCubeCells.count()
        cubeCells = result['cubeCells']
        id = numCurrentCubeCells + 1
        for cubeCell in cubeCells:
            cubeCell['id'] = id
            id += 1

        # Save the cube cells
        self.db[cubeName].insert_many(cubeCells)

        # Merge the distincts
        existingDistincts = cube['distincts']
        dcs = result['distincts']
        for dc in dcs:
            if dc not in existingDistincts:
                existingDistincts.append(dc)
        self.__updateCubeProperty__(cubeName, { "$set": {"distincts" : existingDistincts}})

        # Redo the stats
        allCubeCells = self.getCubeCells(cubeName)
        stats = self.getStats(allCubeCells)
        self.__updateCubeProperty__(cubeName, { "$set": {"stats" : stats}})

    #
    # Delete cube cells from a cube
    #
    def deleteCubeCells(self, cubeName, filter):
        cube = self.getCube(cubeName)
        if (cube == None):
            raise ValueError("Cube does not exist:" + cubeName)

        self.db[cubeName].remove(filter)

        allCubeCells = self.getCubeCells(cubeName)

        # Redo distincts
        distincts =  {}
        for cell in allCubeCells:
            for k, v in cell['dates'].items():
                self.__addToDistincts__(distincts, k, str(v))
            for k, v in cell['dimensions'].items():
                self.__addToDistincts__(distincts, k, v)

        cube['distincts'] = distincts
        self.db['cube'].update_one({ "name" : cubeName}, { '$set' : { "distincts" : distincts}})

        # Redo stats
        stats = self.getStats(allCubeCells)
        self.__updateCubeProperty__(cubeName, { "$set": {"stats" : stats}})

    #
    # Create cube
    #
    def createCube(self, type, cubeName, cubeDisplayName, cubeCells, distincts, stats, binnings, agg):

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

        # Save the cube cells
        self.db[cubeName].insert_many(cubeCells)

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
    # Update an arbitrary field in a cube cell
    #
    def __updateCubeCellProperty__(self, cubeName, _id, update):
        self.db[cubeName].update_one({ "_id" : _id}, update)

    #
    #  Get cube
    #
    def getCube(self, cubeName):
        return self.db['cube'].find_one({ "name": cubeName })

    #
    #  Query cube cells
    #
    def queryCubeCells(self, cubeName, filter):
        return self.db[cubeName].find(filter)

    #
    #  Get all cube cells
    #
    def getCubeCells(self, cubeName):
        return self.queryCubeCells(cubeName, {})


    #
    # Compute stats on cube
    #
    def getStats(self, cubeCells):

        stats = {}
        measureValues = {}
        for cell in cubeCells:
            for k, v in cell['measures'].items():
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
           if period == 'weekly':
               dt = datetime(d.year, d.month, d.day)
               isoCalendar = dt.isocalendar()
               yearNum = isoCalendar[0]
               weekNum = isoCalendar[1]
               return str(yearNum) + "-" + str(weekNum)
           elif period == 'monthly':
               return str(d.year) + "-" + str(d.month)
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
    def __performBinning__(self, binnings, cubeCell, distincts):

        binnedCubeCell = deepcopy(cubeCell)

        for binning in binnings:

            sourceField = binning['sourceField']
            outputField = binning['outputField']['name']

            if binning['type'] == 'range':
                if sourceField not in cubeCell['measures']:
                    continue
                value = cubeCell['measures'][sourceField]
                label = self.__getNumericBinLabel__(value, binning)
                binnedCubeCell['dimensions'][outputField] = label

            elif binning['type'] == 'enum':
                if sourceField not in cubeCell['dimensions']:
                    continue
                value = cubeCell['dimensions'][sourceField]
                label = self.__getStringBinLabel__(value, binning)
                binnedCubeCell['dimensions'][outputField] = label

            elif binning['type'] == 'date':
                if sourceField not in cubeCell['dates']:
                    continue
                value = cubeCell['dates'][sourceField]
                label = self.__getDateBinLabel__(value, binning)
                binnedCubeCell['dimensions'][outputField] = label

        dimensionKey = ''
        for dimension in sorted(binnedCubeCell['dimensions']):
            self.__addToDistincts__(distincts, dimension, binnedCubeCell['dimensions'][dimension])
            dimensionKey += '#' + dimension + ":" + binnedCubeCell['dimensions'][dimension]
        for dateName in sorted(binnedCubeCell['dates']):
            dimensionKey += '#' + dateName + ":" + str(binnedCubeCell['dates'][dateName])[:10]
        binnedCubeCell['dimensionKey'] = dimensionKey


        return binnedCubeCell

    #
    # Bin cube - Returns the binned cube
    #
    def binCube(self, binnings, sourceCubeName, binnedCubeName, binnedCubeDisplayName=None):
        if binnedCubeDisplayName == None:
            binnedCubeDisplayName = binnedCubeName
        binnedCubeCells = []
        cubeCells = self.getCubeCells(sourceCubeName)
        distincts = {}
        for cubeCell in cubeCells:
            #print cubeCell
            binnedCubeCell = self.__performBinning__(binnings, cubeCell, distincts)
            binnedCubeCells.append(binnedCubeCell)

        stats = self.getStats(binnedCubeCells)
        self.createCube('binned', binnedCubeName, binnedCubeDisplayName, binnedCubeCells, distincts, stats, binnings, None)
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"lastBinnedOn" : datetime.utcnow()}})

        return self.getCube(binnedCubeName)

    #
    # Re-bin cube
    #
    def rebinCube(self, binnings, sourceCubeName, binnedCubeName):
        binnedCubeCells = []
        cubeCells = self.getCubeCells(sourceCubeName)
        distincts = {}
        for cubeCell in cubeCells:
            #print cubeCell
            binnedCubeCell = self.__performBinning__(binnings, cubeCell, distincts)
            binnedCubeCells.append(binnedCubeCell)
        stats = self.getStats(binnedCubeCells)

        # Drop old cube cell collection
        self.db[binnedCubeName].drop()

        # Recreate the new cube cell collection
        self.db[binnedCubeName].insert_many(binnedCubeCells)

        # Update the binned cube
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"binningsUpdatedOn" : datetime.utcnow()}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"lastBinnedOn" : datetime.utcnow()}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"binnings" : binnings}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"stats" : stats}})
        self.__updateCubeProperty__(binnedCubeName, { "$set": {"distincts" : distincts}})

        return self.getCube(binnedCubeName)

    #
    # Aggregate cube
    #
    def aggregateCube(self, cubeName, aggs):
        resultCubes = []
        for agg in aggs:
            aggName = agg['name']
            aggCubeCells = []
            distincts = {}
            pipeline = []
            groupPipelineItem = {}
            pipeline.append(groupPipelineItem)
            idContent = {}
            group = {}
            groupPipelineItem['$group'] = group
            group['_id'] = idContent
            for dimension in agg['dimensions']:
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
                cubeCell = { "id": num, "dimensionKey": "", "dimensions": {}, "measures": {}, "dates": {} }
                num += 1
                for k, v in aggResult.items():
                   if k == '_id':
                      for dimName, dimValue in v.items():
                         cubeCell['dimensions'][dimName] = dimValue
                         self.__addToDistincts__(distincts, dimName, dimValue)
                   else:
                       cubeCell['measures'][k] = v
                   dimensionKey = ''
                   for dim in sorted(cubeCell['dimensions']):
                       dimensionKey += '#' + dim + ':' + cubeCell['dimensions'][dim]
                       cubeCell['dimensionKey'] = dimensionKey
                aggCubeCells.append(cubeCell)

            aggCubeName = cubeName + "_" + aggName
            existingAggCube = self.getCube(aggCubeName)
            stats = self.getStats(aggCubeCells)

            # Does agg cube already exist? If so delete it and reccreate the agg cube
            if existingAggCube != None:
                self.deleteCube(aggCubeName)

            self.createCube('agg', aggCubeName, aggCubeName, aggCubeCells, distincts, stats, None, agg)
            resultCubes.append(self.getCube(aggCubeName))

        return resultCubes

    def addColumn(self, cubeName, newColumnName, type, expression=None, func=None):
        if type != 'numeric' and type != 'string':
            raise ValueError("type must be one of numeric, string")

        if expression == None and func == None:
            raise ValueError("You must supply an expression or function")

        if (expression != None):
            if type  == 'numeric':
               expression = expression.replace('$', "cubeCell['measures']")
            elif type == 'string':
               expression = expression.replace('$', "cubeCell['dimensions']")

        cube = self.getCube(cubeName)
        if (cube == None):
           raise ValueError("Cube does not exist:" + cubeName)

        cubeCells = self.getCubeCells(cubeName)
        tempMap = {}

        for cubeCell in cubeCells:
            try:
                if expression != None:
                    value = eval(expression)
                else:
                    value = func(cubeCell)
                cubeId = cubeCell['id']
                tempMap[cubeId] = value
            except SyntaxError:
                raise SyntaxError("Expression contains syntax error.")

        cubeCells = self.getCubeCells(cubeName)
        for cubeCell in cubeCells:
            cubeId = cubeCell['id']
            value = tempMap[cubeId]
            if (type == 'numeric'):
                cubeCell['measures'][newColumnName] = value
                measureUpdateField = 'measures.' + newColumnName
                self.__updateCubeCellProperty__(cubeName, cubeCell['_id'], { "$set": {measureUpdateField : value}})
            elif (type == 'string'):
                cubeCell['dimensions'][newColumnName] = value
                dimensionUpdateField = 'dimensions.' + newColumnName
                self.__updateCubeCellProperty__(cubeName, cubeCell['_id'], { "$set": {dimensionUpdateField : value}})
                # Reconstruct dimension key
                dimensionKey = self.__reconstructDimensionKey__(cubeCell['dimensions'], cubeCell['dates'])
                self.__updateCubeCellProperty__(cubeName, cubeCell['_id'], { "$set": {"dimensionKey": dimensionKey}})
                self.__addToDistincts__(cube['distincts'], newColumnName, value)

        if type == 'numeric':
            # Recompute stats
            cubeCells = self.getCubeCells(cubeName)
            stats = self.getStats(cubeCells)
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
    # Export cube cells to csv
    #
    def exportCubeToCsv(self, cubeName,csvFilePath):

        cube = self.getCube(cubeName)
        if (cube == None):
            raise ValueError("Cube does not exist:" + cubeName)

        csvfile = open(csvFilePath, 'w')
        cubeCells = self.getCubeCells(cubeName)
        fieldNames = []
        for cubeCell in cubeCells:
            if len(fieldNames) == 0:
                for dimName in sorted(cubeCell['dimensions']):
                   fieldNames.append('S:'+dimName)
                for dateName in sorted(cubeCell['dates']):
                   fieldNames.append('D:'+dateName)
                for measure in sorted(cubeCell['measures']):
                   fieldNames.append('N:'+measure)

                writer = csv.DictWriter(csvfile, fieldnames=fieldNames)
                writer.writeheader()
             
            row = {}
            for dimName in cubeCell['dimensions']:
                value = cubeCell['dimensions'][dimName]
                rawDimName = 'S:'+dimName
                row[rawDimName] = value
            for dateName in cubeCell['dates']:
                value = cubeCell['dates'][dateName]
                rawDateName = 'D:'+dateName
                row[rawDateName] = value
            for measure in cubeCell['measures']:
                value = cubeCell['measures'][measure]
                rawMeasureName = 'N:'+measure
                row[rawMeasureName] = value

            writer.writerow(row)


     
