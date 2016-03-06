import os
import unittest
import shutil
import uuid
import math
import json
import csv
from cubify import CubeSetService

class cubeSetServiceTests(unittest.TestCase):

    def testCreateCubeSet(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'

        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)

        cubeSet = cs.getCubeSet(cubeSetName)
        self.assertTrue(cubeSet != None)
        self.assertTrue(cubeSet['name'] == cubeSetName)
        self.assertTrue(cubeSet['displayName'] == cubeSetName)
        self.assertTrue(cubeSet['owner'] == 'testOwner')
        self.assertTrue(cubeSet['csvFilePath'] == csvFilePath)
        self.assertTrue(cubeSet['sourceCube'] == cubeSetName + '_source')
        self.assertTrue(cubeSet['binnedCube'] == cubeSetName + '_binned')
        binnedCubeName = cubeSet['binnedCube']
        self.assertTrue(len(cubeSet['aggCubes']) == 3)
        self.assertTrue(binnedCubeName + '_' + aggs[0]['name'] in cubeSet['aggCubes'])
        self.assertTrue(binnedCubeName + '_' + aggs[1]['name'] in cubeSet['aggCubes'])
        self.assertTrue(binnedCubeName + '_' + aggs[2]['name'] in cubeSet['aggCubes'])

        os.remove(csvFilePath)

    def testCreateCubeSetWithAutoBinning(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'

        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath)

        cubeSet = cs.getCubeSet(cubeSetName)
        self.assertTrue(cubeSet != None)
        self.assertTrue(cubeSet['name'] == cubeSetName)
        self.assertTrue(cubeSet['displayName'] == cubeSetName)
        self.assertTrue(cubeSet['owner'] == 'testOwner')
        self.assertTrue(cubeSet['csvFilePath'] == csvFilePath)
        self.assertTrue(cubeSet['sourceCube'] == cubeSetName + '_source')
        self.assertTrue(cubeSet['binnedCube'] == cubeSetName + '_binned')
        self.assertFalse("aggs" in cubeSet)

        os.remove(csvFilePath)

    def testUpdateCubeSetDisplayName(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'

        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, None, None)
        cs.updateCubeSetDisplayName(cubeSetName, 'new name')

        cubeSet = cs.getCubeSet(cubeSetName)
        self.assertTrue(cubeSet != None)
        self.assertTrue(cubeSet['name'] == cubeSetName)
        self.assertTrue(cubeSet['displayName'] == 'new name')
        self.assertTrue(cubeSet['owner'] == 'testOwner')
        self.assertTrue(cubeSet['csvFilePath'] == csvFilePath)
        self.assertTrue(cubeSet['sourceCube'] == cubeSetName + '_source')

        os.remove(csvFilePath)

    def testDeleteCubeSet(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, None, None)
        cs.deleteCubeSet(cubeSetName)
        cubeSet = cs.getCubeSet(cubeSetName)
        self.assertTrue(cubeSet == None)

        os.remove(csvFilePath)

    def testDeleteCubeSet2(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)
        cs.deleteCubeSet(cubeSetName)
        cubeSet = cs.getCubeSet(cubeSetName)
        self.assertTrue(cubeSet == None)

        os.remove(csvFilePath)

    def testGetSourceCubeRows(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)
        cubeRows = cs.getSourceCubeRows(cubeSetName)
        self.assertTrue(cubeRows.count() == 14)

        os.remove(csvFilePath)

    def testGetBinnedCubeRows(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)
        binnedCubeRows = cs.getBinnedCubeRows(cubeSetName)

        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#PriceBin:10+#ProductId:P2#QtyBin:0-5#Region:West#State:CA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#PriceBin:10+#ProductId:P2#QtyBin:0-5#Region:West#State:CA#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#PriceBin:5-10#ProductId:P2#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#PriceBin:5-10#ProductId:P2#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#PriceBin:10+#ProductId:P1#QtyBin:5+#Region:NorthEast#State:MA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#PriceBin:10+#ProductId:P1#QtyBin:5+#Region:NorthEast#State:MA#Year:Year2015#Date:2015-10-11')

        os.remove(csvFilePath)

    def testGetAggregatedCubeRows(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)

        agg = aggs[0]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 4)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 2)
            print aggCubeRow

        print '---------'

        agg = aggs[1]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 2)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

        print '---------'

        agg = aggs[2]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 2)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

        print '---------'

        os.remove(csvFilePath)

    def testPerformBinning(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, None)

        # Change the binning
        for binning in binnings:
            if binning['binningName'] == 'QtyBinning':
                bins = []
                bins.append({ "label": "0-3", "min": 0, "max": 3 })
                bins.append({ "label": "3+", "min" : 4, "max": 99999999})
                binning['bins'] = bins

        cs.performBinning(cubeSetName, binnings)

        binnedCubeRows = cs.getBinnedCubeRows(cubeSetName)
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#PriceBin:0-5#ProductId:P1#QtyBin:0-3#Region:West#State:CA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#PriceBin:0-5#ProductId:P1#QtyBin:0-3#Region:West#State:CA#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#PriceBin:10+#ProductId:P1#QtyBin:0-3#Region:West#State:CA#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#PriceBin:10+#ProductId:P1#QtyBin:0-3#Region:West#State:CA#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#PriceBin:10+#ProductId:P2#QtyBin:0-3#Region:West#State:CA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#PriceBin:10+#ProductId:P2#QtyBin:0-3#Region:West#State:CA#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#PriceBin:0-5#ProductId:P1#QtyBin:0-3#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#PriceBin:0-5#ProductId:P1#QtyBin:0-3#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#PriceBin:10+#ProductId:P1#QtyBin:0-3#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#PriceBin:10+#ProductId:P1#QtyBin:0-3#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#PriceBin:5-10#ProductId:P2#QtyBin:3+#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#PriceBin:5-10#ProductId:P2#QtyBin:3+#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#PriceBin:10+#ProductId:P1#QtyBin:3+#Region:NorthEast#State:MA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#PriceBin:10+#ProductId:P1#QtyBin:3+#Region:NorthEast#State:MA#Year:Year2015#Date:2015-10-11')

        os.remove(cubeSetName + '.csv')

    def testPerformAggregation(self):

        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, None)
        cs.performAggregation(cubeSetName, aggs)

        agg = aggs[0]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 4)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 2)
            print aggCubeRow

        print '---------'

        agg = aggs[1]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 2)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

        print '---------'

        agg = aggs[2]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 2)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

        print '---------'

        os.remove(cubeSetName + '.csv')

    def testAddRowsToSourceCube(self):

        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)

        incFileName = 'cubify/tests/testdataIncremental.csv'
        if (os.path.isfile(incFileName) == False):
            incFileName = './testdataIncremental.csv'
        cs.addRowsToSourceCube(cubeSetName, incFileName)

        sourceCubeRows = cs.getSourceCubeRows(cubeSetName)
        self.assertTrue(sourceCubeRows.count() == 21)

        binnedCubeRows = cs.getBinnedCubeRows(cubeSetName)
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2016#Date:2016-10-11')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:West#State:CA#Year:Year2016#Date:2016-10-10')
        self.assertTrue(dimkeys[6] == '#CustomerId:C1#PriceBin:10+#ProductId:P2#QtyBin:0-5#Region:West#State:CA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[7] == '#CustomerId:C1#PriceBin:10+#ProductId:P2#QtyBin:0-5#Region:West#State:CA#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C1#PriceBin:10+#ProductId:P2#QtyBin:0-5#Region:West#State:CA#Year:Year2016#Date:2016-10-11')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#PriceBin:0-5#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2016#Date:2016-10-11')
        self.assertTrue(dimkeys[12] == '#CustomerId:C2#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[13] == '#CustomerId:C2#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[14] == '#CustomerId:C2#PriceBin:10+#ProductId:P1#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2016#Date:2016-10-10')
        self.assertTrue(dimkeys[15] == '#CustomerId:C2#PriceBin:5-10#ProductId:P2#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2014#Date:2014-10-10')
        self.assertTrue(dimkeys[16] == '#CustomerId:C2#PriceBin:5-10#ProductId:P2#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2015#Date:2015-10-10')
        self.assertTrue(dimkeys[17] == '#CustomerId:C2#PriceBin:5-10#ProductId:P2#QtyBin:0-5#Region:NorthEast#State:NY#Year:Year2016#Date:2016-10-10')
        self.assertTrue(dimkeys[18] == '#CustomerId:C3#PriceBin:10+#ProductId:P1#QtyBin:5+#Region:NorthEast#State:MA#Year:Year2014#Date:2014-10-11')
        self.assertTrue(dimkeys[19] == '#CustomerId:C3#PriceBin:10+#ProductId:P1#QtyBin:5+#Region:NorthEast#State:MA#Year:Year2015#Date:2015-10-11')
        self.assertTrue(dimkeys[20] == '#CustomerId:C3#PriceBin:10+#ProductId:P1#QtyBin:5+#Region:NorthEast#State:MA#Year:Year2016#Date:2016-10-11')

        agg = aggs[0]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 4)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 2)
            print aggCubeRow

        print '---------'

        agg = aggs[1]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 2)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

        print '---------'

        agg = aggs[2]
        aggCubeRows = cs.getAggregatedCubeRows(cubeSetName, agg['name'])
        self.assertTrue (aggCubeRows.count(False) == 2)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

        print '---------'

        os.remove(cubeSetName + '.csv')


    def testRemoveRowsFromSourceCube(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)
        cs.removeRowsFromSourceCube(cubeSetName, { "dimensions.State" : "CA"})

        cubeRows = cs.getSourceCubeRows(cubeSetName)
        self.assertTrue(cubeRows.count() == 8)

        dimkeys = []
        for cubeRow in cubeRows:
            dimkeys.append(cubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C2#ProductId:P1#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[1] == '#CustomerId:C2#ProductId:P1#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C2#ProductId:P1#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[3] == '#CustomerId:C2#ProductId:P1#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[4] == '#CustomerId:C2#ProductId:P2#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[5] == '#CustomerId:C2#ProductId:P2#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[6] == '#CustomerId:C3#ProductId:P1#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[7] == '#CustomerId:C3#ProductId:P1#State:MA#Date:2015-10-11')

        os.remove(cubeSetName + '.csv')

    def testExportSourceCubeCsv(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'

        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)

        cs.exportSourceCubeToCsv(cubeSetName, "cubeSetSourceExported.csv")

        with open('cubeSetSourceExported.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldNames = reader.fieldnames
            self.assertTrue(fieldNames == ['S:CustomerId', 'S:ProductId', 'S:State', 'D:Date', 'N:Price', 'N:Qty'])
            rowNum = 0
            for row in reader:
                rowNum += 1
            self.assertTrue (rowNum == 14)

        os.remove(cubeSetName + '.csv')
        os.remove('cubeSetSourceExported.csv')

    def testExportBinnedCubeCsv(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'

        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)

        cs.exportBinnedCubeToCsv(cubeSetName, "cubeSetBinnedExported.csv")

        with open('cubeSetBinnedExported.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldNames = reader.fieldnames
            self.assertTrue(fieldNames == ['S:CustomerId', 'S:PriceBin', 'S:ProductId', 'S:QtyBin', 'S:Region', 'S:State', 'S:Year', 'D:Date', 'N:Price', 'N:Qty'])
            rowNum = 0
            for row in reader:
                rowNum += 1
            self.assertTrue (rowNum == 14)

        os.remove(cubeSetName + '.csv')
        os.remove('cubeSetBinnedExported.csv')

    def testExportAggCubesCsv(self):
        cubeSetName = 'test-' + str(uuid.uuid4())
        csvFilePath =  cubeSetName + '.csv'

        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeSetName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeSetName + '.csv')
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs = CubeSetService('testdb')
        cs.createCubeSet("testOwner", cubeSetName, cubeSetName, csvFilePath, binnings, aggs)

        cs.exportAggCubeToCsv(cubeSetName, "cubeSetAgg" + aggs[0]['name'] + "Exported.csv", aggs[0]['name'])
        with open("cubeSetAgg" + aggs[0]['name'] + "Exported.csv") as csvfile:
            reader = csv.DictReader(csvfile)
            fieldNames = reader.fieldnames
            self.assertTrue(fieldNames == ['S:ProductId', 'S:Region', 'N:AveragePrice', 'N:AverageQty'])
            rowNum = 0
            for row in reader:
                rowNum += 1
            self.assertTrue (rowNum == 4)


        cs.exportAggCubeToCsv(cubeSetName, "cubeSetAgg" + aggs[1]['name'] + "Exported.csv", aggs[1]['name'])
        with open("cubeSetAgg" + aggs[1]['name'] + "Exported.csv") as csvfile:
            reader = csv.DictReader(csvfile)
            fieldNames = reader.fieldnames
            self.assertTrue(fieldNames == ['S:ProductId', 'N:TotalQty'])
            rowNum = 0
            for row in reader:
                rowNum += 1
            self.assertTrue (rowNum == 2)

        cs.exportAggCubeToCsv(cubeSetName, "cubeSetAgg" + aggs[2]['name'] + "Exported.csv", aggs[2]['name'])
        with open("cubeSetAgg" + aggs[2]['name'] + "Exported.csv") as csvfile:
            reader = csv.DictReader(csvfile)
            fieldNames = reader.fieldnames
            self.assertTrue(fieldNames == ['S:ProductId', 'N:AverageRevenue'])
            rowNum = 0
            for row in reader:
                rowNum += 1
            self.assertTrue (rowNum == 2)

        os.remove(cubeSetName + '.csv')
        os.remove("cubeSetAgg" + aggs[0]['name'] + "Exported.csv")
        os.remove("cubeSetAgg" + aggs[1]['name'] + "Exported.csv")
        os.remove("cubeSetAgg" + aggs[2]['name'] + "Exported.csv")
