import os
import unittest
import shutil
import uuid
import math
import json
import csv
from cubify import CubeService

class cubeServiceTests(unittest.TestCase):

    def testCreateCubeCellsFromCsv(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')

        cs = CubeService('testdb')
        result = cs.createCubeCellsFromCsv(cubeName + '.csv')
        cubeCells = result['cubeCells']
        distincts = result['distincts']

        self.assertTrue(len(cubeCells) == 14)
        self.assertTrue(len(distincts) == 4)

        os.remove(cubeName + '.csv')

    def testCreateCubeFromCsv(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        os.remove(cubeName + '.csv')

    def testDeleteCube(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        cs.deleteCube(cubeName)
        os.remove(cubeName + '.csv')

    def testUpdateCubeDisplayName(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        cs.updateCubeDisplayName(cubeName, 'newName')
        cube = cs.getCube(cubeName)
        self.assertTrue(cube['displayName'] == 'newName')
        os.remove(cubeName + '.csv')

    def testGetStats(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        cubeCells = cs.getCubeCells(cubeName)
        stats = cs.getStats(cubeCells)
        for stat in stats:
            self.assertTrue(stat in ['Price', 'Qty'])
            if stat == 'Price':
                self.assertTrue(math.ceil(stats[stat]['std'] * 100) / 100 == 6.42)
                self.assertTrue(stats[stat]['min'] == 1.5)
                self.assertTrue(stats[stat]['max'] == 20.5)
                self.assertTrue(stats[stat]['median'] == 12.5)
                self.assertTrue(stats[stat]['total'] == 164.0)
                self.assertTrue(math.ceil(stats[stat]['mean'] * 100) / 100 == 11.72)
            elif stat == 'Qty':
                self.assertTrue(math.ceil(stats[stat]['std'] * 100) / 100 == 1.81)
                self.assertTrue(stats[stat]['min'] == 1.0)
                self.assertTrue(stats[stat]['max'] == 7.0)
                self.assertTrue(stats[stat]['median'] == 3.0)
                self.assertTrue(stats[stat]['total'] == 44.0)
                self.assertTrue(math.ceil(stats[stat]['mean'] * 100) / 100 == 3.15)

        os.remove(cubeName + '.csv')

    def testBinning(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        cs.binCube(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        binnedCubeCells = cs.getCubeCells(cubeName + '_b')
        dimkeys = []
        for binnedCubeCell in binnedCubeCells:
            dimkeys.append(binnedCubeCell['dimensionKey'])
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

        os.remove(cubeName + '.csv')

    def testReBinning(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        cs.binCube(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        # Change the binning
        for binning in binnings:
            if binning['binningName'] == 'QtyBinning':
                bins = []
                bins.append({ "label": "0-3", "min": 0, "max": 3 })
                bins.append({ "label": "3+", "min" : 4, "max": 99999999})
                binning['bins'] = bins

        cs.rebinCube(binnings, cubeName, cubeName + "_b")

        binnedCubeCells = cs.getCubeCells(cubeName + '_b')
        dimkeys = []
        for binnedCubeCell in binnedCubeCells:
            dimkeys.append(binnedCubeCell['dimensionKey'])
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

        os.remove(cubeName + '.csv')

    def testAggregation(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv',cubeName, cubeName)
        binningFileName = 'cubify/tests/test_binnings.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        cs.binCube(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs.aggregateCube(cubeName + '_b', aggs)

        aggCubeCells = cs.getCubeCells(cubeName + '_b_agg1')
        self.assertTrue (aggCubeCells.count() == 4)
        for aggCubeCell in aggCubeCells:
            self.assertTrue(len(aggCubeCell['dimensions']) == 2)
            print aggCubeCell

        print '---------'

        aggCubeCells = cs.getCubeCells(cubeName + '_b_agg2')
        self.assertTrue (aggCubeCells.count() == 2)
        print aggCubeCells.count()
        for aggCubeCell in aggCubeCells:
            self.assertTrue(len(aggCubeCell['dimensions']) == 1)
            print aggCubeCell

        print '---------'

        aggCubeCells = cs.getCubeCells(cubeName + '_b_agg3')
        self.assertTrue (aggCubeCells.count() == 2)
        print aggCubeCells.count()
        for aggCubeCell in aggCubeCells:
            self.assertTrue(len(aggCubeCell['dimensions']) == 1)
            print aggCubeCell

        print '---------'

        os.remove(cubeName + '.csv')


    def testExportCubeToCsv(self):

        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        cs.exportCubeToCsv(cubeName, "testExported.csv")

        with open('testExported.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldNames = reader.fieldnames
            self.assertTrue(fieldNames == ['S:CustomerId', 'S:ProductId', 'S:State', 'D:Date', 'N:Price', 'N:Qty'])
            rowNum = 0
            for row in reader:
                rowNum += 1
            self.assertTrue (rowNum == 14)

        os.remove(cubeName + '.csv')
        os.remove('testExported.csv')


        
