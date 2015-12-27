import os
import unittest
import shutil
import uuid
import math
import json
import csv
from cubify import CubeService

def funcx(cubeCell):
    if (cubeCell['dimensions']['State'] == 'CA'):
        return 3
    else:
        return 2

def funcy(cubeCell):
    if cubeCell['dimensions']['ProductId'] == 'P1' and cubeCell['measures']['Qty'] <= 5:
        return 'SMALL'
    elif cubeCell['dimensions']['ProductId'] == 'P1' and cubeCell['measures']['Qty'] > 5:
        return 'LARGE'
    elif cubeCell['dimensions']['ProductId'] == 'P2' and cubeCell['measures']['Qty'] <= 3:
        return 'SMALL'
    elif cubeCell['dimensions']['ProductId'] == 'P2' and cubeCell['measures']['Qty'] > 3:
        return 'LARGE'
    elif cubeCell['dimensions']['ProductId'] == 'P3' and cubeCell['measures']['Qty'] <= 6:
        return 'SMALL'
    elif cubeCell['dimensions']['ProductId'] == 'P3' and cubeCell['measures']['Qty'] > 6:
        return 'LARGE'
    else:
        return 'SMALL'

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

    def testCreateCubeFromCube(self):
        cubeName = 'test-' + str(uuid.uuid4())
        toCubeName = 'test2-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        cs.createCubeFromCube(cubeName, { "dimensions.State" : "NY" }, toCubeName)

        toCube = cs.getCube(toCubeName)
        self.assertTrue (toCube != None)
        distincts = toCube['distincts']
        self.assertTrue (distincts['State'] == {'NY' : 6})
        #print toCube

        toCubeCells = cs.getCubeCells(toCubeName)
        self.assertTrue (toCubeCells.count() == 6)
        #for toCubeCell in toCubeCells:
        #    print toCubeCell

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

    def testBinningDateMonthly(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        binningFileName = 'cubify/tests/test_binnings_date1.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings_date1.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        cs.binCube(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        binnedCubeCells = cs.getCubeCells(cubeName + '_b')
        dimkeys = []
        for binnedCubeCell in binnedCubeCells:
            dimkeys.append(binnedCubeCell['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#ProductId:P1#State:CA#YearMonth:2014-10#Date:2014-10-10')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#ProductId:P1#State:CA#YearMonth:2014-10#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#ProductId:P1#State:CA#YearMonth:2015-10#Date:2015-10-10')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#ProductId:P1#State:CA#YearMonth:2015-10#Date:2015-10-11')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#ProductId:P2#State:CA#YearMonth:2014-10#Date:2014-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#ProductId:P2#State:CA#YearMonth:2015-10#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#ProductId:P1#State:NY#YearMonth:2014-10#Date:2014-10-10')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#ProductId:P1#State:NY#YearMonth:2014-10#Date:2014-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#ProductId:P1#State:NY#YearMonth:2015-10#Date:2015-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#ProductId:P1#State:NY#YearMonth:2015-10#Date:2015-10-11')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#ProductId:P2#State:NY#YearMonth:2014-10#Date:2014-10-10')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#ProductId:P2#State:NY#YearMonth:2015-10#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#ProductId:P1#State:MA#YearMonth:2014-10#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#ProductId:P1#State:MA#YearMonth:2015-10#Date:2015-10-11')

        os.remove(cubeName + '.csv')

    def testBinningDateWeekly(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)
        binningFileName = 'cubify/tests/test_binnings_date2.json'
        if (os.path.isfile(binningFileName) == False):
            binningFileName = './test_binnings_date2.json'
        with open(binningFileName) as binnings_file:
            binnings = json.load(binnings_file)
        cs.binCube(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        binnedCubeCells = cs.getCubeCells(cubeName + '_b')
        dimkeys = []
        for binnedCubeCell in binnedCubeCells:
            dimkeys.append(binnedCubeCell['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#ProductId:P1#State:CA#Week:2014-41#Date:2014-10-10')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#ProductId:P1#State:CA#Week:2014-41#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#ProductId:P1#State:CA#Week:2015-41#Date:2015-10-10')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#ProductId:P1#State:CA#Week:2015-41#Date:2015-10-11')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#ProductId:P2#State:CA#Week:2014-41#Date:2014-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#ProductId:P2#State:CA#Week:2015-41#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#ProductId:P1#State:NY#Week:2014-41#Date:2014-10-10')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#ProductId:P1#State:NY#Week:2014-41#Date:2014-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#ProductId:P1#State:NY#Week:2015-41#Date:2015-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#ProductId:P1#State:NY#Week:2015-41#Date:2015-10-11')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#ProductId:P2#State:NY#Week:2014-41#Date:2014-10-10')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#ProductId:P2#State:NY#Week:2015-41#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#ProductId:P1#State:MA#Week:2014-41#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#ProductId:P1#State:MA#Week:2015-41#Date:2015-10-11')

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

    def testAddColumnNumeric1(self):

        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.addColumn(cubeName, 'Revenue', 'numeric', "$['Qty'] * $['Price']")
        cube = cs.getCube(cubeName)
        stats = cube['stats']
        self.assertTrue ('Revenue' in stats)
        
        cubeCells = cs.getCubeCells(cubeName)
        for cubeCell in cubeCells:
            self.assertTrue ('Revenue' in cubeCell['measures'])
            self.assertTrue (cubeCell['measures']['Revenue'] == cubeCell['measures']['Price'] * cubeCell['measures']['Qty'])

        os.remove(cubeName + '.csv')

    def func(cubeCell):
        if (cubeCell['dimensions']['customerState'] == 'CA'):
            return 3
        else:
            return 2
        
    def testAddColumnNumeric2(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.addColumn(cubeName, 'Discount', 'numeric', None, funcx)
        cube = cs.getCube(cubeName)
        stats = cube['stats']
        self.assertTrue ('Discount' in stats)

        cubeCells = cs.getCubeCells(cubeName)
        for cubeCell in cubeCells:
            self.assertTrue ('Discount' in cubeCell['measures'])
            if (cubeCell['dimensions']['State'] == 'CA'):
               self.assertTrue (cubeCell['measures']['Discount'] == 3)
            else:
               self.assertTrue (cubeCell['measures']['Discount'] == 2)

        os.remove(cubeName + '.csv')

    def testAddColumnString1(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.addColumn(cubeName, 'ProductCategory', 'string', "'Category1' if $['ProductId'] == 'P1' else 'Category2'", None)
        cube = cs.getCube(cubeName)
        self.assertTrue('ProductCategory' in cube['distincts'])

        cubeCells = cs.getCubeCells(cubeName)
        for cubeCell in cubeCells:
            self.assertTrue ('ProductCategory' in cubeCell['dimensions'])
            if (cubeCell['dimensions']['ProductId'] == 'P1'):
               self.assertTrue (cubeCell['dimensions']['ProductCategory'] == 'Category1')
            else:
               self.assertTrue (cubeCell['dimensions']['ProductCategory'] == 'Category2')

        os.remove(cubeName + '.csv')

    def testAddColumnString2(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.addColumn(cubeName, 'PackageSize', 'string', None, funcy)
        cube = cs.getCube(cubeName)
        self.assertTrue('PackageSize' in cube['distincts'])

        cubeCells = cs.getCubeCells(cubeName)
        for cubeCell in cubeCells:
            self.assertTrue ('PackageSize' in cubeCell['dimensions'])
            if cubeCell['dimensions']['ProductId'] == 'P1' and cubeCell['measures']['Qty'] <= 5:
               self.assertTrue (cubeCell['dimensions']['PackageSize'] == 'SMALL')
            elif cubeCell['dimensions']['ProductId'] == 'P1' and cubeCell['measures']['Qty'] > 5:
               self.assertTrue (cubeCell['dimensions']['PackageSize'] == 'LARGE')
            elif cubeCell['dimensions']['ProductId'] == 'P2' and cubeCell['measures']['Qty'] <= 3:
               self.assertTrue (cubeCell['dimensions']['PackageSize'] == 'SMALL')
            elif cubeCell['dimensions']['ProductId'] == 'P2' and cubeCell['measures']['Qty'] > 3:
               self.assertTrue (cubeCell['dimensions']['PackageSize'] == 'LARGE')
            elif cubeCell['dimensions']['ProductId'] == 'P3' and cubeCell['measures']['Qty'] <= 6:
               self.assertTrue (cubeCell['dimensions']['PackageSize'] == 'SMALL')
            elif cubeCell['dimensions']['ProductId'] == 'P3' and cubeCell['measures']['Qty'] > 6:
               self.assertTrue (cubeCell['dimensions']['PackageSize'] == 'LARGE')

        os.remove(cubeName + '.csv')
