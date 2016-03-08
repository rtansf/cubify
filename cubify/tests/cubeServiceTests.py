import os
import unittest
import shutil
import uuid
import math
import json
import csv
from cubify import CubeService

def funcx(cubeRow):
    if (cubeRow['dimensions']['State'] == 'CA'):
        return 3
    else:
        return 2

def funcy(cubeRow):
    if cubeRow['dimensions']['ProductId'] == 'P1' and cubeRow['measures']['Qty'] <= 5:
        return 'SMALL'
    elif cubeRow['dimensions']['ProductId'] == 'P1' and cubeRow['measures']['Qty'] > 5:
        return 'LARGE'
    elif cubeRow['dimensions']['ProductId'] == 'P2' and cubeRow['measures']['Qty'] <= 3:
        return 'SMALL'
    elif cubeRow['dimensions']['ProductId'] == 'P2' and cubeRow['measures']['Qty'] > 3:
        return 'LARGE'
    elif cubeRow['dimensions']['ProductId'] == 'P3' and cubeRow['measures']['Qty'] <= 6:
        return 'SMALL'
    elif cubeRow['dimensions']['ProductId'] == 'P3' and cubeRow['measures']['Qty'] > 6:
        return 'LARGE'
    else:
        return 'SMALL'

class cubeServiceTests(unittest.TestCase):

    def testCreateCubeRowsFromCsv(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')

        cs = CubeService('testdb')
        result = cs.createCubeRowsFromCsv(cubeName + '.csv')
        cubeRows = result['cubeRows']
        distincts = result['distincts']

        self.assertTrue(len(cubeRows) == 14)
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

        toCubeRows = cs.getCubeRows(toCubeName)
        self.assertTrue (toCubeRows.count() == 6)
        #for toCubeRow in toCubeRows:
        #    print toCubeRow

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
        cubeRows = cs.getCubeRows(cubeName)
        stats = cs.getStats(cubeRows)
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
        cs.binCubeCustom(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
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
        cs.binCubeCustom(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
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
        cs.binCubeCustom(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
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
        cs.binCubeCustom(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        # Change the binning
        for binning in binnings:
            if binning['binningName'] == 'QtyBinning':
                bins = []
                bins.append({ "label": "0-3", "min": 0, "max": 3 })
                bins.append({ "label": "3+", "min" : 4, "max": 99999999})
                binning['bins'] = bins

        cs.rebinCubeCustom(binnings, cubeName, cubeName + "_b")

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
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

        os.remove(cubeName + '.csv')

    def testAutoReBinning(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.binCube(cubeName, cubeName + '_b')

        # Add some new cube rows
        try:
            cs.appendToCubeFromCsv('./testdata-autobin.csv', cubeName)
        except Exception:
            cs.appendToCubeFromCsv('cubify/tests/testdata-autobin.csv', cubeName)

        cs.autoRebinCube(cubeName, cubeName + "_b")

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        #for dimkey in dimkeys:
        #    print dimkey

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#DateBin:2014-10#PriceBin:1-5#ProductId:P1#QtyBin:1-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#DateBin:2014-10#PriceBin:13-17#ProductId:P2#QtyBin:1-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#DateBin:2014-10#PriceBin:17-23#ProductId:P1#QtyBin:1-3#State:CA#Date:2014-10-10')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#DateBin:2015-10#PriceBin:1-5#ProductId:P1#QtyBin:1-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#DateBin:2015-10#PriceBin:13-17#ProductId:P2#QtyBin:1-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#DateBin:2015-10#PriceBin:17-23#ProductId:P1#QtyBin:1-3#State:CA#Date:2015-10-10')
        self.assertTrue(dimkeys[6] == '#CustomerId:C1#DateBin:2015-11#PriceBin:17-23#ProductId:P1#QtyBin:9-13#State:CA#Date:2015-11-10')
        self.assertTrue(dimkeys[7] == '#CustomerId:C1#DateBin:2015-12#PriceBin:13-17#ProductId:P2#QtyBin:7-9#State:CA#Date:2015-12-12')
        self.assertTrue(dimkeys[8] == '#CustomerId:C1#DateBin:2015-12#PriceBin:9-13#ProductId:P1#QtyBin:9-13#State:CA#Date:2015-12-11')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#DateBin:2014-10#PriceBin:1-5#ProductId:P1#QtyBin:1-3#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#DateBin:2014-10#PriceBin:17-23#ProductId:P1#QtyBin:1-3#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#DateBin:2014-10#PriceBin:5-9#ProductId:P2#QtyBin:3-5#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C2#DateBin:2015-10#PriceBin:1-5#ProductId:P1#QtyBin:1-3#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C2#DateBin:2015-10#PriceBin:17-23#ProductId:P1#QtyBin:1-3#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[14] == '#CustomerId:C2#DateBin:2015-10#PriceBin:5-9#ProductId:P2#QtyBin:3-5#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[15] == '#CustomerId:C2#DateBin:2015-11#PriceBin:13-17#ProductId:P1#QtyBin:5-7#State:NY#Date:2015-11-14')
        self.assertTrue(dimkeys[16] == '#CustomerId:C2#DateBin:2015-12#PriceBin:1-5#ProductId:P1#QtyBin:1-3#State:NY#Date:2015-12-13')
        self.assertTrue(dimkeys[17] == '#CustomerId:C2#DateBin:2015-12#PriceBin:17-23#ProductId:P2#QtyBin:5-7#State:NY#Date:2015-12-12')
        self.assertTrue(dimkeys[18] == '#CustomerId:C3#DateBin:2014-10#PriceBin:9-13#ProductId:P1#QtyBin:5-7#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[19] == '#CustomerId:C3#DateBin:2015-10#PriceBin:9-13#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-10-11')
        self.assertTrue(dimkeys[20] == '#CustomerId:C3#DateBin:2015-12#PriceBin:9-13#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-12-15')

        os.remove(cubeName + '.csv')


    def testAutoBinning(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.binCube(cubeName, cubeName + "_b", ["Price", "Qty"])

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#PriceBin:1-5#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#PriceBin:1-5#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#PriceBin:13-17#ProductId:P2#QtyBin:1-2#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#PriceBin:13-17#ProductId:P2#QtyBin:1-2#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#PriceBin:17-21#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-10')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#PriceBin:17-21#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-10')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#PriceBin:1-5#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#PriceBin:1-5#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#PriceBin:17-21#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#PriceBin:17-21#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#PriceBin:5-9#ProductId:P2#QtyBin:3-4#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#PriceBin:5-9#ProductId:P2#QtyBin:3-4#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#PriceBin:9-13#ProductId:P1#QtyBin:5-7#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#PriceBin:9-13#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-10-11')

        os.remove(cubeName + '.csv')

    def testAutoBinningAllMeasures(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.binCube(cubeName, cubeName + "_b")

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        #for dimkey in dimkeys:
        #    print dimkey

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#DateBin:2014-10#PriceBin:1-5#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#DateBin:2014-10#PriceBin:13-17#ProductId:P2#QtyBin:1-2#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#DateBin:2014-10#PriceBin:17-21#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-10')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#DateBin:2015-10#PriceBin:1-5#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#DateBin:2015-10#PriceBin:13-17#ProductId:P2#QtyBin:1-2#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#DateBin:2015-10#PriceBin:17-21#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-10')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#DateBin:2014-10#PriceBin:1-5#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#DateBin:2014-10#PriceBin:17-21#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#DateBin:2014-10#PriceBin:5-9#ProductId:P2#QtyBin:3-4#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#DateBin:2015-10#PriceBin:1-5#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#DateBin:2015-10#PriceBin:17-21#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#DateBin:2015-10#PriceBin:5-9#ProductId:P2#QtyBin:3-4#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#DateBin:2014-10#PriceBin:9-13#ProductId:P1#QtyBin:5-7#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#DateBin:2015-10#PriceBin:9-13#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-10-11')

        os.remove(cubeName + '.csv')

    def testAutoBinning2(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.binCube(cubeName, cubeName + "_b", ["Date", "Qty"])

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#DateBin:2014-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-10')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#DateBin:2014-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#DateBin:2014-10#ProductId:P2#QtyBin:1-2#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#DateBin:2015-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-10')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#DateBin:2015-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#DateBin:2015-10#ProductId:P2#QtyBin:1-2#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#DateBin:2014-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#DateBin:2014-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#DateBin:2014-10#ProductId:P2#QtyBin:3-4#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#DateBin:2015-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#DateBin:2015-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#DateBin:2015-10#ProductId:P2#QtyBin:3-4#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#DateBin:2014-10#ProductId:P1#QtyBin:5-7#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#DateBin:2015-10#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-10-11')

        os.remove(cubeName + '.csv')


    def testAutoBinning3(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.binCube(cubeName, cubeName + "_b", ["Date", "Qty"], { "Date": "monthly"})

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#DateBin:2014-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-10')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#DateBin:2014-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#DateBin:2014-10#ProductId:P2#QtyBin:1-2#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#DateBin:2015-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-10')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#DateBin:2015-10#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#DateBin:2015-10#ProductId:P2#QtyBin:1-2#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#DateBin:2014-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#DateBin:2014-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#DateBin:2014-10#ProductId:P2#QtyBin:3-4#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#DateBin:2015-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#DateBin:2015-10#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#DateBin:2015-10#ProductId:P2#QtyBin:3-4#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#DateBin:2014-10#ProductId:P1#QtyBin:5-7#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#DateBin:2015-10#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-10-11')

        os.remove(cubeName + '.csv')

    def testAutoBinning4(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.binCube(cubeName, cubeName + "_b", ["Date", "Qty"], { "Date": "weekly"})

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#DateBin:2014-41#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-10')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#DateBin:2014-41#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#DateBin:2014-41#ProductId:P2#QtyBin:1-2#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#DateBin:2015-41#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-10')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#DateBin:2015-41#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#DateBin:2015-41#ProductId:P2#QtyBin:1-2#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#DateBin:2014-41#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#DateBin:2014-41#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#DateBin:2014-41#ProductId:P2#QtyBin:3-4#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#DateBin:2015-41#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#DateBin:2015-41#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#DateBin:2015-41#ProductId:P2#QtyBin:3-4#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#DateBin:2014-41#ProductId:P1#QtyBin:5-7#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#DateBin:2015-41#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-10-11')

        os.remove(cubeName + '.csv')

    def testAutoBinning5(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv', cubeName, cubeName)

        cs.binCube(cubeName, cubeName + "_b", ["Date", "Qty"], { "Date": "yearly"})

        binnedCubeRows = cs.getCubeRows(cubeName + '_b')
        dimkeys = []
        for binnedCubeRow in binnedCubeRows:
            dimkeys.append(binnedCubeRow['dimensionKey'])
        dimkeys.sort()

        #for dimkey in dimkeys:
        #    print dimkey

        self.assertTrue(dimkeys[0] == '#CustomerId:C1#DateBin:2014#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-10')
        self.assertTrue(dimkeys[1] == '#CustomerId:C1#DateBin:2014#ProductId:P1#QtyBin:2-3#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[2] == '#CustomerId:C1#DateBin:2014#ProductId:P2#QtyBin:1-2#State:CA#Date:2014-10-11')
        self.assertTrue(dimkeys[3] == '#CustomerId:C1#DateBin:2015#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-10')
        self.assertTrue(dimkeys[4] == '#CustomerId:C1#DateBin:2015#ProductId:P1#QtyBin:2-3#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[5] == '#CustomerId:C1#DateBin:2015#ProductId:P2#QtyBin:1-2#State:CA#Date:2015-10-11')
        self.assertTrue(dimkeys[6] == '#CustomerId:C2#DateBin:2014#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[7] == '#CustomerId:C2#DateBin:2014#ProductId:P1#QtyBin:1-2#State:NY#Date:2014-10-11')
        self.assertTrue(dimkeys[8] == '#CustomerId:C2#DateBin:2014#ProductId:P2#QtyBin:3-4#State:NY#Date:2014-10-10')
        self.assertTrue(dimkeys[9] == '#CustomerId:C2#DateBin:2015#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[10] == '#CustomerId:C2#DateBin:2015#ProductId:P1#QtyBin:1-2#State:NY#Date:2015-10-11')
        self.assertTrue(dimkeys[11] == '#CustomerId:C2#DateBin:2015#ProductId:P2#QtyBin:3-4#State:NY#Date:2015-10-10')
        self.assertTrue(dimkeys[12] == '#CustomerId:C3#DateBin:2014#ProductId:P1#QtyBin:5-7#State:MA#Date:2014-10-11')
        self.assertTrue(dimkeys[13] == '#CustomerId:C3#DateBin:2015#ProductId:P1#QtyBin:5-7#State:MA#Date:2015-10-11')

        os.remove(cubeName + '.csv')

    def testAggregation(self):
        cubeName = 'test-' + str(uuid.uuid4())
        try:
            shutil.copyfile('cubify/tests/testdata.csv', cubeName + '.csv')
        except Exception:
            shutil.copyfile('./testdata.csv', cubeName + '.csv')
        cs = CubeService('testdb')
        cs.createCubeFromCsv(cubeName + '.csv',cubeName, cubeName)

        resultCubes = cs.aggregateCube(cubeName, [['ProductId', 'State'], ['CustomerId'], ['Date', 'State']])
        self.assertEquals(len(resultCubes), 3)

        #for resultCube in resultCubes:
        #    print resultCube

        aggCubeRows = cs.getCubeRows(cubeName + '_ProductId-State')
        self.assertEquals(aggCubeRows.count(), 5)
        for aggCubeRow in aggCubeRows:
            dimensionKey = aggCubeRow['dimensionKey']
            measures = aggCubeRow['measures']
            if dimensionKey == '#ProductId:P1#State:CA':
                self.assertEquals(measures['Total_Qty'], 12)
                self.assertEquals(measures['Total_Price'], 51)
                self.assertEquals(measures['Average_Qty'], 3)
                self.assertEquals(measures['Average_Price'], 12.75)
                self.assertEquals(measures['Count'], 4)
            elif dimensionKey == '#ProductId:P1#State:MA':
                self.assertEquals(measures['Total_Qty'], 14)
                self.assertEquals(measures['Total_Price'], 25)
                self.assertEquals(measures['Average_Qty'], 7)
                self.assertEquals(measures['Average_Price'], 12.5)
                self.assertEquals(measures['Count'], 2)
            elif dimensionKey == '#ProductId:P1#State:NY':
                self.assertEquals(measures['Total_Qty'], 8)
                self.assertEquals(measures['Total_Price'], 39)
                self.assertEquals(measures['Average_Qty'], 2)
                self.assertEquals(measures['Average_Price'], 9.75)
                self.assertEquals(measures['Count'], 4)
            elif dimensionKey == '#ProductId:P2#State:CA':
                self.assertEquals(measures['Total_Qty'], 2)
                self.assertEquals(measures['Total_Price'], 31)
                self.assertEquals(measures['Average_Qty'], 1)
                self.assertEquals(measures['Average_Price'], 15.5)
                self.assertEquals(measures['Count'], 2)
            elif dimensionKey == '#ProductId:P2#State:NY':
                self.assertEquals(measures['Total_Qty'], 8)
                self.assertEquals(measures['Total_Price'], 18)
                self.assertEquals(measures['Average_Qty'], 4)
                self.assertEquals(measures['Average_Price'], 9)
                self.assertEquals(measures['Count'], 2)

        aggCubeRows = cs.getCubeRows(cubeName + '_CustomerId')
        self.assertEquals(aggCubeRows.count(), 3)
        for aggCubeRow in aggCubeRows:
            #print aggCubeRow
            dimensionKey = aggCubeRow['dimensionKey']
            measures = aggCubeRow['measures']
            if dimensionKey == '#C1':
                self.assertEquals(measures['Total_Qty'], 14)
                self.assertEquals(measures['Total_Qty'], 14)
                self.assertEquals(measures['Total_Price'], 82)
                self.assertAlmostEquals(measures['Average_Qty'], 2.333)
                self.assertAlmostEquals(measures['Average_Price'], 13.667)(measures['Total_Price'], 82)
                self.assertAlmostEquals(measures['Average_Qty'], 2.333)
                self.assertAlmostEquals(measures['Average_Price'], 13.667)
            elif dimensionKey == '#C2':
                self.assertEquals(measures['Total_Qty'], 16)
                self.assertEquals(measures['Total_Price'], 57)
                self.assertAlmostEquals(measures['Average_Qty'], 2.667)
                self.assertAlmostEquals(measures['Average_Price'], 9.5)
            elif dimensionKey == '#C3':
                self.assertEquals(measures['Total_Qty'], 14)
                self.assertEquals(measures['Total_Price'], 25)
                self.assertAlmostEquals(measures['Average_Qty'], 7)
                self.assertAlmostEquals(measures['Average_Price'], 12.5)

        aggCubeRows = cs.getCubeRows(cubeName + '_Date-State')
        self.assertEquals(aggCubeRows.count(), 10)
        for aggCubeRow in aggCubeRows:
            #print aggCubeRow['dimensionKey'], aggCubeRow
            dimensionKey = aggCubeRow['dimensionKey']
            measures = aggCubeRow['measures']
            if dimensionKey == '#State:NY#Date:2015-10-11 00:00:00':
                self.assertEquals(measures['Total_Qty'], 2)
                self.assertEquals(measures['Total_Price'], 1.5)
                self.assertAlmostEquals(measures['Average_Qty'], 2)
                self.assertAlmostEquals(measures['Average_Price'], 1.5)
            elif dimensionKey == '#State:NY#Date:2014-10-10 00:00:00':
                self.assertEquals(measures['Total_Qty'], 6)
                self.assertEquals(measures['Total_Price'], 27)
                self.assertAlmostEquals(measures['Average_Qty'], 3)
                self.assertAlmostEquals(measures['Average_Price'], 13.5)

        os.remove(cubeName + '.csv')

    def testCustomAggregation(self):
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
        cs.binCubeCustom(binnings, cubeName, cubeName + '_b', cubeName + '_b')

        aggFileName = 'cubify/tests/test_agg.json'
        if os.path.isfile(aggFileName) == False:
            aggFileName = './test_agg.json'
        with open(aggFileName) as agg_file:
            aggs = json.load(agg_file)

        cs.aggregateCubeCustom(cubeName + '_b', aggs)

        aggCubeRows = cs.getCubeRows(cubeName + '_b_agg1')
        self.assertTrue (aggCubeRows.count() == 4)
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 2)
            print aggCubeRow

        print '---------'

        aggCubeRows = cs.getCubeRows(cubeName + '_b_agg2')
        self.assertTrue (aggCubeRows.count() == 2)
        print aggCubeRows.count()
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

        print '---------'

        aggCubeRows = cs.getCubeRows(cubeName + '_b_agg3')
        self.assertTrue (aggCubeRows.count() == 2)
        print aggCubeRows.count()
        for aggCubeRow in aggCubeRows:
            self.assertTrue(len(aggCubeRow['dimensions']) == 1)
            print aggCubeRow

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
        
        cubeRows = cs.getCubeRows(cubeName)
        for cubeRow in cubeRows:
            self.assertTrue ('Revenue' in cubeRow['measures'])
            self.assertTrue (cubeRow['measures']['Revenue'] == cubeRow['measures']['Price'] * cubeRow['measures']['Qty'])

        os.remove(cubeName + '.csv')

    def func(cubeRow):
        if (cubeRow['dimensions']['customerState'] == 'CA'):
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

        cubeRows = cs.getCubeRows(cubeName)
        for cubeRow in cubeRows:
            self.assertTrue ('Discount' in cubeRow['measures'])
            if (cubeRow['dimensions']['State'] == 'CA'):
               self.assertTrue (cubeRow['measures']['Discount'] == 3)
            else:
               self.assertTrue (cubeRow['measures']['Discount'] == 2)

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

        cubeRows = cs.getCubeRows(cubeName)
        for cubeRow in cubeRows:
            self.assertTrue ('ProductCategory' in cubeRow['dimensions'])
            if (cubeRow['dimensions']['ProductId'] == 'P1'):
               self.assertTrue (cubeRow['dimensions']['ProductCategory'] == 'Category1')
            else:
               self.assertTrue (cubeRow['dimensions']['ProductCategory'] == 'Category2')

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

        cubeRows = cs.getCubeRows(cubeName)
        for cubeRow in cubeRows:
            self.assertTrue ('PackageSize' in cubeRow['dimensions'])
            if cubeRow['dimensions']['ProductId'] == 'P1' and cubeRow['measures']['Qty'] <= 5:
               self.assertTrue (cubeRow['dimensions']['PackageSize'] == 'SMALL')
            elif cubeRow['dimensions']['ProductId'] == 'P1' and cubeRow['measures']['Qty'] > 5:
               self.assertTrue (cubeRow['dimensions']['PackageSize'] == 'LARGE')
            elif cubeRow['dimensions']['ProductId'] == 'P2' and cubeRow['measures']['Qty'] <= 3:
               self.assertTrue (cubeRow['dimensions']['PackageSize'] == 'SMALL')
            elif cubeRow['dimensions']['ProductId'] == 'P2' and cubeRow['measures']['Qty'] > 3:
               self.assertTrue (cubeRow['dimensions']['PackageSize'] == 'LARGE')
            elif cubeRow['dimensions']['ProductId'] == 'P3' and cubeRow['measures']['Qty'] <= 6:
               self.assertTrue (cubeRow['dimensions']['PackageSize'] == 'SMALL')
            elif cubeRow['dimensions']['ProductId'] == 'P3' and cubeRow['measures']['Qty'] > 6:
               self.assertTrue (cubeRow['dimensions']['PackageSize'] == 'LARGE')

        os.remove(cubeName + '.csv')
