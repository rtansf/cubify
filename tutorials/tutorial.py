from cubify import Cubify
import json

# Instantiate Cubify
cubify= Cubify()

#
# Do cleanup from previous runs of this tutorial
#
cubify.deleteCube('purchases')
cubify.deleteCube('purchases_binned_1')
cubify.deleteCube('purchases_binned_2')
cubify.deleteCube('purchases_binned_2_agg1')
cubify.deleteCube('purchases_binned_2_agg2')

# Create a cube called 'purchases'
cube = cubify.createCubeFromCsv('purchases.csv', 'purchases')
print ""
print "Dimensions in purchase cube:"
print cube['distincts']
print ""
print "Measure Statistics in purchase cube:"
print cube['stats']

# Export purchases cube to csv
cubify.exportCubeToCsv('purchases', '/tmp/exported.csv')

# Get all cube cells from purchases cube
cubeCells = cubify.getCubeCells('purchases')
print ""
print "Cube cells in purchases cube:"
for cubeCell in cubeCells:
    print cubeCell

# Query cube cells where ProductId = 'P2'
print ""
print "Cube cells where ProductId = 'P2':"
p2CubeCells = cubify.queryCubeCells('purchases', { 'dimensions.ProductId' : 'P2' })
for p2CubeCell in p2CubeCells:
    print p2CubeCell

# Perform Qty binning on cube
with open('qtyBinning.json') as qtyBinning_file:
    qtyBinning = json.load(qtyBinning_file)
binnedCube = cubify.binCube(qtyBinning, 'purchases', 'purchases_binned_1')
print ""
print "Dimensions in purchased_binned_1 cube:"
print binnedCube['distincts']

cubify.exportCubeToCsv('purchases_binned_1', '/tmp/exportedBinned1.csv')

# Perform Qty, Price, TransactionDate and CustomeState binnings on cube
with open('binnings.json') as binnings_file:
    binnings = json.load(binnings_file)
binnedCube2 = cubify.binCube(binnings, 'purchases', 'purchases_binned_2')
print ""
print "Dimensions in purchased_binned_2 cube:"
print binnedCube2['distincts']

cubify.exportCubeToCsv('purchases_binned_2', '/tmp/exportedBinned2.csv')

# Aggregate cube - example 1
with open('agg1.json') as agg_file:
    agg = json.load(agg_file)
aggCubes = cubify.aggregateCube('purchases_binned_2', agg)
aggCube = aggCubes[0]

print ""
print "Cube cells of aggregated cube: " + aggCube['name']
aggCubeCells = cubify.getCubeCells(aggCube['name'])
for aggCubeCell in aggCubeCells:
   print aggCubeCell

cubify.exportCubeToCsv(aggCube['name'], '/tmp/aggCube1.csv')

# Aggregate cube - example 2
with open('agg2.json') as agg_file:
    agg = json.load(agg_file)
aggCubes = cubify.aggregateCube('purchases_binned_2', agg)
aggCube = aggCubes[0]

print ""
print "Cube cells of aggregated cube: " + aggCube['name']
aggCubeCells = cubify.getCubeCells(aggCube['name'])
for aggCubeCell in aggCubeCells:
   print aggCubeCell

cubify.exportCubeToCsv(aggCube['name'], '/tmp/aggCube2.csv')

print ""
print "********* End Tutorial **********"
