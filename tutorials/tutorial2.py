#
# Cubify Tutorial Part 2
#
# This tutorial shows you how to use CubeSets 
#

from cubify import Cubify
import json

# Instantiate Cubify
cubify= Cubify()

#
# Do cleanup from previous runs of this tutorial
#
cubify.deleteCubeSet('purchasesCubeSet')

# Create a cube set called 'purchasesCubeSet' with binnings defined in binning.json and aggregations defined in aggs.json

with open('binnings.json') as binnings_file:
    binnings = json.load(binnings_file)

with open('aggs.json') as aggs_file:
    aggs = json.load(aggs_file)

cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet', 'Purchases Cube Set', 'purchases.csv', binnings, aggs)

print ""
print "CubeSet created successfully"
print ""

cubeCells = cubify.getSourceCubeCells('purchasesCubeSet')
binnedCubeCells = cubify.getBinnedCubeCells('purchasesCubeSet')
agg1CubeCells = cubify.getAggregatedCubeCells('purchasesCubeSet', 'agg1')
agg2CubeCells = cubify.getAggregatedCubeCells('purchasesCubeSet', 'agg2')

print ""
print "Cube cells in purchasesCubeSet's source cube:"
for cubeCell in cubeCells:
    print cubeCell
print ""

print ""
print "Cube cells in purchasesCubeSet's binned cube:"
for cubeCell in binnedCubeCells:
    print cubeCell
print ""

print ""
print "Cube cells in purchasesCubeSet's agg1 cube:"
for cubeCell in agg1CubeCells:
    print cubeCell
print ""

print ""
print "Cube cells in purchasesCubeSet's agg2 cube:"
for cubeCell in agg2CubeCells:
    print cubeCell
print ""

#
#  Add more cells to purchasesCubeSet's source cube
#
cubify.addCellsToSourceCube('purchasesCubeSet', 'morePurchases.csv')

agg1CubeCells = cubify.getAggregatedCubeCells('purchasesCubeSet', 'agg1')
print ""
print "Cube cells in purchasesCubeSet's agg1 cube:"
for cubeCell in agg1CubeCells:
    print cubeCell
print ""

agg2CubeCells = cubify.getAggregatedCubeCells('purchasesCubeSet', 'agg2')
print ""
print "Cube cells in purchasesCubeSet's agg2 cube:"
for cubeCell in agg2CubeCells:
    print cubeCell
print ""


print "********* End Tutorial 2 **********"
