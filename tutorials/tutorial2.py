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
cubify.deleteCubeSet('purchasesCubeSet2')

# Create a cube set called 'purchasesCubeSet' (with automatic binning and no aggregations)
cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet', 'Purchases Cube Set', 'purchases.csv')

print ""
print "CubeSet purchasesCubeSet created successfully"
print ""

cubeRows = cubify.getSourceCubeRows('purchasesCubeSet')
binnedCubeRows = cubify.getBinnedCubeRows('purchasesCubeSet')

print ""
print "Cube rows in purchasesCubeSet's source cube:"
for cubeRow in cubeRows:
    print cubeRow
print ""

print ""
print "Cube rows in purchasesCubeSet's binned cube:"
for cubeRow in binnedCubeRows:
    print cubeRow
print ""


# Create a cube set called 'purchasesCubeSet2' with custom binnings defined in binning.json and aggregations defined in aggs.json

with open('binnings.json') as binnings_file:
    binnings = json.load(binnings_file)

with open('aggs.json') as aggs_file:
    aggs = json.load(aggs_file)

cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet2', 'Purchases Cube Set 2', 'purchases.csv', binnings, aggs)

print ""
print "CubeSet purchasesCubeSet2 created successfully"
print ""

cubeRows = cubify.getSourceCubeRows('purchasesCubeSet2')
binnedCubeRows = cubify.getBinnedCubeRows('purchasesCubeSet2')
agg1CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet2', 'agg1')
agg2CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet2', 'agg2')

print ""
print "Cube rows in purchasesCubeSet2's source cube:"
for cubeRow in cubeRows:
    print cubeRow
print ""

print ""
print "Cube rows in purchasesCubeSet2's binned cube:"
for cubeRow in binnedCubeRows:
    print cubeRow
print ""

print ""
print "Cube rows in purchasesCubeSet2's agg1 cube:"
for cubeRow in agg1CubeRows:
    print cubeRow
print ""

print ""
print "Cube rows in purchasesCubeSet2's agg2 cube:"
for cubeRow in agg2CubeRows:
    print cubeRow
print ""

#
#  Add more rows to purchasesCubeSet2's source cube
#
cubify.addRowsToSourceCube('purchasesCubeSet2', 'morePurchases.csv')

binnedCubeRows = cubify.getBinnedCubeRows('purchasesCubeSet2')
print ""
print "Cube rows in purchasesCubeSet2's binned cube:"
for cubeRow in binnedCubeRows:
    print cubeRow
print ""

agg1CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet2', 'agg1')
print ""
print "Cube rows in purchasesCubeSet2's agg1 cube:"
for cubeRow in agg1CubeRows:
    print cubeRow
print ""

agg2CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet2', 'agg2')
print ""
print "Cube rows in purchasesCubeSet2's agg2 cube:"
for cubeRow in agg2CubeRows:
    print cubeRow
print ""


print "********* End Tutorial 2 **********"
