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

cubeRows = cubify.getSourceCubeRows('purchasesCubeSet')
binnedCubeRows = cubify.getBinnedCubeRows('purchasesCubeSet')
agg1CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet', 'agg1')
agg2CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet', 'agg2')

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

print ""
print "Cube rows in purchasesCubeSet's agg1 cube:"
for cubeRow in agg1CubeRows:
    print cubeRow
print ""

print ""
print "Cube rows in purchasesCubeSet's agg2 cube:"
for cubeRow in agg2CubeRows:
    print cubeRow
print ""

#
#  Add more rows to purchasesCubeSet's source cube
#
cubify.addRowsToSourceCube('purchasesCubeSet', 'morePurchases.csv')

binnedCubeRows = cubify.getBinnedCubeRows('purchasesCubeSet')
print ""
print "Cube rows in purchasesCubeSet's binned cube:"
for cubeRow in binnedCubeRows:
    print cubeRow
print ""

agg1CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet', 'agg1')
print ""
print "Cube rows in purchasesCubeSet's agg1 cube:"
for cubeRow in agg1CubeRows:
    print cubeRow
print ""

agg2CubeRows = cubify.getAggregatedCubeRows('purchasesCubeSet', 'agg2')
print ""
print "Cube rows in purchasesCubeSet's agg2 cube:"
for cubeRow in agg2CubeRows:
    print cubeRow
print ""


print "********* End Tutorial 2 **********"
