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

# Create a cube set called 'purchasesCubeSet' (with automatic binning)
cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet', 'purchases.csv')

print ""
print "CubeSet purchasesCubeSet created successfully"
print ""

cubeRows = cubify.getSourceCubeRows(cubeSet)
binnedCubeRows = cubify.getBinnedCubeRows(cubeSet)

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

# Export the binned cube
cubify.exportBinnedCubeToCsv(cubeSet, '/tmp/purchasesCubeSetBinnedCube.csv')

# Now lets aggregate the cube set with the following dimensions ['CustomerState', 'ProductId']
cubify.performAggregation(cubeSet, ['CustomerState', 'ProductId'])

agg1CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'CustomerState-ProductId')
print ""
print "Cube rows in purchasesCubeSet's cube aggregated by CustomerState-ProductId:"
for cubeRow in agg1CubeRows:
    print cubeRow
print ""

agg2CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'CustomerState')
print ""
print "Cube rows in purchasesCubeSet's cube aggregated by CustomerState"
for cubeRow in agg2CubeRows:
    print cubeRow
print ""

# Export the aggregated cubes above
cubify.exportAggCubesToCsv(cubeSet, '/tmp')

# Create a cube set called 'purchasesCubeSet2' with custom binnings defined in binning.json and aggregations defined in aggs.json

with open('binnings.json') as binnings_file:
    binnings = json.load(binnings_file)

with open('aggs.json') as aggs_file:
    aggs = json.load(aggs_file)

cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet2', 'purchases.csv', binnings, aggs)

print ""
print "CubeSet purchasesCubeSet2 created successfully"
print ""

cubeRows = cubify.getSourceCubeRows(cubeSet)
binnedCubeRows = cubify.getBinnedCubeRows(cubeSet)
agg1CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'agg1')
agg2CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'agg2')

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
cubify.addRowsToSourceCube(cubeSet, 'morePurchases.csv')

binnedCubeRows = cubify.getBinnedCubeRows(cubeSet)
print ""
print "Cube rows in purchasesCubeSet2's binned cube:"
for cubeRow in binnedCubeRows:
    print cubeRow
print ""

agg1CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'agg1')
print ""
print "Cube rows in purchasesCubeSet2's agg1 cube:"
for cubeRow in agg1CubeRows:
    print cubeRow
print ""

agg2CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'agg2')
print ""
print "Cube rows in purchasesCubeSet2's agg2 cube:"
for cubeRow in agg2CubeRows:
    print cubeRow
print ""


print "********* End Tutorial 2 **********"
