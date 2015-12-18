#
# Cubify Tutorial Part 1
#
# This tutorial shows you how to use Cubify to:
#
#   1. Create a cube 
#   2. Export a cube
#   3. Query cube cells
#   4. Add columns to a cube 
#   5. Bin a cube
#   6. Aggregate a cube
#

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

#
# Section 1: Create a cube
#

# Create a cube called 'purchases'
cube = cubify.createCubeFromCsv('purchases.csv', 'purchases')
print ""
print "Dimensions in purchase cube:"
print cube['distincts']
print ""
print "Measure Statistics in purchase cube:"
print cube['stats']

#
# Section 2. Export a cube
#

# Export purchases cube to csv
cubify.exportCubeToCsv('purchases', '/tmp/exported.csv')


#
# Section 3: Query a cube
#

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

print ""
print "Cube cells where Price >  21"
cubeCells = cubify.queryCubeCells('purchases', { 'measures.Price' : { '$gt' : 21 }})
for cubeCell in cubeCells:
    print cubeCell

print ""
print "Cube cells where Price >  21 and Product = 'P2'"
cubeCells = cubify.queryCubeCells('purchases', { '$and' : [ { 'measures.Price' : { '$gt' : 21 }},  { 'dimensions.ProductId' : 'P2' } ]})
for cubeCell in cubeCells:
    print cubeCell


#
# Section 4: Adding columns to a cube
#

# Add a numeric column Revenue using an expression
print ""
print "Add a new numeric column called 'Revenue' to the purhases cube"
cubify.addColumn("purchases", "Revenue", "numeric", "$['Price'] * $['Qty']")

# Verify that Revenue is now present in the cube cells
cubeCells = cubify.getCubeCells('purchases')
print "Cube cells in purchases cube after adding Revenue:"
for cubeCell in cubeCells:
    print cubeCell

# Add a numeric column Discount using a function
def computeDiscount(cubeCell):
   if cubeCell['dimensions']['CustomerState'] == 'CA':
        return 3.5
   else:
        return 3.0
print ""
print "Add a new numeric column called 'Discount' to the purhases cube"
cubify.addColumn("purchases", "Discount", "numeric", None, computeDiscount)

# Verify that Discount is now present in the cube cells
cubeCells = cubify.getCubeCells('purchases')
print "Cube cells in purchases cube after adding Discount:"
for cubeCell in cubeCells:
    print cubeCell


# Add a string column, ProductCategory using an expression
print ""
print "Add a new string column called ProductCategory" 
cubify.addColumn('purchases', 'ProductCategory', 'string', "'Category1' if $['ProductId'] == 'P1' else 'Category2'", None)

# Verify that ProductCategory is now present in the cube cells
cubeCells = cubify.getCubeCells('purchases')
print "Cube cells in purchases cube after adding ProductCategory"
for cubeCell in cubeCells:
    print cubeCell

# Add a string column using a function
def computePackageSize(cubeCell):
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
cubify.addColumn('purchases', 'PackageSize', 'string', None, computePackageSize)

# Verify that PackageSize is now present in the cube cells
cubeCells = cubify.getCubeCells('purchases')
print "Cube cells in purchases cube after adding PackageSize"
for cubeCell in cubeCells:
    print cubeCell

#
# Section 5: Binning a cube
#

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

#
# Section 6: Aggregating a cube
#

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
