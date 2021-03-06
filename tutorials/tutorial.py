#
# Cubify Tutorial Part 1
#
# This tutorial shows you how to use Cubify to:
#
#   1. Create a cube 
#   2. Export a cube
#   3. Query cube rows
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
cubify.deleteCube('purchases_autobinned_1')
cubify.deleteCube('purchases_autobinned_2')
cubify.deleteCube('purchases_binned_1')
cubify.deleteCube('purchases_binned_2')
cubify.deleteCube('purchases_binned_2_CustomerId')
cubify.deleteCube('purchases_binned_2_CustomerState-ProductCategory')
cubify.deleteCube('purchases_binned_2_ProductId')
cubify.deleteCube('purchases_binned_2_CustomerId-TransactionDate')
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

# Create a cube from another cube
nyPurchasesCube = cubify.createCubeFromCube(cube, { 'dimensions.CustomerState' : 'NY' }, 'nyPurchases')
print ""
print "Dimensions in ny purchases cube:"
print nyPurchasesCube['distincts']
print ""
print "Measure Statistics in ny purchases cube:"
print nyPurchasesCube['stats']

#
# Section 2. Export a cube
#

# Export purchases cube to csv
cubify.exportCubeToCsv(cube, '/tmp/exported.csv')


#
# Section 3: Query a cube
#

# Get all cube rows from purchases cube
cubeRows = cubify.getCubeRows(cube)
print ""
print "Cube rows in purchases cube:"
for cubeRow in cubeRows:
    print cubeRow

# Query cube rows where ProductId = 'P2'
print ""
print "Cube rows where ProductId = 'P2':"
p2CubeRows = cubify.queryCubeRows(cube, { 'dimensions.ProductId' : 'P2' })
for p2CubeRow in p2CubeRows:
    print p2CubeRow

print ""
print "Cube rows where Price >  21"
cubeRows = cubify.queryCubeRows(cube, { 'measures.Price' : { '$gt' : 21 }})
for cubeRow in cubeRows:
    print cubeRow

print ""
print "Cube rows where Price >  21 and Product = 'P2'"
cubeRows = cubify.queryCubeRows(cube, { '$and' : [ { 'measures.Price' : { '$gt' : 21 }},  { 'dimensions.ProductId' : 'P2' } ]})
for cubeRow in cubeRows:
    print cubeRow


#
# Section 4: Adding columns to a cube
#

# Add a numeric column Revenue using an expression
print ""
print "Add a new numeric column called 'Revenue' to the purhases cube"
cubify.addColumn(cube, "Revenue", "numeric", "$['Price'] * $['Qty']")

# Verify that Revenue is now present in the cube rows
cubeRows = cubify.getCubeRows(cube)
print "Cube rows in purchases cube after adding Revenue:"
for cubeRow in cubeRows:
    print cubeRow

# Add a numeric column Discount using a function
def computeDiscount(cubeRow):
   if cubeRow['dimensions']['CustomerState'] == 'CA':
        return 3.5
   else:
        return 3.0
print ""
print "Add a new numeric column called 'Discount' to the purhases cube"
cubify.addColumn(cube, "Discount", "numeric", None, computeDiscount)

# Verify that Discount is now present in the cube rows
cubeRows = cubify.getCubeRows(cube)
print "Cube rows in purchases cube after adding Discount:"
for cubeRow in cubeRows:
    print cubeRow


# Add a string column, ProductCategory using an expression
print ""
print "Add a new string column called ProductCategory" 
cubify.addColumn(cube, 'ProductCategory', 'string', "'Category1' if $['ProductId'] == 'P1' else 'Category2'", None)

# Verify that ProductCategory is now present in the cube rows
cubeRows = cubify.getCubeRows(cube)
print "Cube rows in purchases cube after adding ProductCategory"
for cubeRow in cubeRows:
    print cubeRow

# Add a string column using a function
def computePackageSize(cubeRow):
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
cubify.addColumn(cube, 'PackageSize', 'string', None, computePackageSize)

# Verify that PackageSize is now present in the cube rows
cubeRows = cubify.getCubeRows(cube)
print "Cube rows in purchases cube after adding PackageSize"
for cubeRow in cubeRows:
    print cubeRow

#
# Section 5: Binning a cube
#

# Perform automatic binning on cube on all measures
binnedCube = cubify.binCube(cube, 'purchases_autobinned_1')
print ""
print "Dimensions in purchases_autobinned_1 cube:"
print binnedCube['distincts']

cubify.exportCubeToCsv(binnedCube, '/tmp/exportedAutoBinned1.csv')

# Perform automatic binning on cube on specific measures
binnedCube = cubify.binCube(cube, 'purchases_autobinned_2', ['TransactionDate','Qty','Price'], {'TransactionDate':'weekly'})
print ""
print "Dimensions in purchases_autobinned_2 cube:"
print binnedCube['distincts']

cubify.exportCubeToCsv(binnedCube, '/tmp/exportedAutoBinned2.csv')

# Perform custom Qty binning on cube
with open('qtyBinning.json') as qtyBinning_file:
    qtyBinning = json.load(qtyBinning_file)
binnedCube1 = cubify.binCubeCustom(qtyBinning, cube, 'purchases_binned_1')
print ""
print "Dimensions in purchased_binned_1 cube:"
print binnedCube1['distincts']

cubify.exportCubeToCsv(binnedCube1, '/tmp/exportedBinned1.csv')

# Perform custom Qty, Price, TransactionDate and CustomeState binnings on cube
with open('binnings.json') as binnings_file:
    binnings = json.load(binnings_file)
binnedCube2 = cubify.binCubeCustom(binnings, cube, 'purchases_binned_2')
print ""
print "Dimensions in purchased_binned_2 cube:"
print binnedCube2['distincts']

cubify.exportCubeToCsv(binnedCube2, '/tmp/exportedBinned2.csv')

#
# Section 6: Aggregating a cube
#

# Aggregate cube - simple example 1
#   Group-by dimensions are: ['CustomerId']
#   Use default cubify aggregation operators sum and average over all Qty and Price measures
aggCube = cubify.aggregateCube(binnedCube2, ['CustomerId'], ['Qty', 'Price'])
print ""
print "Cube rows of aggregated cube: " + aggCube['name']
aggCubeRows = cubify.getCubeRows(aggCube)
for aggCubeRow in aggCubeRows:
   print aggCubeRow
cubify.exportCubeToCsv(aggCube, '/tmp/aggregatedCubeByCustomerId.csv')

# Aggregate cube - simple example 2
#   Group-by dimensions are: ['CustomerState','ProductCategory']
#   Use cubify aggregation operators sum and average over all measures
aggCube = cubify.aggregateCube(binnedCube2, ['CustomerState', 'ProductCategory'])
print ""
print "Cube rows of aggregated cube: " + aggCube['name']
aggCubeRows = cubify.getCubeRows(aggCube)
for aggCubeRow in aggCubeRows:
   print aggCubeRow
cubify.exportCubeToCsv(aggCube, '/tmp/aggregatedCubeByCustomerStateAndProductCategory.csv')


# Aggregate cube - example 1 using custom aggregation definitions
with open('agg1.json') as agg_file:
    agg = json.load(agg_file)
aggCubes = cubify.aggregateCubeCustom(binnedCube2, agg)
aggCube = aggCubes[0]

print ""
print "Cube rows of aggregated cube: " + aggCube['name']
aggCubeRows = cubify.getCubeRows(aggCube)
for aggCubeRow in aggCubeRows:
   print aggCubeRow

cubify.exportCubeToCsv(aggCube, '/tmp/aggCube1.csv')

# Aggregate cube - example 2 using custom aggregation definitions
with open('agg2.json') as agg_file:
    agg = json.load(agg_file)
aggCubes = cubify.aggregateCubeCustom(binnedCube2, agg)
aggCube = aggCubes[0]

print ""
print "Cube rows of aggregated cube: " + aggCube['name']
aggCubeRows = cubify.getCubeRows(aggCube)
for aggCubeRow in aggCubeRows:
   print aggCubeRow

cubify.exportCubeToCsv(aggCube, '/tmp/aggCube2.csv')


print ""
print "********* End Tutorial **********"
