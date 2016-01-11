<h1>Cubify - "Data Cubes On Steroids" </h1>

Cubify is a tool written in python for data analysts who require "data agility". If you need to experiment with different ways of binning data, and viewing data at various levels of granularity Cubify is the tool for you! Cubify allows you to easily create a data cube from CSV and then to transform the cube into other cubes by binning and aggregation.  So what is a data cube? It is simply a structure that organizes data into dimensions and measures, concepts used in data warehouse systems.

Installation
------------

    If you don't have Python installed, go get Python at: https://www.python.org/downloads/
    If you don't have MongoDB installed, go get MongoDB at: https://docs.mongodb.org/manual/installation/
    Make sure MongoDB is running and listening on default port 27017 to run the tutorials.

**Note: You must have MongoDB running to use Cubify.**

Cubify Manual Installation:

    git clone https://github.com/rtansf/cubify.git
    cd cubify
    python setup.py install
 
Cubify Install from docker:  
   
    Coming soon...

Tutorial 
--------

Go to the tutorials folder in the cubify installation, open __tutorial.py__ and follow along with the tutorial commentary below.
You can run the tutorial, by typing: 

    python tutorial.py

In this tutorial you will learn how to:

   1. Create a cube 
   2. Export a cube
   3. Query cube cells
   4. Add columns to a cube 
   5. Bin a cube
   6. Aggregate a cube

So let's get started. cd to the tutorials folder.
In the folder, the file purchases.csv contains a simple dataset containing customer purchase transactions.
Our simple data set contains the following columns:

    Transaction Date  
    Customer ID
    Customer State
    Product ID
    Quantity Purchased
    Unit Price

The contents of the file, purchases.csv are shown below:

    TransactionDate,CustomerId,CustomerState,ProductId,Qty,Price
    2015-10-10,C1,CA,P1,3,20.5
    2015-10-10,C1,CA,P1,3,20.5
    2015-10-10,C1,CA,P2,1,15.5
    2015-10-10,C2,NY,P1,2,20.0
    2015-10-10,C2,NY,P2,4,16.0
    2015-10-11,C2,NY,P1,2,19.5
    2015-10-11,C3,MA,P1,7,18.5
    2015-11-03,C1,CA,P1,3,21.5
    2015-11-10,C1,CA,P1,3,22.0
    2015-11-12,C1,CA,P2,1,22.0
    2015-11-12,C2,NY,P1,2,22.0
    2015-11-13,C2,NY,P2,4,17.0
    2015-11-13,C2,NY,P1,2,22.0
    2015-11-13,C3,MA,P1,7,20.0

1. Creating a cube
------------------
Let's create a cube from our csv file. We we call our cube, "purchases".
First, we create an instance of Cubify and invoke createCubeFromCsv passing in the csv file path and the cube name.

    cubify = Cubify()
    cube = cubify.createCubeFromCsv('purchases.csv', 'purchases')

This returns a new cube containing cube cells. A cube cell is a "row" in a cube; it contains dimensions, and measures.
In our purchases example, dimensions are the string and date columns: TransactionDate, CustomerId, CustomerState and ProductId.
Measures are the numeric columns: Price and Qty.

So the first row of our CSV file becomes the following cube cell:

    {
        "dates" : {
            "TransactionDate" : "2014-10-10"
        },
        "dimensions" : {
            "CustomerId" : "C1",
	    "CustomerState" : "CA",
            "ProductId" : "P1"	
        },
        "measures" : {
            "Qty" : 3.0,
            "Price" : 20.5
        },
        "dimensionKey" : "#CustomerId:C1#ProductId:P1#State:CA#TransactionDate:2014-10-10"
    }

Now, we can examine the properties of the cube we have just created. Some interesting ones are: 'distincts' and 'stats'.
The 'distincts' property shows a list of all dimensions in the cube with their distinct values with the number of rows that contain the value. 

    print cube['distincts']

    {'CustomerState': {
        'NY': 6, 
        'CA': 6, 
        'MA': 2 }, 
     'TransactionDate': {
        '2015-10-10': 5, 
        '2015-10-11': 2, 
        '2015-11-03': 1, 
        '2015-11-10': 1, 
        '2015-11-13': 3, 
        '2015-11-12': 2 }, 
     'CustomerId': {
        'C3': 2, 
        'C2': 6, 
        'C1': 6 }, 
     'ProductId': {
        'P2': 4, 
        'P1': 10 }
    }

The 'stats' property shows a list of measures in the cube with accompanying statistics, such as standard deviation, median, mean, min, max and total.

    print cube['stats']

    {
      'Price': {'std': 2.1688894929555684, 
              'min': 15.5,
              'max': 22.0, 
              'median': 20.25,
              'total': 277.0, 
              'mean':  19.785714285714285},
 
      'Qty':  {'std': 1.8070158058105026, 
              'min': 1.0, 
              'max': 7.0, 
              'median': 3.0, 
              'total': 44.0, 
              'mean': 3.142857142857143 }
    }

You can also create a cube from another cube by applying a filter to the source cube.
The method is called "createCubeFromCube". For example to create a cube containing only cube cells where the CustomerState is "NY" from the 'purchases'
cube above we can call the method like so:

    cubify.createCubeFromCube('purchases', { 'dimensions.CustomerState' : 'NY' }, 'nyPurchases')

For more details about filters, refer to the Cubify Reference documentation.

2. Exporting a cube
-------------------
You can export the cube to a csv file by calling exportCubeToCsv. For example,

    cubify.exportCubeToCsv('purchases', '/tmp/exported.csv')

3. Querying cube cells
-----------------------
You can get cube cells from a cube by calling getCubeCells like so:

    cubeCells = cubify.getCubeCells('purchases')
    for cubeCell in cubeCells:
        print cubeCell

A cube cell has the following basic properties:
    
    dimensionKey - a string of the form: #<dimensionName>:<dimensionValue>[...] It serves as a convenient way to name and reference a cube cell.
    dates - the dimension values for date types 
    dimensions - the dimension values for string types
    measures - the measure values of numeric type
    
You can query cube cells using filters. 
For example to get all cube cells where ProductId = 'P2', do the following:

    p2CubeCells = cubify.queryCubeCells('purchases', { 'dimensions.ProductId' : 'P2' })

Other query examples:
    
    cubeCells = cubify.queryCubeCells('purchases', { 'measures.Price' : { '$gt' : 21 })
    cubeCells = cubify.queryCubeCells('purchases', { '$and' : [ { 'measures.Price' : { '$gt' : 21 }},  { 'dimensions.ProductId' : 'P2' } ]})
    

For details about query syntax, refer to the Cubify Reference documentation.

4. Adding columns
-----------------

You can add new numeric and string columns to a cube using an expression or a function.
For example, to add a numeric column called "Revenue" using the expression Price * Qty, you can call:

    cubify.addColumn("purchases", "Revenue", "numeric", "$['Price'] * $['Qty']", None)

If we want to add a new numeric column called "Discount" using a function called "computeDiscount", first define the function as in the example below.
Note that the function must have one argument which will hold the cubeCell.

    def computeDiscount(cubeCell):
       if cubeCell['dimensions']['CustomerState'] == 'CA':
           return 3.5
       else:
           return 3.0
    
Then call addColumn as in the example below:

    cubify.addColumn("purchases", "Discount", "numeric", None, computeDiscount)

The following example shows you how to add a new string column.
Say you want to add a new column called "ProductCategory" whose value should be "Category1" if the Product is "P1" and "Category2" for all other products.
We can use an expression as in the following example:

    cubify.addColumn('purchases', 'ProductCategory', 'string', "'Category1' if $['ProductId'] == 'P1' else 'Category2'", None)
    
Let's say we want to add another string column, PackageSize which will use a computePackageSize function to return the values 'SMALL' or 'LARGE'.
First define the function,

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

Then call addColumn as in:

    cubify.addColumn('purchases', 'PackageSize', 'string', None, computePackageSize)

   
5. Binning a cube
-----------------
Next, we will bin the measures and dimensions in our purchases cube. This is done with cubify's binning DSL which allows you specify defintions for a binning.
For example, let's say we would like to bin the Qty measure in our cube. We define for simplicity's sake, two bins:  0-5 and 5+
The binning definition for the Qty column (stored in the file qtyBinning.json) is show below:

    [{ "binningName" : "QtyBinning",
       "outputField" : { "name": "QtyBin", "displayName": "Qty Bin" },
       "sourceField" : "Qty",
       "type" : "range",
       "bins" : [
           { "label" : "0-5",         "min": 0,       "max": 5 },
           { "label" : "5+",          "min": 6,       "max": 999999999 }
       ],
       "fallbackLabel" : "None"
    }]

In the definition above, we define the sourceField to be "Qty" and the outputField to be "QtyBin". The outputField will become a new dimension in the binned cube.
Since, we are binning a continuous measure, the type is set to "range". The fallbackLabel is applied to a value that falls outside of our binning ranges. Let's apply the above binning defintion above to our purchases cube to produce a new cube called "purchases_binned_1".

    with open('qtyBinning.json') as binnings_file:
         binnings = json.load(binnings_file)
    binnedCube1 = cubify.binCube(binnings, 'purchases', 'purchases_binned_1')

Now when you list the distinct dimension values of binnedCube1, you will see that a new dimension, QtyBin appears in the list with 12 occurrences of 0-5 bin and 2 occurrences of the 5+ bin.:

    print binnedCube1['distincts']

    'QtyBin': {'0-5': 12, '5+': 2}  ....

Note that after binning, the number of cube cells in the cube do not change. We still have a total of 14 cells in our binned cube. 

If you export purchases_binned_1 to csv, you should see a new column, QtyBin.

    cubify.exportCubeToCsv('purchases_binned_1', '/tmp/exportedBinned1.csv')

    S:CustomerId,S:CustomerState,S:ProductId,S:QtyBin,D:TransactionDate,N:Price,N:Qty
    C1,CA,P1,0-5,2015-10-10 00:00:00,20.5,3.0
    C1,CA,P1,0-5,2015-10-10 00:00:00,20.5,3.0
    C1,CA,P2,0-5,2015-10-10 00:00:00,15.5,1.0
    C2,NY,P1,0-5,2015-10-10 00:00:00,20.0,2.0
    C2,NY,P2,0-5,2015-10-10 00:00:00,16.0,4.0
    C2,NY,P1,0-5,2015-10-11 00:00:00,19.5,2.0
    C3,MA,P1,5+,2015-10-11 00:00:00,18.5,7.0
    C1,CA,P1,0-5,2015-11-03 00:00:00,21.5,3.0
    C1,CA,P1,0-5,2015-11-10 00:00:00,22.0,3.0
    C1,CA,P2,0-5,2015-11-12 00:00:00,22.0,1.0
    C2,NY,P1,0-5,2015-11-12 00:00:00,22.0,2.0
    C2,NY,P2,0-5,2015-11-13 00:00:00,17.0,4.0
    C2,NY,P1,0-5,2015-11-13 00:00:00,22.0,2.0
    C3,MA,P1,5+,2015-11-13 00:00:00,20.0,7.0

Note that in the exported csv, the column names are prefixed with the data type of the column, S: for string, D: for date and N for numeric type.
     
Now look at the file binnings.json. It defines 4 different binnings, QtyBinning (the one discussed above),PriceBinning, TransactionDateBinning, and CustomerStateBinning.

Take a look at TransactionDateBinning. It is a binning of type "date".

    [{ "binningName" : "TransctionDateBinning",
       "outputField" : { "name": "YearMonth", "displayName": "YearMonth" },
       "sourceField" : "TransactionDate",
       "type" : "date",
       "bins" : [
          { "label" : "Sep-2015", "min" : "2015-09-01", "max" : "2015-09-30" },
          { "label" : "Oct-2015", "min" : "2015-10-01", "max" : "2015-10-31" },
          { "label" : "Nov-2015", "min" : "2015-11-01", "max" : "2015-11-30" },
          { "label" : "Dec-2015", "min" : "2015-12-01", "max" : "2015-12-31" }
       ],
       "fallbackLabel" : "Other"
    }]      	  

A date binning is similar to range binning except that the data type of the dimension being binned is of type date. The min and max ranges must be dates in the format "YYYY-mm-dd". In our example above, we are defining the TransactionDate field is being binned into monthly bins in the YearMonth field. 

As a shortcut, the above binning definition for monthly bins can be omitted and replaced with a "period" property like so:

    [{ "binningName" : "TransctionDateBinning",
       "outputField" : { "name": "YearMonth", "displayName": "YearMonth" },
       "sourceField" : "TransactionDate",
       "type" : "date",
       "period" : "monthly", 
       "fallbackLabel" : "Other"
    }]      	  

If you want to bin the Transaction dates into weekly bins, then simply specify "period" as "weekly". 

The third binning type is "enum". Take a look at the RegionBinning definition from the binnings.json file.

    [{ "binningName" : "RegionBinning",
       "outputField" : { "name": "Region", "displayName": "Region" },
       "sourceField" : "CustomerState",
       "type" : "enum",
       "bins" : [
          { "label" : "West",   "value" : "CA" },
          { "label" : "West",   "value" : "WA" },
          { "label" : "Mountain",  "value" : "ID" },
          { "label" : "SouthWest", "value" : "NM" },
          { "label" : "MidWest", "value" : "ND" },
          { "label" : "MidWest", "value" : "SD" },
          { "label" : "NorthEast", "value" : "ME" },
          { "label" : "NorthEast", "value" : "NY" },
          { "label" : "South", "value" : "GA" },
          { "label" : "South", "value" : "LA" }
       ],
       "fallbackLabel" : "Other"
    }]

There are no ranges in enum binnings; we simply apply a bin label to a value. In our example, we are binning the CustomerState field to produce the Region field. 

Now, let's apply binnings.json to our purchases cube, as in the code snippet below.

    with open('binnings.json') as binnings_file:
        binnings = json.load(binnings_file)
    binnedCube2 = cubify.binCube(binnings, 'purchases', 'purchases_binned_2')

Now when you export the binned cube, you will see the new columns, QtyBin, PriceBin, YearMonth and Region.

    cubify.exportCubeToCsv('purchases_binned_2', '/tmp/exportedBinned2.csv')

The binned cube's contents:

    S:CustomerId,S:CustomerState,S:PriceBin,S:ProductId,S:QtyBin,S:Region,S:YearMonth,D:TransactionDate,N:Price,N:Qty
    C1,CA,10+,P1,0-5,West,Oct-2015,2015-10-10 00:00:00,20.5,3.0
    C1,CA,10+,P1,0-5,West,Oct-2015,2015-10-10 00:00:00,20.5,3.0
    C1,CA,10+,P2,0-5,West,Oct-2015,2015-10-10 00:00:00,15.5,1.0
    C2,NY,10+,P1,0-5,NorthEast,Oct-2015,2015-10-10 00:00:00,20.0,2.0
    C2,NY,10+,P2,0-5,NorthEast,Oct-2015,2015-10-10 00:00:00,16.0,4.0
    C2,NY,10+,P1,0-5,NorthEast,Oct-2015,2015-10-11 00:00:00,19.5,2.0
    C3,MA,10+,P1,5+,Other,Oct-2015,2015-10-11 00:00:00,18.5,7.0
    C1,CA,10+,P1,0-5,West,Nov-2015,2015-11-03 00:00:00,21.5,3.0
    C1,CA,10+,P1,0-5,West,Nov-2015,2015-11-10 00:00:00,22.0,3.0
    C1,CA,10+,P2,0-5,West,Nov-2015,2015-11-12 00:00:00,22.0,1.0
    C2,NY,10+,P1,0-5,NorthEast,Nov-2015,2015-11-12 00:00:00,22.0,2.0
    C2,NY,10+,P2,0-5,NorthEast,Nov-2015,2015-11-13 00:00:00,17.0,4.0
    C2,NY,10+,P1,0-5,NorthEast,Nov-2015,2015-11-13 00:00:00,22.0,2.0
    C3,MA,10+,P1,5+,Other,Nov-2015,2015-11-13 00:00:00,20.0,7.0


6. Aggregating a cube
---------------------

Next, let's aggregate the cube, binnedCube2 we created above. This is done with cubify's aggregation DSL.
For example, let's say we want a cube containing the average price grouped by product and region.  
The aggregation definition (in the file agg1.json) is show below:

    [{
       "name" : "agg1",
       "dimensions": ["ProductId", "Region"],
       "measures" : [
           { "outputField" : {"name":"AveragePrice", "displayName": "Average Price"},
             "formula" : { "numerator" : "{ '$avg': '$measures.Price' }", "denominator" : "" }
           }
       ]
    }]

In the definition above, we specify our group-by dimensions, ProductId and Region. Then we define one or more measures which we wish to aggregate - in this case we are only aggregating one measure, Price.
In the outputField, we specify "AveragePrice" as the name of the new column which will hold the aggregated values. Then we define the aggregation formula.
An aggregation formula contains a numerator and denominator. For now, in our simple example, we will only define the numerator as taking the "$avg" operator and applying it to "$measures.Price".  The denominator is used for more complex aggregations such as computing weighted average as we shall see later. (For details of the formula syntax, refer to the Cubify Reference).

Now let's load the aggregation DSL file, agg1.json and call the aggregateCube method to aggregate binnedCube2.

    with open('agg1.json') as agg_file:
        agg = json.load(agg_file)
    aggCubes = cubify.aggregateCube('purchases_binned_2', agg)
    aggCube = aggCubes[0]

The aggregateCube method returns a list of aggregated cubes, but since we only have one aggregation definition in our DSL, the returned list contains one result cube.
The name of our aggregated cube is 'purchases_binned_2_agg1' - the name is automatically generated by Cubify; it is always the name of the cube we are aggregating concatenated with the name of the aggregation definition.

Now when we list the cube cells of our aggregated cube you will see the aggregated cube contains the following: 

    { ..., 'dimensions': {'Region': 'Other', 'ProductId': 'P1'}, 'measures': {'AveragePrice': 19.25}, ...}
    { ..., 'dimensions': {'Region': 'NorthEast', 'ProductId': 'P2'}, 'measures': {'AveragePrice': 16.5}, ...}
    { ..., 'dimensions': {'Region': 'NorthEast', 'ProductId': 'P1'}, 'measures': {'AveragePrice': 20.875}, ...}
    { ..., 'dimensions': {'Region': 'West', 'ProductId': 'P2'}, 'measures': {'AveragePrice': 18.75} ... }
    { ..., 'dimensions': {'Region': 'West', 'ProductId': 'P1'}, 'measures': {'AveragePrice': 21.125} ... }
 
And when we export the cube, the CVS file contains the following:

    S:ProductId,S:Region,N:AveragePrice
    P1,Other,19.25
    P2,NorthEast,16.5
    P1,NorthEast,20.875
    P2,West,18.75
    P1,West,21.125

Now let's apply a more complex aggregation to our binnedCube2. Take a look at agg2.json. 

    [{
       "name" : "agg2",
       "dimensions": ["ProductId"],
       "measures" : [
           { "outputField" : {"name":"TotalQty", "displayName": "Total Qty"},
             "formula" : {"numerator" : "{ '$sum': '$measures.Qty' }", "denominator" : "" }
           },
           { "outputField" : {"name":"AverageRevenue", "displayName": "Average Revenue"},
             "formula" : { "numerator"   : "{ '$sum': { '$multiply': ['$measures.Qty', '$measures.Price'] }}", 
                           "denominator" : "{ '$sum': '$measures.Qty' }" }
           }
       ]
    }]

Our aggregation definition is called "agg2". Here we are grouping by one dimension, ProductId and generating two new columns, TotalQty and AverageRevenue.
This will create a cube which shows the total quantity and average revenue per product.
The formula for TotalQty is self-explanatory. The formula for AverageRevenue uses both numerator and denominator. In the numerator we take the sum of Qty multiplied by Price. In the denominator, we sum Qty. The average revenue is therefore simply the numerator divided by the denominator. Average revenue is an example of a weighted average where we are getting the average price using quantity as a weight.

Now let's load the aggregation DSL file, agg2.json and call the aggregateCube method to aggregate binnedCube2.

    with open('agg2.json') as agg_file:
        agg = json.load(agg_file)
    aggCubes = cubify.aggregateCube('purchases_binned_2', agg)
    aggCube = aggCubes[0]

Now when we list the cube cells for the aggregated cube we get our two new measures, AverageRevenue and TotalQty and cube now contains only 2 cells, one per product.

    {..., 'dimensions': {'ProductId': 'P2'}, 'measures': {'AverageRevenue': 16.95, 'TotalQty': 10.0}, ...}
    {..., 'dimensions': {'ProductId': 'P1'}, 'measures': {'AverageRevenue': 20.294117647058822, 'TotalQty': 34.0}, ...}

And the exported CSV file looks like so:

    S:ProductId,N:AverageRevenue,N:TotalQty
    P2,16.95,10.0
    P1,20.294117647058822,34.0

We have come to end of the first part of our tutorial. 

Part 2 of the tutorial covers the even more powerful concept of "Cube Sets". With minimal effort, a cube set allows you to automatically bin and aggregate your data and thus maintain the different views of your data as it grows and changes over time.

Cube Set Tutorial
-----------------

We have seen the binning definitions (binnings.json)  and aggregation definitions (aggs.json) in tutorial 1. We will now use these definitions together with the source data defined in purchases.csv to create our cube set, called "purchasesCubeSet". 

Open the file, "__tutorials2.py__" in the tutorials folder and follow along with the commentary below. You can execute the tutorial by typing:
   
    python tutorial2.py

Let's create our cube set using the createCubeSet method in cubify like so:

    with open('binnings.json') as binnings_file:
        binnings = json.load(binnings_file)

    with open('aggs.json') as aggs_file:
        aggs = json.load(aggs_file)

    cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet', 'Purchases Cube Set', 'purchases.csv', binnings, aggs)
     
The first argument to createCubeSet is the owner of the cube set. This can be any string. In our example, the owner of the cube set is "tutorial".
The second argument is the name of the cube set. 
The third argument is the display name of the cube set.
The fourth argument is the name of the CSV file that contains our data.
The fifth argument is our binnings definition.
The sixth argument is our aggregations definition.

A cube set is essentially a container for cubes of certain types which are linked together. 
There are 3 types of cubes in a cube set: "source", "binned" and "aggregated".
A cube set consists of one and only one source cube, one and only one binned cube, and one or more aggregated cubes.

In our tutorial, once createCubeSet returns successfully, our purchasesCubeSet contains a "source" cube reflecting the original data imported from purchases.csv.
To get the source cube cells, call getSourceCubeCells like so:

    cubeCells = cubify.getSourceCubeCells('purchasesCubeSet')
 
Our cube set also contains a binned cube - with the binnings defined in binnings.json applied to the source cube.
To get the binned cube cells, call getBinnedCubeCells like so:

    binnedCubeCells = cubify.getBinnedCubeCells('purchasesCubeSet')

Finally our cube set also contains 2 aggregated cubes (resulting from the 2 aggregation definitions defined in aggs.json).
To refresh your memory, here is aggs.json:

    [
        {
           "name" : "agg1",
           "dimensions": ["ProductId", "Region"],
           "measures" : [
               { "outputField" : {"name":"AveragePrice", "displayName": "Average Price"},
                 "formula" : { "numerator" : "{ '$avg': '$measures.Price' }", "denominator" : "" }
               }
           ]
        },
        {
           "name" : "agg2",
           "dimensions": ["ProductId"],
           "measures" : [
               { "outputField" : {"name":"TotalQty", "displayName": "Total Qty"},
                 "formula" : {"numerator" : "{ '$sum': '$measures.Qty' }", "denominator" : "" }
               },
               { "outputField" : {"name":"AverageRevenue", "displayName": "Average Revenue"},
                 "formula" : { "numerator"   : "{ '$sum': { '$multiply': ['$measures.Qty', '$measures.Price'] }}", 
                               "denominator" : "{ '$sum': '$measures.Qty' }" }
               }
           ]
        }
    ]

You can retrieve the cube cells of the aggregated cubes by referring to the aggregation names like so:

    agg1CubeCells = cubify.getAggregatedCubeCells('purchasesCubeSet', 'agg1')
    agg2CubeCells = cubify.getAggregatedCubeCells('purchasesCubeSet', 'agg2')

All is well with our cube set thus far. But now let's say we are in a new month and we have more purchases data to add to our cube set.
Let's assume the new purchases data is in a file called "morePurchases.csv". To add the cells to our source cube in our cube set, simply call:

    cubify.addCellsToSourceCube('purchasesCubeSet', 'morePurchases.csv')

This method adds the new cells to our source cube, as well as updates the binned cube and aggregated cubes. Thus our binned and aggregated cubes are kept in synch with 
the data in our source cube. You can verify that this is so by examining the cells of the binned cube and aggregated cubes.







  
 


 
  







   




 

