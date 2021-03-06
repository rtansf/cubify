![alt text](http://pluralconcepts.com/images/rubik-cube.jpg "Cubify")

<h1>Cubify - "Data Cubes On Steroids" </h1>

Cubify is a tool written in python for data analysts who require "data agility". If you need to experiment with different ways of binning data, and viewing data at various levels of granularity, Cubify is for you! With Cubify, you can easily create a data cube from CSV and then transform the cube into other cubes by binning and aggregation. With just a few calls to the Cubify API, you will start gaining insights into your data. 

So what is a data cube? It is simply a structure that organizes data into dimensions and measures, concepts used in data warehouse systems.

Installation
------------

__PRE-REQUISITES__:

    If you are installing manually and not using Docker - you'll need to have the following installed:
        Python  https://www.python.org/downloads/
        MongoDB at: https://docs.mongodb.org/manual/installation/
    Note: You must have MongoDB up (on listening on the default port, 27107) to run the Cubify tutorials below.
    
__UBUNTU__:
    
    sudo apt-get install python-setuptools
    sudo apt-get install build-essential 
    sudo apt-get install python-dev
    sudo apt-get install python-numpy
    git clone https://github.com/rtansf/cubify.git
    cd cubify
    sudo python setup.py install

__MAC OS X__:

    git clone https://github.com/rtansf/cubify.git
    cd cubify
    sudo python setup.py install

__WINDOWS__:

    git clone https://github.com/rtansf/cubify.git
    cd cubify
    python setup.py install
 
__DOCKER__:
   
    docker run -it rtansf/cubify
    service mongod start
    cd cubify    

Tutorial 
--------

Go to the tutorials folder in the cubify installation, open __tutorial.py__ and follow along with the tutorial commentary below.
You can run the tutorial, by typing: 

    python tutorial.py

In this tutorial you will learn how to:

   1. Create a cube 
   2. Export a cube
   3. Query cube rows
   4. Add columns to a cube 
   5. Bin a cube
   6. Aggregate a cube

So let's get started. cd to the tutorials folder.
In the folder, the file purchases.csv contains a very simple dataset containing customer purchase transactions.
Our simple data set contains the following columns:

    Transaction Date  
    Customer ID
    Customer State
    Product ID
    Quantity Purchased
    Unit Price

The contents of the file, purchases.csv are shown below:

|TransactionDate|CustomerId|CustomerState|ProductId|Qty|Price|
|---------------|----------|-------------|---------|---|-----|
|2015-10-10|C1|CA|P1|3|20.5|
|2015-10-10|C1|CA|P1|3|20.5|
|2015-10-10|C1|CA|P2|1|15.5|
|2015-10-10|C2|NY|P1|2|20.0|
|2015-10-10|C2|NY|P2|4|16.0|
|2015-10-11|C2|NY|P1|2|19.5|
|2015-10-11|C3|MA|P1|7|18.5|
|2015-11-03|C1|CA|P1|3|21.5|
|2015-11-10|C1|CA|P1|3|22.0|
|2015-11-12|C1|CA|P2|1|22.0|
|2015-11-12|C2|NY|P1|2|22.0|
|2015-11-13|C2|NY|P2|4|17.0|
|2015-11-13|C2|NY|P1|2|22.0|
|2015-11-13|C3|MA|P1|7|20.0|

In the dataset above, the columns TransactionDate, CustomerId, CustomerState and ProductId are the **dimensions** and Qty and Price are the **measures**. 

1. Creating a cube
------------------

![alt text](http://pluralconcepts.com/images/SourceCube.png "Source Cube")

The diagram above shows that a cube can be created from raw data contained in a CSV file. Cubes created from raw data are termed "source cubes". 
Let's create a cube from our raw data contained in the CSV file. We we call our specific cube, "purchases".
First, we create an instance of Cubify and invoke createCubeFromCsv passing in the csv file path and the cube name.

    cubify = Cubify()
    cube = cubify.createCubeFromCsv('purchases.csv', 'purchases')

This returns a new cube containing cube rows. A cube row is simply a row in a cube, albeit a special row - it contains dimensions, and measures.
In our purchases example, dimensions are the string and date columns: TransactionDate, CustomerId, CustomerState and ProductId.
Measures are the numeric columns: Price and Qty.

So the first row of our CSV file becomes the following cube row:

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
The method is called "createCubeFromCube". For example to create a cube containing only cube rows where the CustomerState is "NY" from the 'purchases'
cube above we can call the method like so:

    nyPurchasesCube = cubify.createCubeFromCube(cube, { 'dimensions.CustomerState' : 'NY' }, 'nyPurchases')

The first argument is the name of the cube we are querying, followed by the filter, and then the name of the new cube.
The above will result in a cube called 'nyPurchases'.

For more details about filters, refer to the <a href="http://pluralconcepts.com/cubify_reference">Cubify Reference documentation.</a>

2. Exporting a cube
-------------------
You can export the cube to a csv file by calling exportCubeToCsv. For example,

    cubify.exportCubeToCsv(cube, '/tmp/exported.csv')

3. Querying cube rows
-----------------------
You can get cube rows from a cube by calling getCubeRows like so:

    cubeRows = cubify.getCubeRows(cube)
    for cubeRow in cubeRows:
        print cubeRow

A cube row has the following basic properties:
    
    dimensionKey - a string of the form: #<dimensionName>:<dimensionValue>[...] It serves as a convenient way to name and reference a cube row.
    dates - the dimension values for date types 
    dimensions - the dimension values for string types
    measures - the measure values of numeric type
    
You can query cube rows using filters. 
For example to get all cube rows where ProductId = 'P2', do the following:

    p2CubeRows = cubify.queryCubeRows(cube, { 'dimensions.ProductId' : 'P2' })

Other query examples:
    
    cubeRows = cubify.queryCubeRows(cube, { 'measures.Price' : { '$gt' : 21 })
    cubeRows = cubify.queryCubeRows(cube, { '$and' : [ { 'measures.Price' : { '$gt' : 21 }},  { 'dimensions.ProductId' : 'P2' } ]})
    

For details about query syntax, refer to the <a href="http://pluralconcepts.com/cubify_reference">Cubify Reference documentation.</a>

4. Adding columns
-----------------

You can add new numeric and string columns to a cube using an expression or a function.
For example, to add a numeric column called "Revenue" using the expression Price * Qty, you can call:

    cubify.addColumn(cube, "Revenue", "numeric", "$['Price'] * $['Qty']", None)

If we want to add a new numeric column called "Discount" using a function called "computeDiscount", first define the function as in the example below.
Note that the function must have one argument which will hold the cubeRow.

    def computeDiscount(cubeRow):
       if cubeRow['dimensions']['CustomerState'] == 'CA':
           return 3.5
       else:
           return 3.0
    
Then call addColumn as in the example below:

    cubify.addColumn(cube, "Discount", "numeric", None, computeDiscount)

The following example shows you how to add a new string column.
Say you want to add a new column called "ProductCategory" whose value should be "Category1" if the Product is "P1" and "Category2" for all other products.
We can use an expression as in the following example:

    cubify.addColumn(cube, 'ProductCategory', 'string', "'Category1' if $['ProductId'] == 'P1' else 'Category2'", None)
    
Let's say we want to add another string column, PackageSize which will use a computePackageSize function to return the values 'SMALL' or 'LARGE'.
First define the function,

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

Then call addColumn as in:

    cubify.addColumn(cube, 'PackageSize', 'string', None, computePackageSize)

   
5. Binning a cube
-----------------

![alt text](http://pluralconcepts.com/images/BinnedDataCube.png "Binned Cube")

From our source cube, we can create a "binned cube". The process of binning creates new dimensions in the cube.
For example in our purchases cube, we have a measure called "Price" which is a continuous numeric value. By binning this measure, we create a label for the range in which the price value falls. For example if the price falls between 1 and 10, the label is "1-10", and if it is between 10 and 20, the label is "10-20". These labels
become the values of a dimension called "PriceBin" in our cube.

Cubify can automatically bin a measure by introspecting the distribution of values occurring in that measure to determine the number of bins and the range of each bin.
It uses a variant of Sturge's algorithm for this. See https://en.wikipedia.org/wiki/Histogram

So continuing with our tutorial, we can instruct cubify to automatically bin all measures in our purchases cube to produce a new cube called "purchases_autobinned_1" like so:

    binnedCube = cubify.binCube(cube, 'purchases_autobinned_1')

If you examine the dimensions in binned cube, you will see new ones, "RevenueBin", "PriceBin", "QtyBin", "DiscountBin", "TransactionDateBin".

    print binnedCube['distincts']

    {'CustomerState': {'NY': 6, 'CA': 6, 'MA': 2}, 
     'RevenueBin': {'15-41': 4, '41-67': 7, '67-93': 1, '119-145': 2}, 
     'PriceBin': {'15-16': 2, '18-19': 1, '19-22': 10, '16-17': 1}, 
     'ProductCategory': {'Category1': 10, 'Category2': 4}, 
     'PackageSize': {'SMALL': 10, 'LARGE': 4}, 
     'DiscountBin': {'3-4': 6, '3-3': 8}, 
     'TransactionDateBin': {'201511': 7, '201510': 7}, 
     'QtyBin': {'2-3': 4, '5-7': 2, '1-2': 6, '3-4': 2}, 
     'CustomerId': {'C3': 2, 'C2': 6, 'C1': 6},
     'ProductId': {'P2': 4, 'P1': 10}
    }

Note that the TransactionDateBin's values are monthly, the default binning period for dates. Note that the monthly bins are in the format [yyyy][mm] where yyyy is the year and mm is the month in the range 1 - 12. You will see in the next example how to give hints to cubify to use other periods such as 'weekly' or 'yearly' for the bins.

Note that after binning, the number of cube rows in the binned cube are the same as the original cube. We still have a total of 14 rows in our binned cube. When we export our binned cube, it will 
look like so:

|S:CustomerId|S:CustomerState|S:DiscountBin|S:PackageSize|S:PriceBin|S:ProductCategory|S:ProductId|S:QtyBin|S:RevenueBin|S:TransactionDateBin|D:TransactionDate|N:Discount|N:Price|N:Qty|N:Revenue|
|------------|---------------|-------------|-------------|----------|-----------------|-----------|--------|------------|--------------------|-----------------|----------|-------|-----|---------|
|C1|CA|3-4|SMALL|15-16|Category2|P2|1-2|15-41|201510|2015-10-10|3.5|15.5|1.0|15.5|
|C1|CA|3-4|SMALL|19-22|Category1|P1|2-3|41-67|201510|2015-10-10|3.5|20.5|3.0|61.5|
|C1|CA|3-4|SMALL|19-22|Category1|P1|2-3|41-67|201510|2015-10-10|3.5|20.5|3.0|61.5|
|C1|CA|3-4|SMALL|19-22|Category1|P1|2-3|41-67|201511|2015-11-03|3.5|21.5|3.0|64.5|
|C1|CA|3-4|SMALL|19-22|Category1|P1|2-3|41-67|201511|2015-11-10|3.5|22.0|3.0|66.0|
|C1|CA|3-4|SMALL|19-22|Category2|P2|1-2|15-41|201511|2015-11-12|3.5|22.0|1.0|22.0|
|C2|NY|3-3|LARGE|15-16|Category2|P2|3-4|41-67|201510|2015-10-10|3.0|16.0|4.0|64.0|
|C2|NY|3-3|LARGE|16-17|Category2|P2|3-4|67-93|201511|2015-11-13|3.0|17.0|4.0|68.0|
|C2|NY|3-3|SMALL|19-22|Category1|P1|1-2|15-41|201510|2015-10-10|3.0|20.0|2.0|40.0|
|C2|NY|3-3|SMALL|19-22|Category1|P1|1-2|15-41|201510|2015-10-11|3.0|19.5|2.0|39.0|
|C2|NY|3-3|SMALL|19-22|Category1|P1|1-2|41-67|201511|2015-11-12|3.0|22.0|2.0|44.0|
|C2|NY|3-3|SMALL|19-22|Category1|P1|1-2|41-67|201511|2015-11-13|3.0|22.0|2.0|44.0|
|C3|MA|3-3|LARGE|18-19|Category1|P1|5-7|119-145|201510|2015-10-11|3.0|18.5|7.0|129.5|
|C3|MA|3-3|LARGE|19-22|Category1|P1|5-7|119-145|201511|2015-11-13|3.0|20.0|7.0|140.0|
    
In the next example, we instruct cubify to bin specific measures, Qty, Price and the TransactionDate. For TransactionDate, we pass in 'weekly' as the hint to cubify.
We will call our new binned cube, 'purchases_autobinned_2'.

    binnedCube = cubify.binCube(cube, 'purchases_autobinned_2', ['TransactionDate','Qty','Price'], {'TransactionDate':'weekly'})

If you examine the dimensions in binned cube, you will see the following new ones, "PriceBin", "QtyBin", "TransactionDateBin". Note that TransactionDateBin now uses weekly bins; the label is in the format [yyyy][ww] where yyyy is the year and ww is the week number is 1 to 52:

    print binnedCube['distincts']

    {'CustomerState': {'NY': 6, 'CA': 6, 'MA': 2}, 
     'PriceBin': {'15-16': 2, '18-19': 1, '19-22': 10, '16-17': 1}, 
     'ProductCategory': {'Category1': 10, 'Category2': 4},
     'PackageSize': {'SMALL': 10, 'LARGE': 4}, 
     'TransactionDateBin': {'201541': 7, '201546': 6, '201545': 1}, 
     'QtyBin': {'2-3': 4, '5-7': 2, '1-2': 6, '3-4': 2}, 
     'CustomerId': {'C3': 2, 'C2': 6, 'C1': 6},
     'ProductId': {'P2': 4, 'P1': 10}
    }

Cubify also support custom binning definitions. This gives you complete flexibility in defining the bin size and bin width.

Custom binning is done with cubify's binning DSL which allows you specify defintions for a binning.
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
    binnedCube1 = cubify.binCubeCustom(binnings, cube, 'purchases_binned_1')

Now when you list the distinct dimension values of binnedCube1, you will see that a new dimension, QtyBin appears in the list with 12 occurrences of 0-5 bin and 2 occurrences of the 5+ bin.:

    print binnedCube1['distincts']

    'QtyBin': {'0-5': 12, '5+': 2}  ....

If you export purchases_binned_1 to csv, you should see a new column, QtyBin.

    cubify.exportCubeToCsv(binnedCube1, '/tmp/exportedBinned1.csv')

|S:CustomerId|S:CustomerState|S:PackageSize|S:ProductCategory|S:ProductId|S:QtyBin|D:TransactionDate|N:Discount|N:Price|N:Qty|N:Revenue|
|------------|---------------|-------------|-----------------|-----------|--------|-----------------|----------|-------|-----|---------|
|C1|CA|SMALL|Category1|P1|0-5|2015-10-10|3.5|20.5|3.0|61.5|
|C1|CA|SMALL|Category1|P1|0-5|2015-10-10|3.5|20.5|3.0|61.5|
|C1|CA|SMALL|Category1|P1|0-5|2015-11-03|3.5|21.5|3.0|64.5|
|C1|CA|SMALL|Category1|P1|0-5|2015-11-10|3.5|22.0|3.0|66.0|
|C1|CA|SMALL|Category2|P2|0-5|2015-10-10|3.5|15.5|1.0|15.5|
|C1|CA|SMALL|Category2|P2|0-5|2015-11-12|3.5|22.0|1.0|22.0|
|C2|NY|LARGE|Category2|P2|0-5|2015-10-10|3.0|16.0|4.0|64.0|
|C2|NY|LARGE|Category2|P2|0-5|2015-11-13|3.0|17.0|4.0|68.0|
|C2|NY|SMALL|Category1|P1|0-5|2015-10-10|3.0|20.0|2.0|40.0|
|C2|NY|SMALL|Category1|P1|0-5|2015-10-11|3.0|19.5|2.0|39.0|
|C2|NY|SMALL|Category1|P1|0-5|2015-11-12|3.0|22.0|2.0|44.0|
|C2|NY|SMALL|Category1|P1|0-5|2015-11-13|3.0|22.0|2.0|44.0|
|C3|MA|LARGE|Category1|P1|5+|2015-10-11|3.0|18.5|7.0|129.5|
|C3|MA|LARGE|Category1|P1|5+|2015-11-13|3.0|20.0|7.0|140.0|

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
    binnedCube2 = cubify.binCubeCustom(binnings, cube, 'purchases_binned_2')

Now when you export the binned cube, you will see the new columns, QtyBin, PriceBin, YearMonth and Region.

    cubify.exportCubeToCsv(binnedCube2, '/tmp/exportedBinned2.csv')

The binned cube's contents:

|S:CustomerId|S:CustomerState|S:PackageSize|S:PriceBin|S:ProductCategory|S:ProductId|S:QtyBin|S:Region|S:YearMonth|D:TransactionDate|N:Discount|N:Price|N:Qty|N:Revenue|
|------------|---------------|-------------|----------|-----------------|-----------|--------|--------|-----------|-----------------|----------|-------|-----|---------|
|C1|CA|SMALL|10+|Category1|P1|0-5|West|Nov-2015|2015-11-03|3.5|21.5|3.0|64.5|
|C1|CA|SMALL|10+|Category1|P1|0-5|West|Nov-2015|2015-11-10|3.5|22.0|3.0|66.0|
|C1|CA|SMALL|10+|Category1|P1|0-5|West|Oct-2015|2015-10-10|3.5|20.5|3.0|61.5|
|C1|CA|SMALL|10+|Category1|P1|0-5|West|Oct-2015|2015-10-10|3.5|20.5|3.0|61.5|
|C1|CA|SMALL|10+|Category2|P2|0-5|West|Nov-2015|2015-11-12|3.5|22.0|1.0|22.0|
|C1|CA|SMALL|10+|Category2|P2|0-5|West|Oct-2015|2015-10-10|3.5|15.5|1.0|15.5|
|C2|NY|LARGE|10+|Category2|P2|0-5|NorthEast|Nov-2015|2015-11-13|3.0|17.0|4.0|68.0|
|C2|NY|LARGE|10+|Category2|P2|0-5|NorthEast|Oct-2015|2015-10-10|3.0|16.0|4.0|64.0|
|C2|NY|SMALL|10+|Category1|P1|0-5|NorthEast|Nov-2015|2015-11-12|3.0|22.0|2.0|44.0|
|C2|NY|SMALL|10+|Category1|P1|0-5|NorthEast|Nov-2015|2015-11-13|3.0|22.0|2.0|44.0|
|C2|NY|SMALL|10+|Category1|P1|0-5|NorthEast|Oct-2015|2015-10-10|3.0|20.0|2.0|40.0|
|C2|NY|SMALL|10+|Category1|P1|0-5|NorthEast|Oct-2015|2015-10-11|3.0|19.5|2.0|39.0|
|C3|MA|LARGE|10+|Category1|P1|5+|Other|Nov-2015|2015-11-13|3.0|20.0|7.0|140.0|
|C3|MA|LARGE|10+|Category1|P1|5+|Other|Oct-2015|2015-10-11|3.0|18.5|7.0|129.5|


6. Aggregating a cube
---------------------

![alt text](http://pluralconcepts.com/images/AggregatedCube.png "Aggregated Cube")

The diagram above shows that you can aggregate a source or a binned cube to produce one or more aggregated cubes. 

Inputs to aggregation are: (1) the "group-by" dimensions, (2) the measures which are to be aggregated and (3) the aggregation formula to apply to each measure. 
For example, if we aggregate the cube, "purchases_binned_2" (created in the previous example) using CustomerId as the group-by dimension and Qty and Price as the measures and 
Average and Sum as the aggregation formula, the result would be a new cube with the following columns:

    CustomerId
    Count
    AverageQty
    TotalQty
    AveragePrice
    TotalPrice

Note that every aggregated cube will have a Count column showing the number of occurrences for a given group-by dimension tuple.

Going back to our tutorial, we will aggregate the cube, "purchases_binned_2" as described above. To do this, simply invoke cubify's aggregateCube method like so:

    aggCube = cubify.aggregateCube(binnedCube2, ['CustomerId'], ['Qty', 'Price'])

Note that we did not pass in the aggregation formulae for the measures. Cubify uses the default aggregation formulae, 'Average' and 'Sum'.
(In later examples, we will see how to use custom formulae for our aggregations.)

The aggregateCube method returns an aggregated cube. cubify will automatically name the cube by appending the group-by dimensions to the name of the original cube.
In our example, the aggregated cube will be called 'purchases_binned_2_CustomerId'. If we export the aggregated cube:

    cubify.exportCubeToCsv(aggCube, '/tmp/aggregatedCubeByCustomerId.csv')

We get the following csv output:

|S:CustomerId|N:Average_Price|N:Average_Qty|N:Count|N:Total_Price|N:Total_Qty|
|------------|---------------|-------------|-------|-------------|-----------|
|C1|20.333333333333332|2.3333333333333335|6|122.0|14.0|
|C2|19.416666666666668|2.6666666666666665|6|116.5|16.0|
|C3|19.25|7.0|2|38.5|14.0|

In our next example, we aggregate our cube using the group-by dimensions, CustomerState and ProductCategory. We will aggregate all measures in the cube using the
default formulae, 'Average' and 'Sum'.

    aggCube = cubify.aggregateCube(binnedCube2, ['CustomerState', 'ProductCategory'])

Note that we are omitting the 3rd argument for measures in our call to aggregateCube above. cubify interprets this as aggregating all measures.
The name of the aggregated cube is 'purchases_binned_2_CustomerState-ProductCategory'.
The output of the aggregated cube now contains the average and total of all measures in the cube like so:

|S:CustomerState|S:ProductCategory|N:Average_Discount|N:Average_Price|N:Average_Qty|N:Average_Revenue|N:Count|N:Total_Discount|N:Total_Price|N:Total_Qty|N:Total_Revenue|
|---------------|-----------------|------------------|---------------|-------------|-----------------|-------|----------------|-------------|-----------|---------------|
|CA|Category1|3.5|21.125|3.0|63.375|4|14.0|84.5|12.0|253.5|
|CA|Category2|3.5|18.75|1.0|18.75|2|7.0|37.5|2.0|37.5|
|MA|Category1|3.0|19.25|7.0|134.75|2|6.0|38.5|14.0|269.5|
|NY|Category1|3.0|20.875|2.0|41.75|4|12.0|83.5|8.0|167.0|
|NY|Category2|3.0|16.5|4.0|66.0|2|6.0|33.0|8.0|132.0|

Now let's turn to more complex aggregations using custom formulae. This is done with cubify's aggregation DSL.
For example, let's say we want a cube containing the average price grouped by Product and Region.  
The aggregation definition (in the file agg1.json) is show below:

    [{
       "name" : "agg1",
       "dimensions": ["ProductId", "Region"],
       "measures" : [
           { "outputField" : { "name" : "AveragePrice", "displayName" : "Average Price" },
             "formula" : { "numerator" : { "aggOperator" : "avg", "expression" : "Price" }, "denominator" : {} }
           }
       ]
    }]

In the definition above, we specify our group-by dimensions, ProductId and Region. Then we define one or more measures which we wish to aggregate - in this case we are aggregating one measure, Price.
In the outputField, we specify "AveragePrice" as the name of the new column which will hold the aggregated values. Then we define the aggregation formula.
An aggregation formula contains a numerator and denominator. For now, in our simple example, we will only define the numerator as taking the "avg" aggregation operator and applying it to the expression, "Price". The denominator is empty. Is used for more complex aggregations such as computing weighted average as we shall see later. (For details of the formula syntax, refer to the <a href="http://pluralconcepts.com/cubify_reference">Cubify Reference documentation.</a>)

Now let's load the aggregation DSL file, agg1.json and call the aggregateCube method to aggregate "purchases_binned_2".

    with open('agg1.json') as agg_file:
        agg = json.load(agg_file)
    aggCubes = cubify.aggregateCube(binnedCube2, agg)
    aggCube = aggCubes[0]

The aggregateCube method returns a list of aggregated cubes, but since we only have one aggregation definition in our DSL, the returned list contains one result cube.
The name of our aggregated cube is 'purchases_binned_2_agg1' - the name is automatically generated by Cubify; it is always the name of the cube we are aggregating concatenated with the name of the aggregation definition.

Now when we list the cube rows of our aggregated cube you will see the aggregated cube contains the following: 

    { ..., 'dimensions': {'Region': 'Other', 'ProductId': 'P1'}, 'measures': {'AveragePrice': 19.25}, ...}
    { ..., 'dimensions': {'Region': 'NorthEast', 'ProductId': 'P2'}, 'measures': {'AveragePrice': 16.5}, ...}
    { ..., 'dimensions': {'Region': 'NorthEast', 'ProductId': 'P1'}, 'measures': {'AveragePrice': 20.875}, ...}
    { ..., 'dimensions': {'Region': 'West', 'ProductId': 'P2'}, 'measures': {'AveragePrice': 18.75} ... }
    { ..., 'dimensions': {'Region': 'West', 'ProductId': 'P1'}, 'measures': {'AveragePrice': 21.125} ... }
 
And when we export the cube, the CVS file contains the following:

|S:ProductId|S:Region|N:AveragePrice|N:Count|
|-----------|--------|--------------|-------|
|P1|NorthEast|20.875|4|
|P1|Other|19.25|2|
|P1|West|21.125|4|
|P2|NorthEast|16.5|2|
|P2|West|18.75|2|

Now let's apply a more complex aggregation to our "purchases_binned_2" cube. Take a look at agg2.json. 

    [{
       "name" : "agg2",
       "dimensions": ["ProductId"],
       "measures" : [
           { "outputField" : { "name":"TotalQty", "displayName": "Total Qty"},
             "formula" : { "numerator" : { "aggOperator" : "sum", "expression" : "Qty" }, "denominator" : {} }
           },
           { "outputField" : {"name":"AverageRevenue", "displayName": "Average Revenue"},
             "formula" : { "numerator"   : { "aggOperator" : "sum", "expression" : "Qty * Price" }, 
                           "denominator" : { "aggOperator" : "sum", "expression" : "Qty" } }
           }
       ]
    }]

Our aggregation definition is called "agg2". Here we are grouping by one dimension, ProductId and generating two new columns, TotalQty and AverageRevenue.
This will create a cube which shows the total quantity and average revenue per product.
The formula for TotalQty is self-explanatory. The formula for AverageRevenue uses both numerator and denominator. In the numerator we take the sum of Qty multiplied by Price. In the denominator, we sum Qty. The average revenue is therefore simply the numerator divided by the denominator. Average revenue is an example of a weighted average where we are getting the average price using quantity as a weight.

Now let's load the aggregation DSL file, agg2.json and call the aggregateCube method to aggregate the cube, "purchases_binned_2".

    with open('agg2.json') as agg_file:
        agg = json.load(agg_file)
    aggCubes = cubify.aggregateCube(binnedCube2, agg)
    aggCube = aggCubes[0]

Now when we list the cube rows for the aggregated cube we get our two new measures, AverageRevenue and TotalQty and cube now contains only 2 rows, one per product.

    {..., 'dimensions': {'ProductId': 'P2'}, 'measures': {'AverageRevenue': 16.95, 'TotalQty': 10.0}, ...}
    {..., 'dimensions': {'ProductId': 'P1'}, 'measures': {'AverageRevenue': 20.294117647058822, 'TotalQty': 34.0}, ...}

And the exported CSV file looks like so:

|S:ProductId|N:AverageRevenue|N:Count|N:TotalQty|
|-----------|----------------|-------|----------|
|P1|20.294117647058822|10|34.0|
|P2|16.95|4|10.0|

We have come to end of the first part of our tutorial. 

Part 2 of the tutorial covers the even more powerful concept of "Cube Sets". With minimal effort, a cube set allows you to automatically bin and aggregate your data and thus maintain the different views of your data as it grows and changes over time.

Cube Set Tutorial
-----------------

![alt text](http://pluralconcepts.com/images/cubeset.png "Cube Set")

The above diagram illustrates the concept of a cube set. A cube set is essentially a container for cubes which are linked together. 
There are 3 types of cubes in a cube set: "source", "binned" and "aggregated". A cube set consists of one and only one source cube, one and only one binned cube, and zero or more aggregated cubes. As raw data is ingested into the source cube over time, the state of the downstream binned and aggregated cubes are always kept in synch with one another.

Open the file, "__tutorials2.py__" in the tutorials folder and follow along with the commentary below. You can execute the tutorial by typing:
   
    python tutorial2.py

1. Creating a cube set
----------------------
Let's create our cube set using the createCubeSet method in cubify like so. In this first example, we keep things simple and let cubify perform automatic
binning for us and we will not define any aggregations initially. 

    cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet', 'purchases.csv')
    
The first argument to createCubeSet is the owner of the cube set. This can be any string. In our example, the owner of the cube set is "tutorial".
The second argument is the name of the cube set. 
The third argument is the name of the CSV file that contains our data.

In our tutorial, once createCubeSet returns successfully, our purchasesCubeSet contains a "source" cube reflecting the original data imported from purchases.csv.
To get the source cube rows, call getSourceCubeRows like so:

    cubeRows = cubify.getSourceCubeRows(cubeSet)

Our cube set also contains a binned cube - with the binnings automatically determined by cubify.
To get the binned cube rows, call getBinnedCubeRows like so:

    binnedCubeRows = cubify.getBinnedCubeRows(cubeSet)

We can export the binned cube from our cubeset like so:

    cubify.exportBinnedCubeToCsv(cubeSet, '/tmp/purchasesCubeSetBinnedCube.csv')

2. Aggregating a cube set (simple)
----------------------------------
Now let's aggregate our cube set with the following dimensions, ['CustomerState', 'ProductId']:
    
    aggCubes = cubify.performAggregation(cubeSet, ['CustomerState', 'ProductId'])

The above method will perform 2 aggregations on the binned cube in our cube set to produce 2 aggregated cubes. The first aggregation will group by 'CustomerState' and 'ProductId' and the second aggregation will group by 'CustomerState'. Now to get the cube rows of the first aggregated cube, we simple call the method getAggregatedCubeRows passing in the name of our cube set and the reference to the aggregated cube, 'CustomerState-ProductId' like so:

    agg1CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'CustomerState-ProductId')

Note that the reference name of an aggregated cube is a concatenation of the group-by dimension names. In the example above it is, 'CustomerState-ProductId'.
The reference name of the second aggregated cube is simply 'CustomerState'. To get the cube rows for this cube:

    agg2CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'CustomerState')

To get all aggregated rows, pass in  'ALL' as the second argument:

    aggRows = cubify.getAggregatedCubeRows(cubeSet, 'ALL')

We can export the aggreated cubes by calling the exportAggregateCubeToCsv method like so.

    cubify.exportAggCubeToCsv(cubeSet, '/tmp/purchasesCubeSet-aggregated-by-CustomerState-ProductId.csv', 'CustomerState-ProductId')
    cubify.exportAggCubeToCsv(cubeSet, '/tmp/purchasesCubeSet-aggregated-by-CustomerState.csv, 'CustomerState')

The exported cubes would look like:

|S:CustomerState|S:ProductId|N:Average_Price|N:Average_Qty|N:Count|N:Total_Price|N:Total_Qty|
|---------------|-----------|---------------|-------------|-------|-------------|-----------|
|CA|P1|21.125|3.0|4|84.5|12.0|
|CA|P2|18.75|1.0|2|37.5|2.0|
|MA|P1|19.25|7.0|2|38.5|14.0|
|NY|P1|20.875|2.0|4|83.5|8.0|
|NY|P2|16.5|4.0|2|33.0|8.0|

and

|S:CustomerState|N:Average_Price|N:Average_Qty|N:Count|N:Total_Price|N:Total_Qty|
|---------------|---------------|-------------|-------|-------------|-----------|
|CA|20.333333333333332|2.3333333333333335|6|122.0|14.0|
|MA|19.25|7.0|2|38.5|14.0|
|NY|19.416666666666668|2.6666666666666665|6|116.5|16.0|


3. Aggregating a cube set (custom)
----------------------------------
Now Let's create our cube set using the createCubeSet using custom binnings and aggregations. We will call this cube set, 'purchasesCubeSet2'. We have seen the binning definitions (binnings.json)  and aggregation definitions (aggs.json) in tutorial 1. We will now use these definitions together with the source data defined in purchases.csv to create our cube set, called "purchasesCubeSet". 
Here's the code:

    with open('binnings.json') as binnings_file:
        binnings = json.load(binnings_file)

    with open('aggs.json') as aggs_file:
        aggs = json.load(aggs_file)e

    cubeSet = cubify.createCubeSet('tutorial', 'purchasesCubeSet2', 'purchases.csv', binnings, aggs)

Note that we now pass in the binnings and aggregations to the createCubeSet method.    

Our cube set now contains 2 aggregated cubes (resulting from the 2 aggregation definitions defined in aggs.json).
To refresh your memory, here is aggs.json:

    [
       {
          "name" : "agg1",
          "dimensions": ["ProductId", "Region"],
          "measures" : [
              { "outputField" : {"name":"AveragePrice", "displayName": "Average Price"},
                "formula" : { "numerator" : { "aggOperator" : "avg", "expression" : "Price" }, "denominator" : {} }
              }
          ]
       },
       {
          "name" : "agg2",
          "dimensions": ["ProductId"],
          "measures" : [
              { "outputField" : {"name":"TotalQty", "displayName": "Total Qty"},
                "formula" : {"numerator" : { "aggOperator": "sum", "expression" : "Qty" }, "denominator" : {} }
              },
              { "outputField" : {"name":"AverageRevenue", "displayName": "Average Revenue"},
                "formula" : { "numerator"   : { "aggOperator": "sum", "expression": "Qty * Price" }, 
                              "denominator" : { "aggOperator": "sum", "expression": "Qty" }}
              }
          ]
       }
    ]

You can retrieve the cube rows of the aggregated cubes by referring to the aggregation names like so:

    agg1CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'agg1')
    agg2CubeRows = cubify.getAggregatedCubeRows(cubeSet, 'agg2')

4. Adding cube rows to cube set
-------------------------------
All is well with our cube set thus far. But now let's say we are in a new month and we have more purchases data to add to our cube set.
Let's assume the new purchases data is in a file called "morePurchases.csv". To add the rows to our source cube in our cube set, simply call:

    cubify.addRowsToSourceCube(cubeSet, 'morePurchases.csv')

This method adds the new rows to our source cube, as well as updates the binned cube and aggregated cubes. Thus our binned and aggregated cubes are kept in synch with 
the data in our source cube. You can verify that this is so by examining the rows of the binned cube and aggregated cubes (left as an exercise).







  
 


 
  







   




 

