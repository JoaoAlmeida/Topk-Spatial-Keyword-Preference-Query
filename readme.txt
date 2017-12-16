Instructions: How to use this code.

In order to execute a Top-k Spatial Keyword Preference Query (SKPQ), you need an indexed database. 

This code allows you to build an Inverted File or a Spatial Inverted Index (S2I) and execute a SKPQ query.


------ Building an Indexed Database ------

1. Open the input file "s2i\framework.properties"

2. In "experiment.name" point the index you want. If you want an Inverted File set "experiment.name=IFBuildIndex", 
if you want a S2I set "experiment.name=S2IBuildIndex"

3. Set in "experiment.database" the database name (i.e. experiment.database = NorthAmerica)

4. Describe the index file path in "experiment.folder". This folder has to be empty.

	5. (Optional) if you are building a large index, set the s2i.fileCacheSize = 100000000. This will increase the indexing speed.

6. Set the objects of reference (features) file Path in "dataset.featuresFile" (i.e. dataset.featuresFile = na-hotel.txt).
This file can be downloaded at https://goo.gl/zHExTn

7. Set objects of interest file path in "dataset.objectsFile" (i.e. dataset.objectsFile = hotel.txt).
This file can be downloaded at https://goo.gl/zHExTn

8. Execute SPKFramework.java at s2i\src\framework

Your index is stored in the experiment folder.

The "buildVeniceIndex.properties" you have downloaded is an example how to build a S2I index for Venice Database. 



------ Executing the SKPQ query ------

1. Open the input file "s2i\framework.properties"

2. In "experiment.name" point the query you want. If you want a SKPQ query on an Inverted File set "experiment.name=IFSearch", 
if you want a SKPQ query on a S2I set "experiment.name=PreferenceSearch"

3. Choose the spatial neighborhood at "query.neighborhood". query.neighborhood = 0 for nearest neighbor (NN) and query.neighborhood = 1 for range
If you want two different spatial neighborhoods at the same experiment set query.neighborhood = 0;1

	4. (Optional) If you choose range, point the query radius in "query.radius". query.radius = 0.005 is approximately 200 meters.

5. Set the algorithm to process the query. Using S2I you can choose experiment.plusMode = false for SIA algorithm and experiment.plusMode = true for SIA+ algorithm.
Note that if you are using an Inverted File to process the query, you have only one algorithm: experiment.plusMode = false is for IFA algorithm when using an Inverted File.

6. Set in "experiment.database" the database name (i.e. experiment.database = NorthAmerica)

7. Describe the index file path in "experiment.folder"

8. Set the objects of reference (features) file Path in "dataset.featuresFile" (i.e. dataset.featuresFile = na-hotel.txt).
This file can be downloaded at https://goo.gl/zHExTn

9. Set objects of interest file path in "dataset.objectsFile" (i.e. dataset.objectsFile = hotel.txt).
This file can be downloaded at https://goo.gl/zHExTn

10. Execute SPKFramework.java at s2i\src\framework

The "preferenceSerarch.properties" you have downloaded is an example how to process a SKPQ query using SIA algorithm. Two experiments will be executed: 
the first one is a SKPQ query processed with range spatial neighborhood and the second one with NN spatial neighborhood. 
The experiment data is printed at output folder.  You can set the output folder file path in "experiment.output".