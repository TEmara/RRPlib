# RRPlib: A Spark Library for Representing HDFS Blocks as A Set of Random Sample Data Blocks

RRPlib is a spark library for representing HDFS blocks as a set of random sample data blocks. This Library is a valuable tool to make the analysis process of the massive data an attainable operation. It mainly has three components data generator, RRP, and massive-RRP. The purpose of the data generator component is to generate synthetic datasets for classification and regression. RRP component is an implementation example for the round-random partitioner in [1] and also the same for the massive-RRP component.

## Building RRPlib

RRPlib comes packaged with a self-contained Maven installation to ease building and deployment from source located under the build/ directory. This script will automatically download and setup all necessary build requirements locally within the build/ directory itself. As an example, one can build a version of RRPlib as follows:
```
$ ./build/mvn -DskipTests clean package
```

## Illustrative Examples

### Data generation

The generation process can be done through the command-line console. For example, in order to generate a dataset named <i>DS001</i> with the following parameters: number of features = 100, number of classes = 100, number of records = 100,000,000, and number of blocks = 1000, apply the following command line,
```
$ spark-submit --class szu.bdi.apps.generateClassData RRPlib.jar DS001 100000000 100 100 1000 
```

### Data partitioning

After generating the datasets, it can be repartitioned using RRP through the following command: 
```
$ spark-submit --class szu.bdi.apps.partitionerApp RRPlib.jar <iDS> <oDS> <Q>
```

Where <i> iDS </i> is the path of the input dataset, <i>oDS</i> is the path of the output dataset, and <i>Q</i> is the partitions number of the output dataset.

As an example, to generate the dataset <i>DS001P</i> from <i>DS001</i> with the same number of partitions <i>1000</i>, we executed the following command:

```
$ spark-submit --class szu.bdi.apps.partitionerApp RRPlib.jar DS001 DS001P 1000
```
The user also has the ability to import the jar file and use the library's API, <i>RRP(...).partitionByRRP(...)</i>. 
A complete example to demonstrate how to use it is as follows,

```
import org.apache.spark.sql.SparkSession
import szu.bdi.partitioner._

val spark = SparkSession.builder.master("yarn").getOrCreate()

val sourcePath: String = "DS001"
val finalPath: String = "DS001P"
val numPartReq = 1000

val sc = spark.sparkContext
val datasetRDD = sc.textFile(sourcePath)

val finalRDD = RRP(datasetRDD).partitionByRRP(numPartReq)
finalRDD.saveAsTextFile(finalPath)
```

## For more details
For more details about the architecture of RRP and massive-RRP please see our
[paper](https://doi.org/10.1016/j.jss.2018.11.007)

## References 
[1] T. Z. Emara, J. Z. Huang, A distributed data management system to support large-scale data analysis, Journal of Systems
and Software 148 (2019) 105-115. doi:https://doi.org/10.1016/j.jss.2018.11.007.
