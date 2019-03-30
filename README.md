# RRPlib: A Spark Library for Representing HDFS Blocks as A Set of Random Sample Data Blocks

RRPlip is a spark library for representing HDFS blocks as a set of random sample data blocks. This Library is a valuable tool to make the analysis process of the massive data an attainable operation. It mainly has three components data generator, RRP, and massive-RRP. The purpose of the data generator component is to generate synthetic datasets for classification and regression. RRP component is an implementation example for the round-random partitioner in [1] and also the same for the massive-RRP component.

## Illustrative Examples

The generation process can be done through the command-line console. For example, in order to generate a dataset named DS001 with the following parameters: number of features = 100, number of classes = 100, number of records = 100,000,000, and number of blocks = 1000, we apply the following command line,
```
$ spark-submit --class szu.bdi.generators.GenerateDataSet RRPlib.jar DS001 100000000 100 100 1000 
```
After generating the datasets, we repartitioned them using RRP through the following command: 
\begin{tcolorbox}
\begin{lstlisting}[language=bash]
$ spark-submit --class szu.bdi.apps.partitionerApp RRPlib.jar \
  <iDS> <oDS> <Q>
\end{lstlisting}
\end{tcolorbox}

Where <i> iDS </i> is the path of the input dataset, <i>oDS</i> is the path of the output dataset, and <i>Q</i> is the partitions number of the output dataset.

As an example, to generate the dataset <i>DS001P</i> from <i>DS001</i> with the same number of partitions <i>1000</i>, we executed the following command:

```
$ spark-submit --class szu.bdi.apps.partitionerApp RRPlib.jar DS001 DS001P 1000
```
The user also has the ability to import the jar file and use the library's API, <i>RRP(...).partitionByRRP(...)</i>. A complete example demonstrates how to use it is as follows,

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

## Status
The source code in this repository is a research prototype and only implements the scheduling techniques described in our paper.
The existing Spark unit tests pass with our changes and we are actively working on adding more tests for Drizzle.
We are also working towards a Spark JIRA to discuss integrating Drizzle with the Apache Spark project.

Finally we would like to note that extensions to integrate Structured Streaming and Spark ML will be implemented separately.

## For more details
For more details about the architecture of Drizzle please see our
[Spark Summit 2015 Talk](https://spark-summit.org/2016/events/low-latency-execution-for-apache-spark/)
and our [Technical Report](http://shivaram.org/drafts/drizzle.pdf)

## Acknowledgements
This is joint work with Aurojit Panda, Kay Ousterhout, Mike Franklin, Ali Ghodsi, Ben Recht and Ion
Stoica from the [AMPLab](http://amplab.cs.berkeley.edu) at UC Berkeley.
