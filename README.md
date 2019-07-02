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
The user also has the ability to import the jar file and use the library's API. 

A complete example to demonstrate how to use it is as follows,

```
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import szu.bdi.generators.ClassificationData.generateMultiClasses

val sc = new SparkContext(conf)

val hadoopConf = new Configuration()
val hdfs = FileSystem.get(hadoopConf)

val FinalPath: String = "DS001"
val ClassCount: Int = 100 
val numRecords: Long = 1000000 //real total record no. = recordCount * ClassCount * repeatFactor
val numFeatures: Int = 100
val numPartitions: Int = 10 // real total number of partitions = numPartitions * ClassCount * repeatFactor

hdfs.mkdirs(new Path(FinalPath))
generateMultiClasses(sc, hdfs, FinalPath, numFeatures, numRecords, numPartitions, ClassCount)

println("all done")
```
For further examples to generate big data appropriate to regression tasks, you can find these examples, [generateRegDataWithGamma](https://github.com/TEmara/RRPlib/blob/master/src/main/scala/szu/bdi/apps/generateRegDataWithGamma.scala) and [generateRegDataWithNormal](https://github.com/TEmara/RRPlib/blob/master/src/main/scala/szu/bdi/apps/generateRegDataWithNormal.scala).

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
### Big data analysis
To apply machine learning algorithms, this library is implemented based on Spark, therefore the existing [Spark’s machine learning library](https://spark.apache.org/docs/2.0.0/ml-guide.html) can be used. For example, using [random forest classifier](https://spark.apache.org/docs/2.0.0/ml-classification-regression.html#random-forest-classifier):

```
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

// A function to load and parse the data file, converting it to a DataFrame.
def importDF(spark: SparkSession,strPath: String): DataFrame={
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  val RDD = sc.textFile(strPath)
    .map(textLine => {
      val columns = textLine.split(",")
      def label(): Double = columns.last.toDouble//labels(columns.last.toDouble)
      def features(): org.apache.spark.ml.linalg.Vector = Vectors.dense(columns.dropRight(1).map(t => t.toDouble))
      LabeledPoint(label(), features())
    })
  sqlContext.createDataFrame(RDD).toDF("label", "features")
}

// Load and parse the data file, converting it to a DataFrame
val data = importDF("DS001P/part-00000")

// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .fit(data)
// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data)

// Split the data into training and test sets (30% held out for testing).
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a RandomForest model.
val rf = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setNumTrees(10)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// Chain indexers and forest in a Pipeline.
val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
println("Learned classification forest model:\n" + rfModel.toDebugString)

```
## For more details
For more details about the architecture of RRP and massive-RRP please see our
[paper](https://doi.org/10.1016/j.jss.2018.11.007)

## References 
[1] T. Z. Emara, J. Z. Huang, A distributed data management system to support large-scale data analysis, Journal of Systems
and Software 148 (2019) 105-115. doi:https://doi.org/10.1016/j.jss.2018.11.007.
