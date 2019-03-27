package szu.bdi

import org.apache.spark.SparkConf

object ConfigObjDev {
  var sparkConf = new SparkConf()

  if (System.getProperty("os.name").startsWith("win") || System.getProperty("os.name").startsWith("Win")) {
    sparkConf.setJars(List("D:\\work\\IdeaProjects\\SamplingExp\\out\\artifacts\\SamplingExp_jar\\SamplingExp.jar"))
  }

  sparkConf
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "hdfs://Hera01:8020//user/spark/spark2ApplicationHistory")
    .set("spark.yarn.historyServer.address", "http://Hera01:18089")
    .set("spark.executor.memory", "10g")//10
    .set("spark.executor.cores", "5")//21  11  5
    .set("spark.executor.instances", "19")//51  12  149
    .set("spark.yarn.executor.memoryOverhead","2g")
    .set("spark.driver.memory", "10g")
    .set("spark.driver.cores", "5")
    .set("spark.yarn.driver.memoryOverhead","2g")
    .set("spark.yarn.jars","local:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*")
    .set("spark.driver.extraLibraryPath","local:/opt/cloudera/parcels/CDH-5.13.0-1.cdh5.13.0.p0.29/lib/hadoop/lib/native")
    .set("spark.yarn.appMasterEnv.PYSPARK_PYTHON","local:/opt/cloudera/parcels/Anaconda-4.3.1/bin/python")
    //.set("spark.yarn.am.extraLibraryPath","local:/opt/cloudera/parcels/CDH-5.13.0-1.cdh5.13.0.p0.29/lib/hadoop/lib/native")
    //.set("spark.yarn.config.gatewayPath","local:/opt/cloudera/parcels")
  //.set("spark.scheduler.listenerbus.eventqueue.size","100000")
  //.set("spark.default.parallelism","1490")
  //.set("spark.cleaner.ttl", "60s")
  //.set("spark.driver.maxResultSize","2g")
  //.setMaster("yarn")
  //.set("spark.submit.deployMode","cluster")
}
