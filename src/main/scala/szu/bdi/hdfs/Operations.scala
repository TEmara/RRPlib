package szu.bdi.hdfs

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import szu.bdi.utils.HdfsIterator

object Operations {

  def takeFiles(from: String, to: String, count: Int)(implicit hdfs: FileSystem):Boolean ={
    val srcFileList = hdfs.listStatus(new Path(from))
    if (srcFileList.isEmpty) false
    else{
      if (!hdfs.exists(new Path(to))) hdfs.mkdirs(new Path(to))
      val takenList = srcFileList.take(count)
      for (idx <- takenList.indices){
        val filePath = takenList(idx).getPath
        hdfs.rename(filePath, new Path(to + "/" + filePath))
      }
      true
    }
  }

  def takeRandomFiles(from: String, to: String, count: Int, preserveFileName: Boolean)(implicit hdfs: FileSystem):Boolean ={
    val df = new java.text.DecimalFormat("00000")
    val srcFileList = hdfs.listStatus(new Path(from))
    if (srcFileList.isEmpty) false
    else{
      if (!hdfs.exists(new Path(to))) hdfs.mkdirs(new Path(to))
      //val startCounting = hdfs.listStatus(new Path(strDest)).length
      val xx= scala.util.Random.shuffle((0 until srcFileList.length).toList).take(count)
      if (preserveFileName) {
        for (idx <- xx.indices) {
          val filePath = srcFileList(xx(idx)).getPath
          hdfs.rename(filePath, new Path(to + "/" + filePath.getName))
        }
      }else{
        val startCounting = hdfs.listStatus(new Path(to)).length
        for (idx <- xx.indices) {
          val filePath = srcFileList(xx(idx)).getPath
          hdfs.rename(filePath, new Path(to + "/part-" + df.format(idx + startCounting)))
        }
      }
      true
    }
  }

  def moveFiles(from: String, to: String, preserveFileName: Boolean)(implicit hdfs: FileSystem):Boolean ={
    if (!hdfs.exists(new Path(to))) hdfs.mkdirs(new Path(to))
    val df = new java.text.DecimalFormat("000000")
    val srcFileList = hdfs.listStatus(new Path(from))
    if (srcFileList.isEmpty) false
    else{

      if (preserveFileName) {
        for (idx <- srcFileList.indices) {
          val filePath = srcFileList(idx).getPath
          val ok = hdfs.rename(filePath, new Path(to + "/" + filePath.getName))
        }
      }else{
        val ri = hdfs.listFiles(new Path(to),false)
        val startCounting = HdfsIterator.getSize(ri)
        for (idx <- srcFileList.indices) {
          val filePath = srcFileList(idx).getPath
          val ok = hdfs.rename(filePath, new Path(to + "/part-" + df.format(idx + startCounting)))
        }
      }
      true
    }
  }

  def moveFiles1(from: String, to: String)(implicit hdfs: FileSystem):Boolean ={
    if (!hdfs.exists(new Path(to))) hdfs.mkdirs(new Path(to))
    val df = new java.text.DecimalFormat("000000")
    val srcFileList = hdfs.listStatus(new Path(from))
    if (srcFileList.isEmpty) false
    val ri = hdfs.listFiles(new Path(to),false)
    //val startCounting = HdfsIterator.getSize(ri)
    val rnd = scala.util.Random.nextInt(100)
    for (i <- 0 until srcFileList.length){
      //val ok = hdfs.rename(dirList(i).getPath, new Path(strDest + "/part-" + df.format(i+startCounting)))
      val ok = hdfs.rename(srcFileList(i).getPath, new Path(to + "/part-" + rnd+ df.format(i)))
    }
    true
  }

  def moveFilesWithCount1(strSource: String, strDest: String, count: Int)(implicit hdfs: FileSystem):Boolean ={
    if (!hdfs.exists(new Path(strDest))) hdfs.mkdirs(new Path(strDest))
    val df = new java.text.DecimalFormat("00000")
    val dirList = hdfs.listStatus(new Path(strSource))
    if (dirList.isEmpty) return false
    val rnd = scala.util.Random.nextInt(100)
    for (i <- dirList.length - 1 to dirList.length - count by -1){
      val ok = hdfs.rename(dirList(i).getPath, new Path(strDest + "/part-" + rnd + df.format(i)))
    }
    true
  }


  def moveFilesFromSubDir(strSource: String, strDest: String)(implicit hdfs: FileSystem) :Unit={
    if (!hdfs.exists(new Path(strDest))) hdfs.mkdirs(new Path(strDest))
    val df = new java.text.DecimalFormat("000000")
    val dirList = hdfs.listStatus(new Path(strSource))

    for (j <- 0 until dirList.length){
      val fileList = hdfs.listStatus(dirList(j).getPath)
      var startCounting = hdfs.listStatus(new Path(strDest)).length
      for (i <- 0 until fileList.length){
        if (fileList(i).getPath.getName != "_SUCCESS")
          hdfs.rename(fileList(i).getPath, new Path(strDest + "/part-" + df.format(i+startCounting)))
      }
    }
  }

  def moveFilesOneLevelUp(strDest: String)(implicit hdfs: FileSystem) :Unit={
    //if (!hdfs.exists(new Path(strDest))) hdfs.mkdirs(new Path(strDest))
    val df = new java.text.DecimalFormat("0000")
    val dirList = hdfs.listStatus(new Path(strDest))

    for (j <- 0 until dirList.length){
      if (dirList(j).isDirectory){
        val fileList = hdfs.listStatus(dirList(j).getPath)
        val startCounting = hdfs.listStatus(new Path(strDest)).length
        for (i <- 0 until fileList.length){
          if (fileList(i).getPath.getName != "_SUCCESS")
            hdfs.rename(fileList(i).getPath, new Path(strDest + "/" + dirList(j).getPath.getName + "-" + df.format(i)))
        }
      }
    }
  }

  def deleteAllSubDir(parent: String)(implicit hdfs: FileSystem) :Unit={
    //if (!hdfs.exists(new Path(strDest))) hdfs.mkdirs(new Path(strDest))
    val df = new java.text.DecimalFormat("0000")
    val parentList = hdfs.listStatus(new Path(parent))

    for (j <- parentList.indices)
      if (parentList(j).isDirectory)
        hdfs.delete(parentList(j).getPath, true)
  }


  def copyFiles(strSource: String, strDest: String)(implicit hdfs: FileSystem):Boolean ={
    if (!hdfs.exists(new Path(strDest))) hdfs.mkdirs(new Path(strDest))
    val df = new java.text.DecimalFormat("000000")
    val dirList = hdfs.listStatus(new Path(strSource))
    if (dirList.isEmpty) return false
    val startCounting = hdfs.listStatus(new Path(strDest)).length
    for (i <- 0 until dirList.length){
      FileUtil.copy(hdfs, dirList(i).getPath, hdfs, new Path(strDest + "/part-" + df.format(i+startCounting)),false, false, hdfs.getConf)
    }
    true
  }

  // Move files in a random order
  def randMoveFiles(strSource: String, strDest: String)(implicit hdfs: FileSystem):Unit ={
    if (!hdfs.exists(new Path(strDest))) hdfs.mkdirs(new Path(strDest))
    val df = new java.text.DecimalFormat("000000")
    val srcFileList = hdfs.listStatus(new Path(strSource))
    val xx= util.Random.shuffle((0 until srcFileList.length).toList)
    for (f <- xx.indices) {
      val ok = hdfs.rename(srcFileList(xx(f)).getPath, new Path(strDest + "/part-" + df.format(f)))
    }
  }

  // Move randomly a particular number of files
  def randMoveFilesWithCount(strSource: String, strDest: String, count: Int)(implicit hdfs: FileSystem):Boolean ={
    if (!hdfs.exists(new Path(strDest))) hdfs.mkdirs(new Path(strDest))
    val df = new java.text.DecimalFormat("00000")
    val srcFileList = hdfs.listStatus(new Path(strSource))
    if (srcFileList.isEmpty) false
    else{
      //val startCounting = hdfs.listStatus(new Path(strDest)).length
      val xx= scala.util.Random.shuffle((0 until srcFileList.length).toList).take(count)
      for (idx <- xx.indices) {
        val filePath = srcFileList(xx(idx)).getPath
        hdfs.rename(filePath, new Path(strDest + "/" + filePath.getName))
      }
      true
    }
  }

  def randCutWithCount(strSource: String, strDest: String, count: Int)(implicit hdfs: FileSystem):Boolean ={
    val dirItr = hdfs.listFiles(new Path(strSource),false)
    var fileList: List[String] = Nil
    while (dirItr.hasNext){
      val f = dirItr.next()
      if(f.isFile) fileList = fileList.::(f.getPath.getName)
    }

    var rndFileList= scala.util.Random.shuffle(fileList)
    var d=1
    while(rndFileList.nonEmpty){
      val xx= rndFileList.take(count)
      if (!hdfs.exists(new Path(strDest + "/DS" + d))) hdfs.mkdirs(new Path(strDest + "/DS" + d))
      for (f <- xx.indices) {
        hdfs.rename(new Path(strSource + "/" + xx(f)), new Path(strDest + "/DS" + d + "/" +xx(f)))
      }
      rndFileList=rndFileList.drop(count)
      d=d+1
    }
    true
  }


  def printFileContents(filePath: String)(implicit hdfs: FileSystem): Unit={
    val file = new Path(filePath)
    printFileContents(file)
  }

  def printFileContents(filePath: org.apache.hadoop.fs.Path)(implicit hdfs: FileSystem): Unit={
    if (!hdfs.exists(filePath)) println("File is not exist")
    else{
      val bfr = new BufferedReader(new InputStreamReader(hdfs.open(filePath)))
      var str = bfr.readLine
      while (str != null){
        println(str)
        str = bfr.readLine
      }
    }
  }

  def printDirContents(dirPath: String)(implicit hdfs: FileSystem): Unit={
    val dir = new Path(dirPath)
    val fileList = hdfs.listStatus(dir)
    if (fileList.isEmpty) println("Error: Directory is empty")
    else{
      for (idx <- fileList.indices){
        val filePath = fileList(idx).getPath
        printFileContents(filePath)
      }
    }
  }
}
