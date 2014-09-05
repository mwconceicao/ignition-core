package ignition.core.jobs.utils

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime


object SparkContextUtils {

  implicit class SparkContextImprovements(sc: SparkContext) {

    private def getFileSystem(path: Path): FileSystem = {
      path.getFileSystem(sc.hadoopConfiguration)
    }

    private def getStatus(commaSeparatedPaths: String, removeEmpty: Boolean): Seq[FileStatus] = {
      val paths = ignition.core.utils.HadoopUtils.getPathStrings(commaSeparatedPaths).map(new Path(_)).toSeq
      val fs = getFileSystem(paths.head)
      for {
        path <- paths
        status <- Option(fs.globStatus(path)).getOrElse(Array.empty).toSeq
        if status.isDirectory || !removeEmpty || status.getLen > 0 // remove empty files if necessary
      } yield status
    }

    // This call is equivalent to a ls -d in shell, but won't fail if part of a path matches nothing,
    // For instance, given path = s3n://bucket/{a,b}, it will work fine if a exists but b is missing
    def sortedGlobPath(path: String, removeEmpty: Boolean = true): Seq[String] = {
      val paths = ignition.core.utils.HadoopUtils.getPathStrings(path)
      paths.flatMap(p => getStatus(p, removeEmpty)).map(_.getPath.toString).distinct.sorted
    }

    // This method's purpose is to skip empty files on a given path (to work around the fact that empty files gives errors to hadoop)
    // if the path expands only to files, it will just filter out the empty ones
    // if it expands to a directory, then it will get all non empty files from this directory (but will ignore subdirectories)
    def nonEmptyFilesPath(path: String): String = {
      // getStatus only get non empty files
      val status = getStatus(path, removeEmpty = true)
      val (dirs, files) = status.partition(f => f.isDirectory)
      val filesFromDirs = dirs.flatMap(dir => getStatus(dir.getPath.toString + "/*", removeEmpty = true)).filter(p => p.isFile)
      val finalFilesStatus = files.filter(_.isFile) ++ filesFromDirs
      val finalFiles = finalFilesStatus.map(_.getPath.toString).toSet

      if (finalFiles.isEmpty)
        throw new Exception(s"Zero non-empty files matched by: $path")

      finalFiles.mkString(",")
    }

    private def nonEmptyTextFile(path: String): RDD[String] = {
      sc.textFile(nonEmptyFilesPath(path))
    }

    private def filterPaths(path: String, requireSuccess: Boolean = false, startDate: Option[DateTime], endDate: Option[DateTime]): Seq[String] = {
      val basePaths = sortedGlobPath(path).reverse.dropWhile(p => requireSuccess && sortedGlobPath(s"$p/{_SUCCESS,_FINISHED}", removeEmpty = false).isEmpty).reverse
      if (startDate.isEmpty && endDate.isEmpty)
          basePaths
      else
        basePaths.filter(p => {
          val date = PathUtils.extractDate(p)
          val goodStartDate = startDate.isEmpty || date.equals(startDate.get) || date.isAfter(startDate.get)
          val goodEndDate = endDate.isEmpty || date.equals(endDate.get) || date.isBefore(endDate.get)
          goodStartDate && goodEndDate
        })
    }

    def getFilteredPaths(path: String, requireSuccess: Boolean,
                         startDate: Option[DateTime],
                         endDate: Option[DateTime], lastN: Option[Int]): Seq[String] = {
      require(lastN.isEmpty || endDate.isDefined, "If you are going to get the last files, better specify the end date to avoid getting files in the future")
      val paths = filterPaths(path, requireSuccess, startDate, endDate)
      if (lastN.isEmpty) paths else paths.takeRight(lastN.get)
    }

    def filterAndGetTextFiles(path: String, requireSuccess: Boolean = false,
                              startDate: Option[DateTime] = None,
                              endDate: Option[DateTime] = None,
                              lastN: Option[Int] = None): RDD[String] = {
      val paths = getFilteredPaths(path, requireSuccess, startDate, endDate, lastN).mkString(",")
      if (paths.isEmpty)
        throw new Exception(s"Tried to get by date with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      else
        nonEmptyTextFile(paths)
    }

    private def stringHadoopFile(path: String): RDD[String] = {
      sc.sequenceFile(nonEmptyFilesPath(path), classOf[LongWritable], classOf[org.apache.hadoop.io.BytesWritable])
        .map({ case (k, v) => new String(v.getBytes) })
    }

    def filterAndGetStringHadoopFiles(path: String, requireSuccess: Boolean = false,
                                      startDate: Option[DateTime] = None,
                                      endDate: Option[DateTime] = None,
                                      lastN: Option[Int] = None): RDD[String] = {
      val paths = getFilteredPaths(path, requireSuccess, startDate, endDate, lastN).mkString(",")
      if (paths.isEmpty)
        throw new Exception(s"Tried to get by date with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      else
        stringHadoopFile(getFilteredPaths(path, requireSuccess, startDate, endDate, lastN).mkString(","))
    }
  }
}
