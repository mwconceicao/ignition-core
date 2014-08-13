package ignition.core.jobs.utils

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.spark.rdd.RDD


object SparkContextUtils {

  implicit class SparkContextImprovements(sc: SparkContext) {

    private def getFileSystem(path: Path): FileSystem = {
      path.getFileSystem(sc.hadoopConfiguration)
    }

    private def getStatus(commaSeparatedPaths: String): Seq[FileStatus] = {
      val paths = ignition.core.utils.HadoopUtils.getPathStrings(commaSeparatedPaths).map(new Path(_)).toSeq
      val fs = getFileSystem(paths.head)
      for {
        path <- paths
        status <- Option(fs.globStatus(path)).getOrElse(Array.empty).toSeq
        if status.isDirectory || status.getLen > 0 // remove empty files
      } yield status
    }

    // This call is equivalent to a ls -d in shell, but won't fail if part of a path matches nothing,
    // For instance, given path = s3n://bucket/{a,b}, it will work fine if a exists but b is missing
    def sortedGlobPath(path: String): Seq[String] = {
      val paths = ignition.core.utils.HadoopUtils.getPathStrings(path)
      paths.flatMap(getStatus(_)).map(_.getPath.toString).distinct.sorted
    }

    // This method's purpose is to skip empty files on a given path (to work around the fact that empty files gives errors to hadoop)
    // if the path expands only to files, it will just filter out the empty ones
    // if it expand to a directory, then it will get all non empty files from this directory (but will ignore subdirectories)
    def nonEmptyFilesPath(path: String): String = {
      // getStatus only get non empty files
      val status = getStatus(path)
      val (dirs, files) = status.partition(f => f.isDirectory)
      val filesFromDirs = dirs.flatMap(dir => getStatus(dir.getPath.toString + "/*")).filter(p => p.isFile)
      val finalFilesStatus = files.filter(_.isFile) ++ filesFromDirs
      val finalFiles = finalFilesStatus.map(_.getPath.toString).toSet

      if (finalFiles.isEmpty)
        throw new Exception(s"Zero non-empty files matched by: $path")

      finalFiles.mkString(",")
    }

    def nonEmptyTextFile(path: String): RDD[String] = {
      sc.textFile(nonEmptyFilesPath(path))
    }

    def lastNonEmptyTextFile(path: String, lastN: Int = 1): RDD[String] = {
      require(lastN > 0)
      val paths = sortedGlobPath(path)
      if (paths.size < lastN) {
        throw new Exception(s"Tried to get last $lastN files/dirs of path $path, but the resulting number of paths $paths is less than the required")
      } else {
        sc.nonEmptyTextFile(paths.mkString(","))
      }
    }

    def stringHadoopFile(path: String): RDD[String] = {
      sc.sequenceFile(nonEmptyFilesPath(path), classOf[LongWritable], classOf[org.apache.hadoop.io.BytesWritable])
        .map({ case (k, v) => new String(v.getBytes) })
    }

    def lastStringHadoopFiles(path: String, lastN: Int = 1): RDD[String] = {
      require(lastN > 0)
      val paths = sortedGlobPath(path)
      if (paths.size < lastN) {
        throw new Exception(s"Tried to get last $lastN files/dirs of path $path, but the resulting number of paths $paths is less than the required")
      } else {
        sc.stringHadoopFile(paths.mkString(","))
      }
    }
  }
}
