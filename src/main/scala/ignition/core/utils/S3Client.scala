package ignition.core.utils

import java.io.{BufferedReader, InputStreamReader}

import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.{S3Bucket, S3Object}
import org.jets3t.service.security.AWSCredentials


class S3Client {
  val service = new RestS3Service(new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")))

  def writeContent(bucket: String, key: String, content: String) {
    val obj = new S3Object(key, content)
    obj.setContentType("text/plain")
    service.putObject(bucket, obj)
  }

  def readContent(bucket: String, key: String): S3Object = {
    service.getObject(new S3Bucket(bucket), key)
  }

  def listNameFiles(bucket: String, key: String): Array[S3Object] = {
    service.listObjects(new S3Bucket(bucket), key, null)
  }

  def fileExists(bucket: String, key: String): Boolean = {
    try {
      service.getObjectDetails(new S3Bucket(bucket), key)
      true
    } catch {
      case e: org.jets3t.service.S3ServiceException if e.getResponseCode == 404 => false
    }
  }
}
