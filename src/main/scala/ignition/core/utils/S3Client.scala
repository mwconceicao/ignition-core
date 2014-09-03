package ignition.core.utils

import java.util.Properties

import org.jets3t.service.{Constants, Jets3tProperties}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.{S3Bucket, S3Object}
import org.jets3t.service.security.AWSCredentials


class S3Client {

  val jets3tProperties = {
    val jets3tProperties = Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
    val properties = new Properties()
    properties.put("httpclient.max-connections", "50") // The maximum number of simultaneous connections to allow globally
    properties.put("httpclient.retry-max", "20") // How many times to retry connections when they fail with IO errors

    jets3tProperties.loadAndReplaceProperties(properties, "chaordic'")
    jets3tProperties
  }

  val service = new RestS3Service(
    new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")),
    null, null, jets3tProperties
  )

  def writeContent(bucket: String, key: String, content: String): S3Object = {
    val obj = new S3Object(key, content)
    obj.setContentType("text/plain")
    service.putObject(bucket, obj)
  }

  def readContent(bucket: String, key: String): S3Object = {
    service.getObject(new S3Bucket(bucket), key)
  }

  def list(bucket: String, key: String): Array[S3Object] = {
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
