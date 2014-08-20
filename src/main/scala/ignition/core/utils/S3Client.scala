package ignition.core.utils

import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import org.jets3t.service.security.AWSCredentials


class S3Client {
  val service = new RestS3Service(new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")))

  def writeContent(bucket: String, key: String, content: String) {
    val obj = new S3Object(key, content)
    obj.setContentType("text/plain")
    service.putObject(bucket, obj)
  }

  def fileExists(bucket: String, key: String): Boolean = {
    try {
      service.getObjectDetails(bucket, key, null, null, null, null)
      true
    } catch {
      case e: org.jets3t.service.S3ServiceException if e.getResponseCode == 404 => false
    }
  }
}
