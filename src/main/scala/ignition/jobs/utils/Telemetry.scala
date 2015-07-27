package ignition.jobs.utils

import com.timgroup.statsd.NonBlockingStatsDClient

object Telemetry {

  private lazy val statsd = new NonBlockingStatsDClient("ignition.search", "localhost", 8125)

  def errorDetected(error: String) {
    statsd.increment(error)
  }

}