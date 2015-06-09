package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.ExecutionRetry
import ignition.core.jobs.utils.SparkContextUtils._
// filterAndGetTextFiles requires a date extractor:
import ignition.core.jobs.utils.SimplePathDateExtractor.default

// This job is also used as a Sanity Check for cluster initialization
object SitemapXMLSetup extends ExecutionRetry {

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val now = runnerContext.config.date

    val count = executeRetrying {
      val somedata = sc.textFile("/tmp/data/")

      somedata.count
    }

    println(s"Hello all $count somedata!")
  }
}
