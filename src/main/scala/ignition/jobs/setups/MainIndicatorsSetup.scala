package ignition.jobs.setups

import java.util.concurrent.Executors

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs._
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

object MainIndicatorsSetup extends SearchETL {

  override lazy val logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(1).withTimeAtStartOfDay())
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.withTime(23, 59, 59, 999))

    val timeoutSaveOperation: FiniteDuration = 30 minutes

    logger.info(s"Starting MainIndicatorsETL for start=$start, end=$end")

    logger.info(s"Parsing Search logs...")
    val searchLogs = parseSearchLogs(sc, start, end).persist()
    logger.info(s"Parsing AutoComplete logs...")
    val autoCompleteLogs = parseAutoCompleteLogs(sc, start, end).persist()
    logger.info(s"Parsing Click logs...")
    val clickLogs = parseClickLogs(sc, start, end).persist()

    val results = MainIndicators.process(searchLogs, autoCompleteLogs, clickLogs)
    var i = 0
    results.foreach {
        rdd => {
          rdd.saveAsTextFile(s"/tmp/out_$i.txt")
          i = i + 1
        }
      }

  }

//    saveToDashBoard.onComplete {
//      case Success(_) => logger.info("TransactionETL - GREAT SUCCESS")
//      case Failure(exception) => logger.error("Error on saving metrics to dashboard API", exception)
//    }
//
//    try {
//      Await.ready(saveToDashBoard, timeoutSaveOperation)
//    } catch {
//      case NonFatal(exception) =>
//        logger.error("Error on saving metrics to dashboard API", exception)
//    }
//  }

}
