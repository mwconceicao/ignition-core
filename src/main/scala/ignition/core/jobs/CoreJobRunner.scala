package ignition.core.jobs

import org.apache.spark.SparkContext
import org.joda.time.DateTime

object CoreJobRunner {

  case class RunnerContext(sparkContext: SparkContext,
                           config: RunnerConfig)


  case class RunnerConfig(setupName: String = "nosetup",
                          date: DateTime = DateTime.now,
                          tag: String = "notag",
                          user: String = "nouser",
                          master: String = "local[*]")

  def runJobSetup(args: Array[String], jobsSetups: Map[String, (RunnerContext) => Unit]) {
    val parser = new scopt.OptionParser[RunnerConfig]("Runner") {
      help("help") text("prints this usage text")
      arg[String]("<setup-name>") required() action { (x, c) =>
        c.copy(setupName = x)
      } text(s"one of ${jobsSetups.keySet}")
      opt[String]('d', "date") action { (x, c) =>
        c.copy(date = new DateTime(x))
      }
      opt[String]('t', "tag") action { (x, c) =>
        c.copy(tag = x)
      }
      opt[String]('u', "user") action { (x, c) =>
        c.copy(user = x)
      }
      opt[String]('m', "master") action { (x, c) =>
        c.copy(master = x)
      }
    }

    parser.parse(args, RunnerConfig()) map { config =>
      val jobSetup = jobsSetups.get(config.setupName)

      require(jobSetup.isDefined,
        s"Invalid job setup ${config.setupName}, available jobs setups: ${jobsSetups.keySet}")

      val appName = s"${config.setupName}.${config.tag}"
      val sc = new SparkContext(config.master, appName,
        System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)

      val context = RunnerContext(sc, config)

      jobSetup.get.apply(context)

      sc.stop()
    }
  }
}
