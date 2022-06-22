import ApplicationArguments.{END_DATE, START_DATE, getOrException}
import DataSourses.{autoKnuRules, incomeExpenseItemDf, taxTransactionDf}
import com.concurrentthought.cla.Args
import org.slf4j.{Logger, LoggerFactory}



object Application extends SparkSession {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val finalArgs: Args = ApplicationArguments(args).parsedInitialArgs


    logger.info(s"[TECH] Start application with args: ${finalArgs.allValues} and failures args: ${finalArgs.failures}")

    val startDate: String = getOrException[String](finalArgs, START_DATE, required = true)
    val endDate: String = getOrException[String](finalArgs, END_DATE, required = true)

    val processor = AutoKNUProcessor(startDate, endDate, taxTransactionDf, autoKnuRules, incomeExpenseItemDf)

    processor.run().write.partitionBy("input_dt").parquet("folder")

  }
}
