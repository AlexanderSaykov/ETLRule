import Schemas.{taxTransactionAutoTaxItemPattern, taxTransactionRnu45}
import org.apache.spark.sql.DataFrame

object DataSourses extends SparkSession {

  val taxTransactionDf = spark.read.option("multiline", value = false).schema(taxTransactionRnu45).json("tables/for_auto_knu.json")

  val autoKnuRules = spark.read.option("multiline", value = false).schema(taxTransactionAutoTaxItemPattern).json("tables/rule.json")

  val incomeExpenseItemDf: DataFrame = spark.read.option("multiline", value = false).json("tables/income_expense_item.json")

}
