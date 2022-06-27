import Constants._
import Schemas._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Date, Timestamp}
import java.time.LocalDate

case class AutoKnuRule(
                        id_pk: Long,
                        dt_tax_item: String,
                        kt_tax_item: String,
                        dt_tax_item_ofr_list: String,
                        kt_tax_item_ofr_list: String,
                        account_mask_rule: String,
                        dt_account_mask_list: String,
                        kt_account_mask_list: String,
                        payment_purpose_rule: String,
                        payment_purpose_pattern_list: String,
                        start_date: Date,
                        end_date: Date,
                        updated_ts: Timestamp
                      )

class AutoKNUProcessor  (startDate: String,
                         endDate: String,
                         taxTransactionDf: DataFrame,
                         autoTaxItemRules: DataFrame,
                         incomeExpenseItemDf: DataFrame)  extends SparkSession {
  import spark.implicits._
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val startDateFromT1: LocalDate = LocalDate.parse(startDate)
  val endDateFromT1: LocalDate = LocalDate.parse(endDate)

  /**
   * Compares two start dates
   * @param pattern accepts AutoKnu pattern table
   * @return max start date
   */

  def getBeginUsage(pattern: AutoKnuRule): String = {
    val startDateFromPattern = LocalDate.parse(pattern.start_date.toString)
    if (startDateFromPattern.isAfter(startDateFromT1)) {
      startDateFromPattern.toString
    } else {
      startDateFromT1.toString
    }
  }

  /**
   * Compares two end dates
   * @param pattern accepts AutoKnu rule pattern table
   * @return min end date
   */

  def getEndUsage(pattern: AutoKnuRule): String = {
    val endDateFromPattern = LocalDate.parse(pattern.end_date.toString)
    if (endDateFromPattern.isAfter(endDateFromT1)) {
      endDateFromT1.toString
    } else {
      endDateFromPattern.toString
    }
  }

  /**
   * removes duplicates from table
   * @param column - KT_TAX_ITEM or DT_TAX_ITEM
   * @param df - dataframe
   * @return filtered dataframe without duplicates
   */

  def dropDuplicatesKnu(column: String, df: DataFrame) = {
    val windowSpec = Window.partitionBy(IdpK).orderBy(col("rule").asc)
    df.filter(col(column).isNotNull)
      .withColumn("rule", when(col(column) === "30000" || col(column) === "50000", 1).otherwise(2))
      .withColumn("rank", row_number.over(windowSpec))
      .filter(col("rank") === 1)
  }

  /**
   * creates filter condition
   * @param pattern - accepts AutoKnu rule pattern table
   * @return filter condition for dataframe
   */


  def createCondition(pattern: AutoKnuRule): Column = {

    val ofrCondition =
      Seq(
        createCondition(DtTaxItemOfr, pattern.dt_tax_item_ofr_list),
        createCondition(KtTaxItemOfr, pattern.kt_tax_item_ofr_list)
      )
        .flatten
        .reduceOption(_ && _)

    val accountCondition =
      Seq(
        createCondition(DtAccount, pattern.dt_account_mask_list),
        createCondition(KtAccount, pattern.kt_account_mask_list)
      )
        .flatten
        .reduceOption(_ && _) match {
        case None => None
        case Some(cond) => Option(if (pattern.account_mask_rule == Include) cond else not(cond))
      }

    val paymentCondition = createCondition(PaymentPurpose, pattern.payment_purpose_pattern_list) match {
      case None => None
      case Some(cond) => Option(if (pattern.payment_purpose_rule == Include) cond else not(cond))
    }

    Seq(
      ofrCondition, accountCondition, paymentCondition
    )
      .flatten
      .reduceOption(_ && _).getOrElse(lit(false))

  }

  /**
   * creates filter condition for one column
   * @param columnName - name of column to be filtered
   * @param columnValue - list of values for filter
   * @return condition for using in filter
   */


  def createCondition(columnName: String, columnValue: String): Option[Column] = {

    columnValue match {
      case null | "" => None
      case _ => Option(columnValue.split("(?<!/),")
        .map(_.replace("/,", ","))
        .map(_.trim.replace('?', '_'))
        .map(col(columnName).like)
        .reduce(_ || _)
      )
    }

  }

  /**
   * Checks the pattern rules for being correct according to business logic
   * @param column - name of column to be checked
   * @return filtered and corrected dataframe
   */

  def checkPattern(column: String): DataFrame = {

    val tax_item = if (column == DtTaxItemOfrList) DtTaxItem
    else KtTaxItem

    val check = autoTaxItemRules
      .filter(!col(column).rlike("[%?]"))
      .filter(col(column) =!= "" && col(column).isNotNull)
      .join(incomeExpenseItemDf.select(TaxItemOfr, TaxItemCode, ActualFrom, ActualTo), autoTaxItemRules(column) === incomeExpenseItemDf(TaxItemOfr), "inner")
      .dropDuplicates()
      .filter(col(tax_item) === col(TaxItemCode))
      .withColumn(EndDate, least(col(EndDate), lit(endDate).cast(DateType)))
      .filter(col(ActualFrom) <= col(EndDate) && (col(ActualTo) >= col(EndDate) || col(ActualTo).isNull))
      .select(IdpK).map(f => f.getLong(0)).collect.toList

    autoTaxItemRules
      .filter(col(IdpK).isin(check: _*))
      .union(autoTaxItemRules.filter(col(tax_item) === 30000 || col(tax_item) === 50000))
  }

  /**
   * main transform method
   * @return returns transformed dataframe according to business logic
   */


  def run(): DataFrame = {
    if (autoTaxItemRules.take(1).isEmpty) {

      logger.warn("[TECH] Table tax_transaction_auto_tax_item_pattern is empty. Nothing will be written into T_auto")
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], taxTransactionRnu45AutoTaxItemCorrection)
    } else {
      val autoKnuRulesAfterCheck = checkPattern(DtTaxItemOfrList).union(checkPattern(KtTaxItemOfrList))

      logger.warn(s"[TECH] TaxItemRules table is checked. ids of applied rules are ${autoKnuRulesAfterCheck.select(IdpK).map(f => f.getLong(0)).collect.toList.sorted}")

      val autoKnuRules: Array[AutoKnuRule] = autoKnuRulesAfterCheck.as[AutoKnuRule].collect()

      val filteredTransactionsDf = autoKnuRules
        .map(createCondition)
        .map(taxTransactionDf.filter)
        .zip(autoKnuRules)
        .map { case (x: DataFrame, y: AutoKnuRule) => x
          .filter(to_date(col(OperationDate), DateFmtPattern) <= getEndUsage(y) &&
            to_date(col(OperationDate), DateFmtPattern) >= getBeginUsage(y))
          .withColumn(DtTaxItem, when(x.col(DtAccount).like("7%"), lit(y.dt_tax_item)))
          .withColumn(KtTaxItem, when(x.col(KtAccount).like("7%"), lit(y.kt_tax_item)))
        }
        .reduce(_ union _)
        .withColumn(KtTaxItem, when(col(KtTaxItem) === "", null).otherwise(col(KtTaxItem)))
        .withColumn(DtTaxItem, when(col(DtTaxItem) === "", null).otherwise(col(DtTaxItem)))

      val finalReport = dropDuplicatesKnu(KtTaxItem, filteredTransactionsDf).union(dropDuplicatesKnu(DtTaxItem, filteredTransactionsDf))
        .withColumn(CreatedTs, current_timestamp().as(CreatedTs))
        .select(col(IdpK),
          to_date(col(InputDt), DateFmtPattern).as(InputDt),
          col(DtTaxItem).as(DtTaxItemAuto),
          col(KtTaxItem).as(KtTaxItemAuto),
          col(OperationDate),
          col(CreatedTs))

      /**
       * according to business logic there should not be empty DT_TAX_ITEM_AUTO
       * and KT_TAX_ITEM_AUTO columns. This methods aggregate df, so there is
       * no empty DT_TAX_ITEM_AUTO and KT_TAX_ITEM_AUTO columns
       * @param column - name of column
       * @return aggregated dataframe
       */


      def taxItemAggregation(column: String) = {
        finalReport.groupBy(IdpK).agg(max(column).as(column))
      }

      val finalReportDtAgg = taxItemAggregation(DtTaxItemAuto)
      val finalReportKtAgg = taxItemAggregation(KtTaxItemAuto)

      val joinedTables = finalReportDtAgg.join(finalReportKtAgg, Seq(IdpK), "inner")

      finalReport.drop(DtTaxItemAuto, KtTaxItemAuto).join(joinedTables, Seq(IdpK), "inner").dropDuplicates()
    }
  }
}

object AutoKNUProcessor {
  def apply(startDate: String,
            endDate: String,
            taxTransactionDf: DataFrame,
            autoTaxItemRulesDf: DataFrame,
            incomeExpenseItemDf: DataFrame) = new AutoKNUProcessor(startDate, endDate, taxTransactionDf, autoTaxItemRulesDf, incomeExpenseItemDf)
}