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
    val windowSpec = Window.partitionBy(ID_PK).orderBy(col("rule").asc)
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
        createCondition(DT_TAX_ITEM_OFR, pattern.dt_tax_item_ofr_list),
        createCondition(KT_TAX_ITEM_OFR, pattern.kt_tax_item_ofr_list)
      )
        .flatten
        .reduceOption(_ && _)

    val accountCondition =
      Seq(
        createCondition(DT_ACCOUNT, pattern.dt_account_mask_list),
        createCondition(KT_ACCOUNT, pattern.kt_account_mask_list)
      )
        .flatten
        .reduceOption(_ && _) match {
        case None => None
        case Some(cond) => Option(if (pattern.account_mask_rule == INCLUDE) cond else not(cond))
      }

    val paymentCondition = createCondition(PAYMENT_PURPOSE, pattern.payment_purpose_pattern_list) match {
      case None => None
      case Some(cond) => Option(if (pattern.payment_purpose_rule == INCLUDE) cond else not(cond))
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

    val tax_item = if (column == DT_TAX_ITEM_OFR_LIST) DT_TAX_ITEM
    else KT_TAX_ITEM

    val check = autoTaxItemRules
      .filter(!col(column).rlike("[%?]"))
      .filter(col(column) =!= "" && col(column).isNotNull)
      .join(incomeExpenseItemDf.select(TAX_ITEM_OFR, TAX_ITEM_CODE, ACTUAL_FROM, ACTUAL_TO), autoTaxItemRules(column) === incomeExpenseItemDf(TAX_ITEM_OFR), "inner")
      .dropDuplicates()
      .filter(col(tax_item) === col(TAX_ITEM_CODE))
      .withColumn(END_DATE, least(col(END_DATE), lit(endDate).cast(DateType)))
      .filter(col(ACTUAL_FROM) <= col(END_DATE) && (col(ACTUAL_TO) >= col(END_DATE) || col(ACTUAL_TO).isNull))
      .select(ID_PK).map(f => f.getLong(0)).collect.toList

    autoTaxItemRules
      .filter(col(ID_PK).isin(check: _*))
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
      val autoKnuRulesAfterCheck = checkPattern(DT_TAX_ITEM_OFR_LIST).union(checkPattern(KT_TAX_ITEM_OFR_LIST))

      logger.warn(s"[TECH] TaxItemRules table is checked. ids of applied rules are ${autoKnuRulesAfterCheck.select(ID_PK).map(f => f.getLong(0)).collect.toList.sorted}")

      val autoKnuRules: Array[AutoKnuRule] = autoKnuRulesAfterCheck.as[AutoKnuRule].collect()

      val filteredTransactionsDf = autoKnuRules
        .map(createCondition)
        .map(taxTransactionDf.filter)
        .zip(autoKnuRules)
        .map { case (x: DataFrame, y: AutoKnuRule) => x
          .filter(to_date(col(OPERATION_DATE), DATE_FMT_PATTERN) <= getEndUsage(y) &&
            to_date(col(OPERATION_DATE), DATE_FMT_PATTERN) >= getBeginUsage(y))
          .withColumn(DT_TAX_ITEM, when(x.col(DT_ACCOUNT).like("7%"), lit(y.dt_tax_item)))
          .withColumn(KT_TAX_ITEM, when(x.col(KT_ACCOUNT).like("7%"), lit(y.kt_tax_item)))
        }
        .reduce(_ union _)
        .withColumn(KT_TAX_ITEM, when(col(KT_TAX_ITEM) === "", null).otherwise(col(KT_TAX_ITEM)))
        .withColumn(DT_TAX_ITEM, when(col(DT_TAX_ITEM) === "", null).otherwise(col(DT_TAX_ITEM)))

      val finalReport = dropDuplicatesKnu(KT_TAX_ITEM, filteredTransactionsDf).union(dropDuplicatesKnu(DT_TAX_ITEM, filteredTransactionsDf))
        .withColumn(CREATED_TS, current_timestamp().as(CREATED_TS))
        .select(col(ID_PK),
          to_date(col(INPUT_DT), DATE_FMT_PATTERN).as(INPUT_DT),
          col(DT_TAX_ITEM).as(DT_TAX_ITEM_AUTO),
          col(KT_TAX_ITEM).as(KT_TAX_ITEM_AUTO),
          col(OPERATION_DATE),
          col(CREATED_TS))

      /**
       * according to business logic there should not be empty DT_TAX_ITEM_AUTO
       * and KT_TAX_ITEM_AUTO columns. This methods aggregate df, so there is
       * no empty DT_TAX_ITEM_AUTO and KT_TAX_ITEM_AUTO columns
       * @param column - name of column
       * @return aggregated dataframe
       */


      def taxItemAggregation(column: String) = {
        finalReport.groupBy(ID_PK).agg(max(column).as(column))
      }

      val finalReportDtAgg = taxItemAggregation(DT_TAX_ITEM_AUTO)
      val finalReportKtAgg = taxItemAggregation(KT_TAX_ITEM_AUTO)

      val joinedTables = finalReportDtAgg.join(finalReportKtAgg, Seq(ID_PK), "inner")

      finalReport.drop(DT_TAX_ITEM_AUTO, KT_TAX_ITEM_AUTO).join(joinedTables, Seq(ID_PK), "inner").dropDuplicates()
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