import DataSourses.{autoKnuRules, incomeExpenseItemDf, taxTransactionDf}
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite


class KnuTest extends AnyFunSuite with SparkSession{

 val startDate: String = "1800-03-01"
  val endDate: String = "9999-03-01"



 val AutoKNUdf = new AutoKNUProcessor(startDate, endDate, taxTransactionDf, autoKnuRules, incomeExpenseItemDf).run()

 AutoKNUdf.cache
 AutoKNUdf.count

  /**
   * Rule 1
   */
  test("1 Set dt_tax_item = 21495 if payment_purpose is  RNU57(D)-fis-(AUTO_KNU) and dt_tax_item_ofr = 47803.99") {
    assertResult(1)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011728").count())
  }

  test("2 Got empty dataset because there is no kt_tax_item_ofr for this id_pk") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011729").count())
  }

  test("3 Got empty dataset because there is no payment_purpose match for this id_pk") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011730").count())
  }

  test("4 Set dt_tax_item = = 21495 if payment_purpose is RNU46(D)-fis-(AUTO_KNU), and dt_tax_item_ofr = 29407.98") {
    assertResult(1)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011731").count())
  }

  test("5 Got empty dataset because there is no kt_tax_item_of match for this id_pk") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011732").count())
  }

  test("6 Got empty dataset because there is no payment_purpose match for this id_pk") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011733").count())
  }

  /**
   * Rule 2
   */
  test("7 Set kt_tax_item = = 30000 if kt_tax_item_ofr = 11117% and payment_purpose is RNU57(D)-fis-(AUTO_KNU) != 'NOT_AUTO_KNU' and dt_account_mask != 47423?????????????37%") {
    assertResult(1)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011734").count())
  }

  test("8 Got empty dataset because there is no dt_account_mask match for this id_pk") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011735").count())
  }

  test("9 Got empty dataset because there is no payment_purpose match for this id_pk") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011736").count())
  }

  test("10 Got empty dataset because there is no dt_tax_item_of match for this id_pk") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011737").count())
  }

  test("11 Set kt_tax_item = = 30000 if kt_tax_item_ofr = 21102% and payment_purpose is != 'NOT_AUTO_KNU' and dt_account_mask != 47423?????????????37%") {
    assertResult(1)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000011738").count())
  }

  test("12 Nor rules found") {
    val AutoKNUdf = new AutoKNUProcessor("2021-04-01", "2021-04-01", taxTransactionDf, autoKnuRules, incomeExpenseItemDf)
    assertResult(0) (AutoKNUdf.run().count)
  }

  test("13 dt_account should pass like7% filter") {
    assertResult(0)(AutoKNUdf.filter(col("id_pk") === "Z1000001-7781-301v-8b56-000000002383").count())
  }

}
