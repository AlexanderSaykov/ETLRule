
import org.apache.spark.sql.types._

object Schemas {

  val taxTransactionRnu45AutoTaxItemCorrection: StructType = StructType(List(
    StructField("id_pk", StringType),
    StructField("input_dt", DateType),
    StructField("dt_tax_item_auto", StringType),
    StructField("kt_tax_item_auto", StringType),
    StructField("created_ts", TimestampType)
  ))

  val taxTransactionRnu45: StructType = StructType(
    List(
      StructField("id_pk", StringType),
      StructField("transaction_date", TimestampType),
      StructField("value_date", TimestampType),
      StructField("asnu_code", StringType),
      StructField("dt_account", StringType),
      StructField("kt_account", StringType),
      StructField("amount", DecimalType(38, 10)),
      StructField("payment_purpose", StringType),
      StructField("dt_balance_account", StringType),
      StructField("dt_tax_item_ofr", StringType),
      StructField("kt_balance_account", StringType),
      StructField("kt_tax_item_ofr", StringType),
      StructField("is_drpl", StringType),
      StructField("mem_order_number", StringType),
      StructField("mem_order_date", TimestampType),
      StructField("document_type", StringType),
      StructField("document_number", StringType),
      StructField("document_date", StringType),
      StructField("user_name", StringType),
      StructField("ctl_validfrom", TimestampType),
      StructField("operation_date", TimestampType, nullable = false),
      StructField("is_spod", BooleanType),
      StructField("usl_div_sk", LongType),
      StructField("usl_div_dp_code", StringType),
      StructField("usl_div_code", StringType),
      StructField("input_dt", DateType)
    )
  )

  val taxTransactionAutoTaxItemPattern: StructType = StructType(List(
    StructField("id_pk", LongType),
    StructField("dt_tax_item", StringType),
    StructField("kt_tax_item", StringType),
    StructField("dt_tax_item_ofr_list", StringType),
    StructField("kt_tax_item_ofr_list", StringType),
    StructField("account_mask_rule", StringType),
    StructField("dt_account_mask_list", StringType),
    StructField("kt_account_mask_list", StringType),
    StructField("payment_purpose_rule", StringType),
    StructField("payment_purpose_pattern_list", StringType),
    StructField("start_date", DateType),
    StructField("end_date", DateType),
    StructField("description", StringType),
    StructField("user_login", StringType),
    StructField("updated_ts", TimestampType)
  ))


}
