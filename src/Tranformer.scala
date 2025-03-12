import Models.{EsdlAccOpenDate, EsdlPartyProd, EsdlRef, EsdlTransaction, PrmConfig, StgCertPayAmlReport}
import org.apache.spark.sql.functions.{col, concat_ws, current_date, current_timestamp, date_format, datediff, from_utc_timestamp, lit, regexp_extract, regexp_replace, to_date, when}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession, functions}

import scala.util.Try

object Tranformer {

  val spark: SparkSession = spark

  def joinEsdlTransactions(esdl: Dataset[EsdlTransaction], esdlPartyProd: Dataset[EsdlPartyProd], esdlRef: Dataset[EsdlRef],
                           openDate: Dataset[EsdlAccOpenDate])(certapay: Dataset[StgCertPayAmlReport]): Dataset[EsdlTransaction] = {
    import spark.implicits._

    val joinedData = certapay.join(esdl,
      when(col("transaction_id").isNotNull, col("transaction_id") === esdl("source_transaction_id"))
        .otherwise(col("cr_dr_ind") === lit("C") && col("instructing_agent_fi") =!= lit("CA000010") && col("account_servicer_reference") === col("source_transaction_id")),
      "left")

    import spark.implicits._
    val enrichedData = joinedData
      .withColumn("txn_id", when(col("cr_dr_ind") === "CRDT" && col("instructing_agent_fi") =!= "CA000010", col("account_servicer_reference")).otherwise(col("src_txn_id")))
      .withColumn("card_number", joinedData("card_number"))
      .withColumn("account_number", when(col("debtor_account").isNotNull, regexp_replace(functions.split(col("debtor_account"), "-")(2), "^0+", "")).otherwise(lit(null)))
      .withColumn("holding_branch_key", when(col("debtor_account").isNotNull, regexp_replace(functions.split(col("debtor_account"), "-")(2), "^0+", "")).otherwise(lit(null)))
      .withColumn("holding_branch_key", when(col("debtor_account").isNotNull, regexp_replace(functions.split(col("debtor_account"), "-")(1), "^0+", "")).otherwise(lit(null)))
      .withColumn("orig_process_date", lit("YYYY-MM-DD")) // Populate with fixed value as per rules
      .withColumn("loaded_to_cerebro", lit("N"))
      .withColumn("loaded_to_hunter", lit("Y"))
      .withColumn("opp_account_number", when(col("creditor_account").isNotNull, regexp_replace(functions.split(col("creditor_account"), "-")(2), "^0+", "")).otherwise(lit(null)))
      .withColumn("opp_branch_key", when(col("creditor_account").isNotNull, regexp_replace(functions.split(col("creditor_account"), "-")(1), "^0+", "")).otherwise(lit(null)))
      .withColumn("rules_cash_cad_equivalent_amt", lit(0.00))
      .withColumn("operation_type", when(col("operation_type").isNotNull, col("operation_type")).otherwise(lit(null))) // Join logic for operation_type
      .withColumn("on_status_code", col("payment_status"))
      //.withColumn("on_response_cd", when(col("on_response_cd").isNotNull, col("on_response_cd")).otherwise(lit(null))) // Join logic for on_response_cd
      .withColumn("rules_cad_equivalent_amt", col("cad_equivalent_amt"))
      .withColumn("emit_transfer_cd", col("interac_ref_num"))
      .withColumn("emit_recipient_name", col("creditor_name"))
      .withColumn("recipient_sms", col("creditor_mobile_no"))
      .withColumn("sender_email", col("debtor_email"))
      .withColumn("processing_date", current_date())
      //.withColumn("row_update_date", date_format(current_timestamp(), "YYYY-MM-DD HH:mm:ss")) // ETL to populate in format YYYY-MM-DD HH:mm:ss
      .withColumn("recipient_email", col("creditor_email_phone"))
      .withColumn("instr_agent_id", col("instructed_agent_fi"))
      .withColumn("instrg_agent_clearing_system", col("instructing_agent_fi"))
      .withColumn("txn_status", col("transaction_type"))
      .withColumn("txn_type", col("transaction_type_desc"))
      .withColumn("sndr_agt_name", col("debtor_name"))
      // .withColumn("utc_txn_date", date_format(col("acceptance_date_time"), "YYYY-MM-DD")) // Timestamps the date part of acceptance_date_time in UTC to target
      // .withColumn("utc_txn_time", date_format(col("acceptance_date_time"), "HH:mm:ss")) // Timeframe the time part of acceptance_date_time in UTC to target
      .withColumn("cust1_org_legal_name", col("debtor_legal_name"))
      .withColumn("cust2_org_legal_name", col("creditor_legal_name"))
      .withColumn("user_id", col("user_id")) // Join logic for user_id
      .withColumn("currency_conversion_rate", col("currency_conversion_rate"))
      .withColumn("user_device_type", col("user_device_type"))
      .withColumn("user_session_date_time", col("user_session_date_time"))
      .withColumn("transaction_memo_line_1", col("creditor_memo"))
      .withColumn("client_ip_addr", col("creditor_ip_address"))
      // .withColumn("debtor_id", col("debtor_id"))
      .withColumn("addi_field_10", col("creditor_id"))
      .withColumn("x_tran_exchange_rate", lit(null))
      .withColumn("addi_field_7", col("debtor_account"))
      .withColumn("addi_field_8", col("creditor_account"))
      .withColumn("addi_field_9", col("fraud_check_action"))
      .withColumn("source_transaction_id", when(col("cr_dr_ind").isin("CRDT") && !col("instructing_agent_fi").isin("CA000010"), col("account_servicer_Reference")).otherwise(col("src_txn_id")))
      //.withColumn("msg_type_code", col("msg_type_code"))
      .withColumn("acct_curr_cd", col("currency"))
      .withColumn("orig_curr_cd", col("currency"))
      .withColumn("orig_curr_amount", col("amount"))
      .withColumn("cad_equivalent_amt", col("amount"))
      .withColumn("orig_curr_cash_amount", lit(0.00))
      .withColumn("cash_cad_equivalent_amt", lit(0.00))
      .withColumn("acct_to_cad_rate", lit(1))
      .withColumn("txn_to_acct_rate", lit(1))
      .withColumn("txn_to_cad_rate", lit(1))
      .withColumn("txn_curr_rate_ind", lit("TXN"))

    // Lookup logic for instrg_agent_clearing_system
    val enrichedWithClearingSystem = enrichedData
      .join(esdlRef, esdlRef("source_id") === lit("CERTAPAY") && esdlRef("typ") === lit("financial_institution_number") &&
        col("processing_date").between(esdlRef("effective_from"), esdlRef("effective_to")) &&
        col("instrg_agent_clearing_system") === esdlRef("source_json.fl_num")
        || col("instr_agent_id") === esdlRef("source_json.fl_num") &&
        col("instr_agent_id") === esdlRef("source_json.fl_num"), "left")
      .withColumn("cust1_bank_name", when(esdlRef("target_json.full_name").isNotNull, esdlRef("target_json.full_name")).otherwise(lit("UNKN")))
      .withColumn("cust2_bank_name", when(esdlRef("target_json.full_name").isNotNull, esdlRef("target_json.full_name")).otherwise(lit("UNKN")))
      .withColumn("instrg_agent_clearing_system", when(col("target_json.full_name").isNotNull, col("target_json.full_name")).otherwise(lit("UNKN")))
      .withColumn("source_system_cd", when( col("typ") === "source_system" && col("source_id") === "RDM_DFLT" && col("source_cd") === "CERTAPAY" && col("processing_date").between(col("effective_from"), col("effective_to")), col("target_cd")).otherwise("UNKN"))
      .withColumn("channel_cd", lit("04"))
      .withColumn("execution_local_date_time", col("execution_local_date_time"))
      .withColumn("posting_date", to_date(from_utc_timestamp(col("acceptance_date_time"), "EST")))
      .withColumn("cr_dr_code", when(col("cr_dr_ind") === "CRDT", lit("C")).when(col("cr_dr_ind") === "DBIT", lit("D")))
      .withColumn("cash_code", lit("N"))



    println("second")
    enrichedWithClearingSystem.show()



    joinedData.as[EsdlTransaction]



  }
}
