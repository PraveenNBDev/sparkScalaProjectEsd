import Models.{EsdlAccOpenDate, EsdlPartyProd, EsdlRef, EsdlTransaction, PrmConfig, StgCertPayAmlReport}
import org.apache.spark.sql.functions.{coalesce, col, concat_ws, current_date, current_timestamp, date_format, datediff, from_utc_timestamp, get_json_object, lit, regexp_extract, regexp_replace, to_date, when}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession, functions}

object Tranformer {

  val spark: SparkSession = spark
  import org.apache.log4j.{Logger, Level}
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.spark_project").setLevel(Level.INFO)

  def joinEsdlWithCertapay(esdlTransactions: Dataset[EsdlTransaction], esdlRef: Dataset[EsdlRef])(stgCertapay: Dataset[StgCertPayAmlReport]):
  DataFrame = {
    val esdlOLB = esdlTransactions.filter(col("source_system_cd") === "OLB")

    val dateFormat = "yyyy-MM-dd"

    val esdlTransactionsDs = esdlOLB.alias("esdl").withColumn("account_number_esdl", col("account_number"))
      .withColumn("msg_type_code_esdl", col("msg_type_code"))
      .withColumn("holding_branch_key_esdl", col("holding_branch_key"))
      .withColumn("account_key_prod", col("account_key"))
    val stgCertapayDs = stgCertapay.alias("stg").withColumn("account_number_pay", col("account_number"))
      .withColumn("debtor_id_pay", col("debtor_id"))
      .withColumn("account_key_pay", col("account_key"))
      .withColumn("product_type_code_pay", col("product_type_code"))

    // Build complex join conditions
    val joinCondition = {
      val fiRefCondition = (
        (stgCertapayDs.col("transaction_id").isNotNull &&
          stgCertapayDs.col("transaction_id") === esdlTransactionsDs.col("source_transaction_id"))
          ||
          (stgCertapayDs.col("transaction_id").isNull &&
            stgCertapayDs.col("cr_dr_ind") === "C" &&
            stgCertapayDs.col("instructing_agent_fi") =!= "CA000010" &&
            stgCertapayDs.col("account_servicer_reference") === esdlTransactionsDs.col("source_transaction_id"))
        )

      val amountCondition = stgCertapayDs.col("amount") === esdlTransactionsDs.col("orig_curr_amount")

      /*
      // Date window condition (±5 days)
      val esdlDate = coalesce(
        to_date(col("esdl.execution_local_date_time"),
          lit("1970-01-01")))
      val stgDate = coalesce(
        to_date(col("stg.acceptance_date_time"),
          lit("1970-01-01")))
      val dateCondition = functions.abs(datediff(esdlDate, stgDate)) <= 5

       */

      fiRefCondition && amountCondition
    }

    val joinedDF = esdlTransactionsDs.join(stgCertapayDs, joinCondition, "left")

    joinedDF.select(
      col("esdl.source_transaction_id"),
      col("stg.transaction_id"),
      col("esdl.execution_local_date_time"),
      col("stg.acceptance_date_time"),
      col("esdl.orig_curr_amount"),
      col("stg.amount"),
      col("stg.cr_dr_ind"),
      col("stg.instructing_agent_fi"),
      col("account_number_esdl"),
      col("esdl.account_key")
    )

    import spark.implicits._
    val enrichedData = joinedDF
      .withColumn("txn_id", when(col("cr_dr_ind") === "CRDT" && col("instructing_agent_fi") =!= "CA000010", col("account_servicer_reference")).otherwise(col("src_txn_id")))
      .withColumn("card_number", joinedDF("card_number"))
      .withColumn("account_number1", when(col("debtor_account").isNotNull, regexp_replace(functions.split(col("debtor_account"), "-")(2), "^0+", "")).otherwise(lit(null)))
      .withColumn("holding_branch_key1", when(col("debtor_account").isNotNull, regexp_replace(functions.split(col("debtor_account"), "-")(2), "^0+", "")).otherwise(lit(null)))
      .withColumn("orig_process_date", lit("YYYY-MM-DD"))
      .withColumn("loaded_to_cerebro", lit("N"))
      .withColumn("loaded_to_hunter", lit("Y"))
      .withColumn("opp_account_number", when(col("creditor_account").isNotNull, regexp_replace(functions.split(col("creditor_account"), "-")(2), "^0+", "")).otherwise(lit(null)))
      .withColumn("opp_branch_key", when(col("creditor_account").isNotNull, regexp_replace(functions.split(col("creditor_account"), "-")(1), "^0+", "")).otherwise(lit(null)))
      .withColumn("rules_cash_cad_equivalent_amt", lit(0.00))
      .withColumn("rules_cad_equivalent_amt", col("cad_equivalent_amt"))
      .withColumn("operation_type", when(col("operation_type").isNotNull, col("operation_type")).otherwise(lit(null)))
      .withColumn("on_status_code", col("payment_status"))
      .withColumn("emt_transfer_cd", col("interac_ref_num"))
      .withColumn("emt_recipient_name", col("creditor_name"))
      .withColumn("recipient_sms", col("creditor_mobile_no"))
      .withColumn("sender_email", col("debtor_email"))
      .withColumn("processing_date", current_date())
      .withColumn("row_update_date", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("recipient_email", col("creditor_email_phone"))
      .withColumn("instr_agent_id", col("instructed_agent_fi"))
      .withColumn("instrg_agent_clearing_system", col("instructing_agent_fi"))
      .withColumn("txn_status", col("transaction_type"))
      .withColumn("txn_type", col("transaction_type_desc"))
      .withColumn("sndr_agt_name", col("debtor_name"))
      .withColumn("utc_txn_date", date_format(col("acceptance_date_time"), "yyyy-MM-dd"))
      .withColumn("utc_txn_time", date_format(col("acceptance_date_time"), "HH:mm:ss"))
      .withColumn("cust1_org_legal_name", col("debtor_legal_name"))
      .withColumn("cust2_org_legal_name", col("creditor_legal_name"))
      .withColumn("user_id", col("user_id")) // Join logic for user_id
      .withColumn("currency_conversion_rate", col("currency_conversion_rate"))
      .withColumn("user_device_type", col("user_device_type"))
      .withColumn("user_session_date_time", col("user_session_date_time"))
      .withColumn("transaction_memo_line_1", col("creditor_memo"))
      .withColumn("client_ip_addr", col("creditor_ip_address"))
      .withColumn("debtor_id1", col("debtor_id_pay"))
      .withColumn("addl_field_10", col("creditor_id"))
      .withColumn("fx_tran_exchange_rate", lit(null))
      .withColumn("addl_field_7", col("debtor_account"))
      .withColumn("addl_field_8", col("creditor_account"))
      .withColumn("addl_field_9", col("fraud_check_action"))
      .withColumn("source_transaction_id", when(col("cr_dr_ind").isin("CRDT") && !col("instructing_agent_fi").isin("CA000010"), col("account_servicer_Reference")).otherwise(col("src_txn_id")))
      .withColumn("msg_type_code1", col("msg_type_code_esdl"))
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
      .withColumn("acct_curr_amount", col("amount"))
      .withColumn("txn_status_code", col("payment_status"))
      .withColumn("txn_response_cd", col("txn_response_cd"))
      .withColumn("instg_agent_clearing_system", col("instructing_agent_fi"))
      .withColumn("cr_dr_code", when(col("cr_dr_ind") === "CRDT", lit("C")).when(col("cr_dr_ind") === "DBIT", lit("D")))
      .withColumn("channel_cd", lit("04"))
      .withColumn("execution_local_date_time", col("execution_local_date_time"))
      .withColumn("posting_date", to_date(from_utc_timestamp(col("acceptance_date_time"), "EST")))
      .withColumn("cash_code", lit("N"))
      .withColumn("orig_curr_cash_amount", lit("0.00"))
      .withColumn("cash_cad_equivalent_amt", lit("0.00"))
      .withColumn("acct_to_cad_rate", lit("1"))
      .withColumn("txn_to_acc_rate", lit("1"))
      .withColumn("txn_to_cad_rate", lit("1"))
      .withColumn("txn_curr_rate_ind", lit("TXN"))
      .withColumn("orig_process_date", date_format(col("acceptance_date_time"), "yyyy-MM-dd"))
      .withColumn("loaded_to_cerebro", lit("N"))
      .withColumn("loaded_to_hunter", lit("Y"))
      .withColumn("txn_tran_exchange_rate", lit("null"))

    // Lookup logic for instrg_agent_clearing_system
    val cust1Enriched = enrichedData
      .join(esdlRef, esdlRef("source_id") === lit("CERTAPAY") && esdlRef("typ") === lit("financial_institution_number") &&
        col("processing_date").between(esdlRef("effective_from"), esdlRef("effective_to")) &&
        col("instrg_agent_clearing_system") ===  esdlRef("source_json"), "left") // will impliment json parsing later
      .withColumn("cust1_bank_name", when(esdlRef("target_json").isNotNull, esdlRef("target_json")).otherwise(lit("UNKN")))
      .withColumn("source_system_cd", lit(null))
      .withColumn("target_cd", lit(null))

    val cust2Enriched = enrichedData
      .join(esdlRef, esdlRef("source_id") === lit("CERTAPAY") && esdlRef("typ") === lit("financial_institution_number") &&
        col("processing_date").between(esdlRef("effective_from"), esdlRef("effective_to")) &&
        col("instr_agent_id") ===  esdlRef("source_json"), "left")
      .withColumn("cust2_bank_name", when(esdlRef("target_json").isNotNull, esdlRef("target_json")).otherwise(lit("UNKN")))
      .withColumn("source_system_cd", lit(null))
      .withColumn("target_cd", lit(null))

    val srcSysCdEnriched = enrichedData
      .join(esdlRef, col("processing_date").between(esdlRef("effective_from"), esdlRef("effective_to")), "left")
      .withColumn("source_system_cd", when( esdlRef("typ").isin("source_system") && esdlRef("source_id").isin("RDM_DFLT")
        && col("source_cd").isin("CERTAPAY") && col("processing_date").between(col("effective_from"), col("effective_to")), esdlRef("target_cd")).otherwise("UNKN"))
      .withColumn("instr_agent_id", lit(null))
      .withColumn("cust1_bank_name", lit(null))
      .withColumn("cust2_bank_name", lit(null))

    val unionWithBothDs = cust1Enriched.union(cust2Enriched).union(srcSysCdEnriched)

    val ds = unionWithBothDs.select(
      col("txn_id"),
      col("card_number"),
      col("account_number1").as("account_number"),
      col("holding_branch_key1").as("holding_branch_key"),
      col("source_transaction_id"),
      col("execution_local_date_time"),
      col("posting_date"),
      col("msg_type_code1").as("msg_type_code"),
      col("acct_curr_cd"),
      col("acct_curr_amount"),
      col("orig_curr_cd"),
      col("orig_curr_amount"),
      col("cad_equivalent_amt"),
      col("ip_address"),
      col("opp_account_number"),
      col("opp_branch_key"),
      col("operation_type"),
      col("txn_status_code"),
      col("txn_response_cd"),
      col("rules_cad_equivalent_amt"),
      col("instrg_agent_clearing_system"),
      col("rules_cash_cad_equivalent_amt"),
      col("emt_transfer_cd"),
      col("emt_recipient_name"),
      col("recipient_sms"),
      col("sender_email"),
      col("instr_agent_id"),
      col("instg_agent_clearing_system"),
      col("txn_status"),
      col("txn_type"),
      col("sndr_agt_name"),
      col("utc_txn_date"),
      col("utc_txn_time"),
      col("cust1_org_legal_name"),
      col("cust2_org_legal_name"),
      col("user_id"),
      col("currency_conversion_rate"),
      col("user_device_type"),
      col("user_session_date_time"),
      col("transaction_memo_line_1"),
      col("client_ip_addr"),
      col("debtor_id1").as("debtor_id"),
      col("addl_field_10"),
      col("addl_field_7"),
      col("addl_field_8"),
      col("addl_field_9"),
      col("cust1_bank_name"),
      col("cust2_bank_name"),
      col("source_system_cd"),
      col("channel_cd"),
      col("cr_dr_code"),
      col("cash_code"),
      col("orig_curr_cash_amount"),
      col("cash_cad_equivalent_amt"),
      col("acct_to_cad_rate"),
      col("txn_to_acc_rate"),
      col("txn_to_cad_rate"),
      col("txn_curr_rate_ind"),
      col("orig_process_date"),
      col("loaded_to_cerebro"),
      col("loaded_to_hunter"),
      col("txn_tran_exchange_rate"),
      col("row_update_date"),
      col("processing_date")
    )

    ds
  }

  def joinForAccountKey(
                         esdlTransactions: Dataset[EsdlTransaction],
                         esdlPartyProd: Dataset[EsdlPartyProd],
                         esdlAccOpenDateDs: Dataset[EsdlAccOpenDate]
                       ): DataFrame = {

    // Step 1: account_number is not null
    val esdlTransactionsDs = esdlTransactions
      .withColumn("account_number_esdl", col("account_number"))
      .withColumn("holding_branch_key_esdl", col("holding_branch_key"))

    val esdlPartyProdDs = esdlPartyProd.withColumn("product_type_code_prod", col("product_type_code"))

    val step1 = esdlTransactionsDs
      .filter(col("account_number_esdl").isNotNull)
      .join(esdlPartyProdDs, esdlTransactionsDs("account_number_esdl") === esdlPartyProdDs("account_number"), "left")
      .filter(
        esdlTransactionsDs("holding_branch_key_esdl").isNotNull &&
          esdlTransactionsDs("holding_branch_key_esdl") === esdlPartyProdDs("holding_branch_key") &&
          esdlPartyProdDs("product_type_code_prod").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          esdlPartyProdDs("relation_type_cd") === "1"
      )
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code_prod"),
        col("relation_type_cd"),
        col("party_key"),
        col("account_key"),
        lit("null").as("null_col1"),
        lit("null").as("null_col2"),
        lit("null").as("null_col3"),
        lit("null").as("null_col4"),
        lit("null").as("null_col5"),
        lit("null").as("null_col6")
      )

    val accountKey1 = step1
      .withColumn(
        "account_key",
        when(col("party_key").isNotNull, col("account_key"))
          .otherwise(
            when(
              col("holding_branch_key_esdl").isNotNull,
              concat_ws("-", lit("Orph"), col("holding_branch_key_esdl"), col("account_number_esdl")))
              .otherwise(concat_ws("-", lit("Orph"), col("account_number_esdl")))
          ))
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code_prod"),
        col("relation_type_cd"),
        col("account_key"),
        col("party_key"),
        lit("null").as("null_col1"),
        lit("null").as("null_col2"),
        lit("null").as("null_col3"),
        lit("null").as("null_col4"),
        lit("null").as("null_col5"),
        lit("null").as("null_col6")
      )

    // Additional lookup when party_key is null
    val ecifCompositeKey = esdlAccOpenDateDs
      .join(esdlTransactionsDs, esdlAccOpenDateDs("product_type_code") === "CL")
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code"),
        lit("null").as("null_col1"),
        col("account_key"),
        lit("null").as("null_col2"),
        col("curr_plc_acct_num"),
        col("holding_branch_key_source"),
        lit("").as("empty_col"),
        lit("null").as("null_col3"),
        lit("null").as("null_col4"),
        lit("null").as("null_col5")
      )

    val additionalLookup = ecifCompositeKey
      .join(
        esdlPartyProdDs,
        ecifCompositeKey("curr_plc_acct_num") === esdlPartyProdDs("ecif_composite_key") &&
          esdlPartyProdDs("product_type_code_prod") === "CL" &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "inner"
      )
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code_prod"),
        col("relation_type_cd"),
        col("account_key"),
        col("party_key"),
        col("curr_plc_acct_num"),
        col("holding_branch_key_source"),
        col("ecif_composite_key"),
        lit("null").as("null_col1"),
        lit("null").as("null_col2"),
        lit("null").as("null_col3")
      )

    // Step 2: account_number is null
    val step2 = esdlTransactionsDs
      .filter(col("account_number_esdl").isNull)
      .join(esdlPartyProdDs, esdlTransactionsDs("opp_account_number") === esdlPartyProdDs("account_number"), "left")
      .filter(
        col("opp_branch_key").isNotNull &&
          esdlTransactionsDs("opp_branch_key") === esdlPartyProdDs("holding_branch_key") &&
          esdlPartyProdDs("product_type_code_prod").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC") &&
          esdlPartyProdDs("relation_type_cd") === "1"
      )
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code_prod"),
        col("relation_type_cd"),
        col("account_key"),
        col("party_key"),
        lit("null").as("null_col1"),
        lit("null").as("null_col2"),
        col("ecif_composite_key"),
        col("opp_branch_key"),
        col("opp_account_number"),
        lit("null").as("null_col3"),
        lit("null").as("null_col4")
      )

    val accountKey2 = step2
      .withColumn(
        "account_key",
        when(col("party_key").isNotNull, col("account_key"))
          .otherwise(
            when(
              col("opp_branch_key").isNotNull,
              concat_ws("-", lit("Orph"), col("opp_branch_key"), col("opp_account_number")))
              .otherwise(concat_ws("-", lit("Orph"), col("opp_account_number"), lit("null"), lit("null")))
          ))
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code_prod"),
        col("relation_type_cd"),
        col("account_key"),
        col("party_key"),
        lit("null").as("null_col1"),
        lit("null").as("null_col2"),
        col("ecif_composite_key"),
        col("opp_branch_key"),
        col("opp_account_number"),
        lit("null").as("null_col3")
      )

    // Step 3: card_number is not null
    val step3 = esdlTransactionsDs
      .filter(col("card_number").isNotNull)
      .join(esdlPartyProdDs, esdlTransactionsDs("card_number") === esdlPartyProdDs("account_number")
        &&
        (esdlTransactionsDs("holding_branch_key").isNull || esdlTransactionsDs("holding_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
        esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC") &&
        esdlPartyProdDs("relation_type_cd") === "1", "left")
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code_prod"),
        col("relation_type_cd"),
        col("account_key"),
        col("party_key"),
        lit("null").as("null_col1"),
        lit("null").as("null_col2"),
        col("ecif_composite_key"),
        col("opp_branch_key"),
        col("opp_account_number"),
        col("card_number")
      )

    val accountKey3 = step3
      .withColumn(
        "account_key1",
        when(col("party_key").isNotNull, col("account_key"))
          .otherwise(concat_ws("-", lit("Orph"), col("card_number")))
      )
      .select(
        col("account_number_esdl"),
        col("holding_branch_key_esdl"),
        col("product_type_code_prod"),
        col("relation_type_cd"),
        col("account_key"),
        col("party_key"),
        lit("null").as("null_col1"),
        lit("null").as("null_col2"),
        col("ecif_composite_key"),
        col("opp_branch_key"),
        col("opp_account_number"),
        col("card_number")
      )


    val finalAccountKey = accountKey1.union(accountKey2).union(accountKey3).union(additionalLookup)

    val accountKeyForMerge = finalAccountKey.select(
      col("account_number_esdl").as("account_number"),
      col("account_key")
    )

    accountKeyForMerge

  }

  def deriveEcifCompositeKey(
                              esdlTransactionsDs: Dataset[_],
                              esdlPartyProdDs: Dataset[_],
                              esdlAccOpenDateDs: Dataset[_]
                            ): DataFrame = {

    val step1 = esdlTransactionsDs
      .filter(col("account_number").isNotNull)
      .join(
        esdlPartyProdDs,
        esdlTransactionsDs("account_number") === esdlPartyProdDs("account_number") &&
          (esdlTransactionsDs("holding_branch_key").isNull || esdlTransactionsDs("holding_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
          esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "left"
      )
      .select(
        esdlTransactionsDs("account_number"),
        esdlTransactionsDs("holding_branch_key"),
        esdlPartyProdDs("party_key"),
        esdlPartyProdDs("ecif_composite_key").as("ecif_composite_key_step1"),
        lit(null).as("opp_account_number"), // Add missing columns for alignment
        lit(null).as("opp_branch_key"),
        lit(null).as("card_number")
      )

    // Additional lookup when party_key is null in step1
    val step1AdditionalLookup = step1
      .filter(col("party_key").isNull)
      .join(
        esdlAccOpenDateDs,
        step1("account_number") === esdlAccOpenDateDs("curr_plc_acct_num") &&
          step1("holding_branch_key").cast("int") === esdlAccOpenDateDs("holding_branch_key_source") &&
          esdlAccOpenDateDs("product_type_code") === "CL",
        "left"
      )
      .select(
        step1("account_number"),
        step1("holding_branch_key"),
        lit(null).as("party_key"),
        esdlAccOpenDateDs("ecif_composite_key").as("ecif_composite_key_step1_additional"),
        lit(null).as("opp_account_number"),
        lit(null).as("opp_branch_key"),
        lit(null).as("card_number")
      )

    // Step 2: account_number is null
    val step2 = esdlTransactionsDs
      .filter(col("account_number").isNull)
      .join(
        esdlPartyProdDs,
        esdlTransactionsDs("opp_account_number") === esdlPartyProdDs("account_number") &&
          (esdlTransactionsDs("opp_branch_key").isNull || esdlTransactionsDs("opp_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
          esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "left"
      )
      .select(
        lit(null).as("account_number"),
        lit(null).as("holding_branch_key"),
        esdlPartyProdDs("party_key"),
        esdlPartyProdDs("ecif_composite_key").as("ecif_composite_key_step1_additional"),
        esdlTransactionsDs("opp_account_number"),
        esdlTransactionsDs("opp_branch_key"),
        lit(null).as("card_number")
      )

    val step2AdditionalLookup = step2
      .filter(col("party_key").isNull)
      .join(
        esdlAccOpenDateDs,
        step2("opp_account_number") === esdlAccOpenDateDs("curr_plc_acct_num") &&
          step2("opp_branch_key").cast("int") === esdlAccOpenDateDs("holding_branch_key_source") &&
          esdlAccOpenDateDs("product_type_code") === "CL",
        "left"
      )
      .select(
        lit(null).as("account_number"),
        lit(null).as("holding_branch_key"),
        lit(null).as("party_key"),
        esdlAccOpenDateDs("ecif_composite_key").as("ecif_composite_key_step2_additional"),
        step2("opp_account_number"),
        step2("opp_branch_key"),
        lit(null).as("card_number")
      )

    val step3 = esdlTransactionsDs
      .filter(col("card_number").isNotNull)
      .join(
        esdlPartyProdDs,
        esdlTransactionsDs("card_number") === esdlPartyProdDs("account_number") &&
          (esdlTransactionsDs("holding_branch_key").isNull || esdlTransactionsDs("holding_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
          esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC") &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "left"
      )
      .select(
        lit(null).as("account_number"), // Add missing columns for alignment
        lit(null).as("holding_branch_key"),
        esdlPartyProdDs("party_key"),
        esdlPartyProdDs("ecif_composite_key").as("ecif_composite_key_step3"),
        lit(null).as("opp_account_number"),
        lit(null).as("opp_branch_key"),
        esdlTransactionsDs("card_number")
      )

    // Combine all steps
    val finalEcifCompositeKey = step1
      .union(step1AdditionalLookup)
      .union(step2)
      .union(step2AdditionalLookup)
      .union(step3)

    val ecifCompositeKeyResult = finalEcifCompositeKey
      .select(
        col("account_number"),
        col("ecif_composite_key_step1").as("ecif_composite_key")
      )

    ecifCompositeKeyResult
  }

  def deriveOrphInd(
                     esdlTransactionsDs: Dataset[EsdlTransaction],
                     esdlPartyProdDs: Dataset[EsdlPartyProd],
                     esdlAccOpenDateDs: Dataset[EsdlAccOpenDate]
                   ): DataFrame = {

    val step1 = esdlTransactionsDs
      .filter(col("account_number").isNotNull)
      .join(
        esdlPartyProdDs,
        esdlTransactionsDs("account_number") === esdlPartyProdDs("account_number") &&
          (esdlTransactionsDs("holding_branch_key").isNotNull || esdlTransactionsDs("holding_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
          esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "left"
      )
      .select(
        esdlTransactionsDs("account_number"),
        esdlTransactionsDs("holding_branch_key"),
        esdlTransactionsDs("opp_account_number"),
        esdlTransactionsDs("opp_branch_key"),
        esdlTransactionsDs("card_number"),
        esdlTransactionsDs("orig_process_date"),
        esdlPartyProdDs("party_key"),
        esdlPartyProdDs("ecif_composite_key").as("ecif_composite_key_step1"),
        lit("N").as("orph_ind") // Default to 'N' if party_key is found
      )

    // Additional lookup when party_key is null in step1
    val step1AdditionalLookup = step1
      .filter(col("party_key").isNull)
      .join(
        esdlAccOpenDateDs,
        step1("account_number") === esdlAccOpenDateDs("curr_plc_acct_num") &&
          step1("holding_branch_key").cast("int") === esdlAccOpenDateDs("holding_branch_key_source") &&
          esdlAccOpenDateDs("product_type_code") === "CL",
        "left"
      )
      .select(
        step1("account_number"),
        step1("holding_branch_key"),
        step1("opp_account_number"),
        step1("opp_branch_key"),
        step1("card_number"),
        step1("orig_process_date"),
        esdlAccOpenDateDs("ecif_composite_key").as("ecif_composite_key_step1_additional"),
        lit("P").as("orph_ind"),
        lit("null")
      )

    val step2 = esdlTransactionsDs
      .filter(col("account_number").isNull)
      .join(
        esdlPartyProdDs,
        esdlTransactionsDs("opp_account_number") === esdlPartyProdDs("account_number") &&
          (esdlTransactionsDs("opp_branch_key").isNull || esdlTransactionsDs("opp_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
          esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "left"
      )
      .select(
        esdlTransactionsDs("account_number"),
        esdlTransactionsDs("holding_branch_key"),
        esdlTransactionsDs("opp_account_number"),
        esdlTransactionsDs("opp_branch_key"),
        esdlTransactionsDs("card_number"),
        esdlTransactionsDs("orig_process_date"),
        esdlPartyProdDs("party_key"),
        esdlPartyProdDs("ecif_composite_key").as("ecif_composite_key_step2"),
        when(col("party_key").isNotNull, lit("N")).otherwise(lit("Y")).as("orph_ind") // Set to 'N' or 'Y'
      )

    // Step 3: Handle orphan records where orph_ind = 'Y'
    val step3 = esdlTransactionsDs
      .filter(col("orph_ind") === "Y")
      .join(
        esdlPartyProdDs,
        esdlTransactionsDs("account_number") === esdlPartyProdDs("account_number") &&
          (esdlTransactionsDs("holding_branch_key").isNull || esdlTransactionsDs("holding_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
          esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "left"
      )
      .withColumn(
        "orph_ind",
        when(
          col("party_key").isNotNull, lit("N") // Update to 'N' if party_key is found
        ).otherwise(
          when(
            datediff(esdlTransactionsDs("processing_date"), col("orig_process_date")) + 1 <= 10,
            col("orph_ind") // Keep 'Y' if age <= 10
          ).otherwise(
            lit("O") // Update to 'O' if age > 10
          )
        )
      ).select(
        esdlTransactionsDs("account_number"),
        esdlTransactionsDs("holding_branch_key"),
        esdlTransactionsDs("opp_account_number"),
        esdlTransactionsDs("opp_branch_key"),
        esdlTransactionsDs("card_number"),
        esdlTransactionsDs("orig_process_date"),
        esdlPartyProdDs("party_key"),
        esdlPartyProdDs("ecif_composite_key"),
        when(col("party_key").isNotNull, lit("N")).otherwise(lit("Y")).as("orph_ind") // Set to 'N' or 'Y'
      )

    // Step 4: Handle orphan records where orph_ind = 'Y' for opp_account_number
    val step4 = esdlTransactionsDs
      .filter(col("orph_ind") === "Y")
      .join(
        esdlPartyProdDs,
        esdlTransactionsDs("opp_account_number") === esdlPartyProdDs("account_number") &&
          (esdlTransactionsDs("opp_branch_key").isNull || esdlTransactionsDs("opp_branch_key") === esdlPartyProdDs("holding_branch_key")) &&
          esdlPartyProdDs("product_type_code").isin("CARD", "CC", "PCFC", "SVSA", "VISA", "PP", "PPC", "PDEP", "DEP", "PLOA", "CL") &&
          esdlPartyProdDs("relation_type_cd") === "1",
        "left"
      )
      .withColumn(
        "orph_ind",
        when(
          col("party_key").isNotNull, lit("N") // Update to 'N' if party_key is found
        ).otherwise(
          when(
            datediff(esdlTransactionsDs("processing_Date"), col("orig_process_date")) + 1 <= 10,
            col("orph_ind") // Keep 'Y' if age <= 10
          ).otherwise(
            lit("O") // Update to 'O' if age > 10
          )
        )
      ).select(
        esdlTransactionsDs("account_number"),
        esdlTransactionsDs("holding_branch_key"),
        esdlTransactionsDs("opp_account_number"),
        esdlTransactionsDs("opp_branch_key"),
        esdlTransactionsDs("card_number"),
        esdlTransactionsDs("orig_process_date"),
        esdlPartyProdDs("party_key"),
        esdlPartyProdDs("ecif_composite_key").as("ecif_composite_key_step2"),
        when(col("party_key").isNotNull, lit("N")).otherwise(lit("Y")).as("orph_ind") // Set to 'N' or 'Y'
      )

    // Combine all steps
    val finalOrphInd = step1
      .union(step1AdditionalLookup)
      .union(step2)
      .union(step3)
      .union(step4)

    // Select the final columns
    val finalOrphIndResult = finalOrphInd.select(
      col("account_number"),
      col("holding_branch_key"),
      col("opp_account_number"),
      col("opp_branch_key"),
      col("card_number"),
      col("orig_process_date"),
      col("orph_ind")
    ).withColumn("orph_ind", coalesce(col("orph_ind"), lit("UNKNOWN")))
      .select(col("account_number"),
        col("orph_ind"))

    finalOrphIndResult
  }

}
