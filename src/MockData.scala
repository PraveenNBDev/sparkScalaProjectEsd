import Models.{SrcFiName, TgtFullName}
import org.apache.spark.sql.functions.{current_date, date_format, to_date}

import java.sql.{Date, Timestamp}

object MockData {

  // Mock data for EsdlTransaction
  val mockEsdlTransaction = Models.EsdlTransaction(
    txn_id = "123456",
    card_number = "987654321",
    account_number = "123456789",
    holding_branch_key = "001",
    account_key = "001-123456789",
    source_system_cd = "CERTAPAY",
    channel_cd = "04",
    source_transaction_id = "654321",
    execution_local_date_time = new Timestamp(System.currentTimeMillis()),
    posting_Date = new Timestamp(System.currentTimeMillis()),
    cr_dr_code = "C",
    cash_code = "N",
    msg_type_code = "MT103",
    acct_curr_cd = "CAD",
    acct_curr_Amount = "1000.00",
    orig_curr_cd = "USD",
    orig_curr_amount = "750.00",
    cad_equivalent_amt = "1000.00",
    orig_curr_cash_amount = "0.00",
    case_Cad_equivalent_amt = "1000.00",
    acct_to_cad_rate = "1.33",
    txn_to_acct_rate = "1.33",
    txn_to_cad_rate = "1.33",
    txn_curr_rate_ind = "TXN",
    ecif_compisite_key = "ECIF123",
    ip_address = "192.168.1.1",
    orph_ind = "N",
    orig_process_date = "2023-10-01",
    loaded_to_cerebro = "N",
    loaded_to_hunter = "Y",
    opp_account_number = "987654321",
    opp_branch_key = "002",
    rules_cash_cad_equivalent_amt = "0.00",
    operation_type = "TRANSFER",
    txn_status_code = "SUCCESS",
    txn_response_cd = "200",
    rules_cad_equivalent_amt = "1000.00",
    emit_transfer_cd = "INTERAC123",
    emit_recipient_name = "John Doe",
    recipient_sms = "1234567890",
    sender_email = "sender@example.com",
    processing_Date = Date.valueOf("2025-01-01"),
    recipient_email = "recipient@example.com",
    instr_agent_id = "AGENT001",
    instrg_agent_clearing_system = "CLEAR001",
    txn_status = "COMPLETED",
    txn_type = "TRANSFER",
    sndr_agt_name = "Sender Agent",
   // utc_txn_date = "2023-10-01",
   // utc_txn_time = "12:00:00",
    cust1_org_legal_name = "Sender Corp",
    cust2_org_legal_name = "Recipient Corp",
    cust1_bank_name = "Bank of Sender",
    cust2_bank_name = "Bank of Receiver",
    currency_conversion_rate = "1.33",
    user_id = "USER001",
    user_device_type = "MOBILE",
    user_session_date_time = "2023-10-01 12:00:00",
    transaction_memo_line_1 = "Payment for services",
    client_ip_addr = "192.168.1.2",
    debtor_id = "DEBTOR001",
    addl_field_10 = "Additional Field 10",
    fx_rate_exchange_rate = "1.33",
    addl_field_7 = "Additional Field 7",
    addl_field_8 = "Additional Field 8",
    addl_field_9 = "Additional Field 9",
   // row_update_date = new Timestamp(System.currentTimeMillis())
  )

  // Mock data for StgCertPayAmlReport
  val mockStgCertPayAmlReport = Models.StgCertPayAmlReport(
    cr_dr_ind = "C",
    account_servicer_reference = "654321",
    src_txn_id = "123456",
    debtor_Account = "010-00162-0020036",
    party_key = "PARTY001",
    account_number = "123456789",
    holding_branch_key = "001",
    product_type_code = "CL",
    relation_type_cd = "1",
    account_key = "001-123456789",
    acceptance_date_time = new Timestamp(System.currentTimeMillis()),
    msg_type_Code = "MT103",
    currency = "CAD",
    amount = 1000.00,
    creditor_Account = "010-00162-0020036",
    payment_status = "SUCCESS",
    interac_ref_num = "INTERAC123",
    creditor_name = "John Doe",
    creditor_mobile_no = "1234567890",
    debtor_email = "sender@example.com",
    creditor_email_phone = "recipient@example.com",
    instructed_agent_fi = "AGENT001",
    instructing_agent_fi = "CLEAR001",
    transaction_type = "TRANSFER",
    transaction_type_desc = "Transfer of funds",
    debtor_name = "Sender Corp",
    debtor_legal_name = "Sender Corp",
    creditor_legal_name = "Recipient Corp",
    creditor_memo = "Payment for services",
    creditor_ip_address = "192.168.1.2",
    creditor_id = "CREDITOR001",
    debtor_id = "DEBTOR001",
    fraud_check_Action = "PASS",
    transaction_id = "654321"
  )

  // Mock data for EsdlPartyProd
  val mockEsdlPartyProd = Models.EsdlPartyProd(
    party_key = "PARTY001",
    account_number = "123456789",
    holding_branch_key = "001",
    product_type_code = "CL",
    relation_type_Cd = "1",
    amount_key = "001-123456789",
    ecif_composite_key = "ECIF123"
  )

  // Mock data for EsdlAccOpenDate
  val mockEsdlAccOpenDate = Models.EsdlAccOpenDate(
    ecif_composite_key = "ECIF123",
    curr_plc_acc_num = "123456789",
    holding_branch_key_source = "001",
    product_type_code = "CL"
  )

  // Mock data for EsdlRef
  val mockEsdlRef = Models.EsdlRef(
    typ = "financial_institution_number",
    source_id = "CERTAPAY",
    source_cd = "CA000010",
    target_cd = "CA000010",
    effective_from = "2023-01-01",
    effectibve_to = "2023-12-31",
    source_json = SrcFiName(fl_num = "001"),
    target_json = TgtFullName(full_name = "Bank of Bank"),
    effective_to = "2023-12-31"
  )

  // Mock data for PrmConfig
  val mockPrmConfig = Models.PrmConfig(
    esdlStgDb = "esdl_stg_db",
    esdlStgTbl = "esdl_stg_table",
    certPayDb = "cert_pay_db",
    certPatTable = "cert_pay_table",
    esdlRefDb = "esdl_ref_db",
    esdlRefTbl = "esdl_ref_table",
    amlReportDb = "aml_report_db",
    amlReportTbl = "aml_report_table",
    openDateDb = "open_date_db",
    openDatetbl = "open_date_table"
  )
}