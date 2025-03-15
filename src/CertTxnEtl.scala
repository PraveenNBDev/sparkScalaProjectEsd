import Models.{EsdlAccOpenDate, EsdlPartyProd, EsdlRef, EsdlTransaction, StgCertPayAmlReport}
import org.apache.spark.sql.SparkSession



object CertTxnEtl extends App{


  val spark = SparkSession.builder()
    .appName("Mock Data Example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val transaction = MockData.mockEsdlTransaction
  val amlReport = MockData.mockStgCertPayAmlReport
  val partyProd = MockData.mockEsdlPartyProd
  val accOpenDate = MockData.mockEsdlAccOpenDate
  val ref = MockData.mockEsdlRef
  val ref1 = MockData.mockEsdlRef1
  val config = MockData.mockPrmConfig

  val esdlTransactionDs = Seq(transaction).toDS()
  val StgCertPayAmlReport = Seq(amlReport).toDS()
  val EsdlPartyProdDs = Seq(partyProd).toDS()
  val EsdlAccOpenDateDs = Seq(accOpenDate).toDS()
  val EsdlRefDs = Seq(ref,ref1).toDS()


  val esdlTxnWithAml = StgCertPayAmlReport.transform(Tranformer.joinEsdlWithCertapay(esdlTransactionDs, EsdlRefDs))
  val accountKeyForMerge = Tranformer.joinForAccountKey(esdlTransactionDs, EsdlPartyProdDs, EsdlAccOpenDateDs)
  val ecifCompKeyForMerge = Tranformer.deriveEcifCompositeKey(esdlTransactionDs, EsdlPartyProdDs, EsdlAccOpenDateDs)
  esdlTxnWithAml.union(accountKeyForMerge).union(ecifCompKeyForMerge).show()
}
