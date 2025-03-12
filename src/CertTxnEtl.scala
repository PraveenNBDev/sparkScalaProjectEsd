import Models.{EsdlAccOpenDate, EsdlPartyProd, EsdlRef, EsdlTransaction, StgCertPayAmlReport}
import Tranformer.joinEsdlTransactions
import org.apache.spark.sql.SparkSession


object CertTxnEtl extends App{

  val spark = SparkSession.builder()
    .appName("Mock Data Example")
    .master("local[*]") // Use all available cores
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  spark.sparkContext.setLogLevel("INFO")

  import spark.implicits._

  val transaction = MockData.mockEsdlTransaction
  val amlReport = MockData.mockStgCertPayAmlReport
  val partyProd = MockData.mockEsdlPartyProd
  val accOpenDate = MockData.mockEsdlAccOpenDate
  val ref = MockData.mockEsdlRef
  val config = MockData.mockPrmConfig

  val esdlTransactionDs = Seq(transaction).toDS()
  val StgCertPayAmlReport = Seq(amlReport).toDS()
  val EsdlPartyProdDs = Seq(partyProd).toDS()
  val EsdlAccOpenDateDs = Seq(accOpenDate).toDS()
  val EsdlRefDs = Seq(ref).toDS()


  val finalDs = StgCertPayAmlReport.transform(joinEsdlTransactions(esdlTransactionDs, EsdlPartyProdDs, EsdlRefDs, EsdlAccOpenDateDs))
    .as[EsdlTransaction]




}
