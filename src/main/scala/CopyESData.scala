import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.elasticsearch.spark.sql.sparkDataFrameFunctions

object ProcessMetas {
  def addID(metaMap: Map[String, String]): String = {
    metaMap("_id")
  }
}

class CopyESData {
  //Application Configuration
  val applicationConfig: Config = ConfigFactory.load("application.conf")

  //Copy From Elastic Details
  val copyFromElasticURL: String = applicationConfig.getString("copy_from.elastic_url")
  val copyFromElasticPort: String = applicationConfig.getString("copy_from.port_number")
  val copyFromElasticIndex: String = applicationConfig.getString("copy_from.index_name")

  //Copy To Elastic Details
  val copyToElasticURL: String = applicationConfig.getString("copy_to.elastic_url")
  val copyToElasticPort: String = applicationConfig.getString("copy_to.port_number")
  val copyToElasticIndex: String = applicationConfig.getString("copy_to.index_name")
  val copyToElasticIndexType: String = applicationConfig.getString("copy_to.type_name")

  //Apache Spark Details
  val sparkAppName: String = applicationConfig.getString("spark_details.app_name")
  val sparkAppURL: String = applicationConfig.getString("spark_details.master_url")
  val sparkConfig: SparkConf = new SparkConf().setMaster(sparkAppURL).setAppName(sparkAppName)
  val sparkSession: SparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()

  val copyFromReader: DataFrameReader = sparkSession.read
    .format("org.elasticsearch.spark.sql")
    .option("es.nodes.wan.only", "true")
    .option("es.index.auto.create", "true")
    .option("es.read.metadata", "true")
    .option("es.port", copyFromElasticPort)
    .option("es.nodes", copyFromElasticURL)

  def performOperation(): Unit = {
    val copyFromDF = copyFromReader.load(copyFromElasticIndex)
    import org.apache.spark.sql.functions.udf
    val withOldIDs = udf { mapToProcess: Map[String, String] => ProcessMetas.addID(mapToProcess) }
    val preparedDF = copyFromDF.withColumn("id", withOldIDs(copyFromDF("_metadata"))).drop("_metadata")

    import org.elasticsearch.spark.sql._
    preparedDF.saveToEs(copyToElasticIndex + "/" + copyToElasticIndexType,
      Map(
        "es.mapping.id" -> "id",
        "es.port" -> copyToElasticPort,
        "es.nodes" -> copyToElasticURL
      ))
  }
}

object RunObject {
  def main(args: Array[String]): Unit = {
    new CopyESData().performOperation()
  }
}