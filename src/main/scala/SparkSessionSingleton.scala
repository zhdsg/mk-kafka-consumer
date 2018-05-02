import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by yaning on 5/2/18.
  */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }

}
