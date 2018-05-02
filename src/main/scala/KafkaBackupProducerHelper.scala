import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.Row

import scala.util.parsing.json.JSONObject

object KafkaBackupProducerHelper {

  @transient  private var kafkaBackupProducer: KafkaProducer[String,String] = _


  def produce(topic:String,messages:Array[Row]): Unit ={
    if(kafkaBackupProducer==null){
      val config = ConfigFactory.load()
      val localDevEnv = config.getBoolean("environment.localDev")
      val kafkaBackupProducerParams = new Properties()
      kafkaBackupProducerParams.put("bootstrap.servers", if (localDevEnv) "localhost:9092" else "10.10.100.11:9092")
      kafkaBackupProducerParams.put("client.id", "MKKafkaConsumer")
      kafkaBackupProducerParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaBackupProducerParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      kafkaBackupProducer = new KafkaProducer[String, String](kafkaBackupProducerParams)
    }
    for(row<-messages) {
      val message = JSONObject(row.getValuesMap(row.schema.fieldNames)).toString()
      kafkaBackupProducer.send(new ProducerRecord[String, String](topic, message))
    }
  }

}
