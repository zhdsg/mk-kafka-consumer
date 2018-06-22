spark2-submit --class com.zhimo.datahub.etl.stage2.MKStage2ServerRefund --master spark://10.10.100.2:7077 mk-kafka-consumer-assembly-1.0.jar
spark2-submit --class com.zhimo.datahub.etl.stage2.MKStage2ServerRevenue --master spark://10.10.100.2:7077 mk-kafka-consumer-assembly-1.0.jar
spark2-submit --class com.zhimo.datahub.etl.stage2.MKStage2ServerSignup --master spark://10.10.100.2:7077 mk-kafka-consumer-assembly-1.0.jar
spark2-submit --class com.zhimo.datahub.etl.stage2.MKStage2ServerStudent --master spark://10.10.100.2:7077 mk-kafka-consumer-assembly-1.0.jar
spark2-submit --class com.zhimo.datahub.etl.stage2.MKStage2Client --master spark://10.10.100.2:7077 mk-kafka-consumer-assembly-1.0.jar