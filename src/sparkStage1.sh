rm nohup.out
touch nohup.out
nohup spark2-submit --master spark://10.10.100.2:7077 --class com.zhimo.datahub.etl.stage1.MKStage1All mk-kafka-consumer-assembly-1.0.jar &
tail -f nohup.out &