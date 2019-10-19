package main.com.pablo.kafka.scala.app

import main.com.pablo.kafka.scala.ConsumerKafkaAvroDeserializer

class ConsumerKafkaApp extends App{

  //Confluent Local - Docker
  val kafkaConfluentDocker="localhost:29092"
  val schemaRegistryUrl="http://localhost:8081"
  val calendarTopic="topic"
  val stockTopic="supply_bdsupply-stock-consolidado-sherpa_stockcons-sap-dl"


  val consumer = new ConsumerKafkaAvroDeserializer(stockTopic,kafkaConfluentDocker,schemaRegistryUrl)
  consumer.readStockTopic()
  //consumer.readCalendarTopic()


}
