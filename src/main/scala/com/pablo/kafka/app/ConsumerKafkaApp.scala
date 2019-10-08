package main.com.pablo.kafka.scala.app

import main.com.pablo.kafka.scala.ConsumerKafkaAvroDeserializer

object ConsumerKafkaApp extends App{

  //Confluent Local - Docker
  /*val kafkaConfluentDocker="localhost:29092"
  val schemaRegistryUrl="http://localhost:8081"
  val topicStock="supply_bdsupply-stock-consolidado-sherpa_stockcons-sap-dl"*/

  //DESA
  val kafkaConfluent="ld6cf01.es.wcorp.carrefour.com:9092,ld6cf02.es.wcorp.carrefour.com:9092,ld6cf03.es.wcorp.carrefour.com:9092"
  val schemaRegistryUrl="http://ld6cf03.es.wcorp.carrefour.com:8081"
  val topicStock="supply_bdsupply-stock-consolidado-sherpa_stockcons-sap-dl"


  val consumer = new ConsumerKafkaAvroDeserializer(topicStock,kafkaConfluent,schemaRegistryUrl)
  //consumer.startCalendar()
  consumer.startStock()



}
