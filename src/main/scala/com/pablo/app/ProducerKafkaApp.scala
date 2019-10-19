package main.com.pablo.kafka.scala.app

import main.com.pablo.kafka.scala.ProducerKafkaAvroSerializer


class ProducerKafkaApp extends App {

  val configPath = getClass.getClassLoader.getResource(".").getPath

  //Local - Docker
  val kafkaConfluent="localhost:29092"
  val schemaRegistryUrl="http://localhost:8081"
  val stockTopic="supply_bdsupply-stock-consolidado-sherpa_stockcons-sap-dl"

  val producer = new ProducerKafkaAvroSerializer(stockTopic,kafkaConfluent,schemaRegistryUrl)
  producer.sendStockRecordListFromFile(configPath+"store_stock_matrix.txt")
  //producer.sendStockRecordList()
  //producer.sendCalendarRecordList(

  System.out.println("Producer kafka OK")

}

