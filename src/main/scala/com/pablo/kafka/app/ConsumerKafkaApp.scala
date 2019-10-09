package main.com.pablo.kafka.scala.app

import main.com.pablo.kafka.scala.ConsumerKafkaAvroDeserializer

object ConsumerKafkaApp extends App{

  //Confluent Local - Docker
  val kafkaConfluentDocker="localhost:29092"
  val schemaRegistryUrl="http://localhost:8081"
  val topic="topic"



  val consumer = new ConsumerKafkaAvroDeserializer(topic,kafkaConfluentDocker,schemaRegistryUrl)
  //consumer.startCalendar()
  consumer.startStock()



}
