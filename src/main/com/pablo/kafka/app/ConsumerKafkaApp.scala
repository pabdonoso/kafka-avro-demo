package main.com.pablo.kafka.app

import com.carrefour.bigdata.ingestion.avro.ConsumerKafkaAvroDeserializer

object ConsumerKafkaApp extends App{

  //Confluent Local - Docker
  val kafkaConfluent="localhost:29092"
  val schemaRegistryUrl="http://localhost:8081"
  val topic="test_schema_avro"

  val consumer = new ConsumerKafkaAvroDeserializer(topic,kafkaConfluent,schemaRegistryUrl)
  consumer.start()

}
