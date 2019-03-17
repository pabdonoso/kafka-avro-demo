package main.com.pablo.kafka.app

import com.carrefour.bigdata.ingestion.avro.{CalendarRecord, ProducerKafkaAvroSerializer}

object ProducerKafkaApp extends App {

  //Confluent Local - Docker
  val kafkaConfluent="localhost:29092"
  val topic="test_schema_avro"
  val producer = new ProducerKafkaAvroSerializer(topic,kafkaConfluent)

  producer.send(createCalendarRecord)



  def createCalendarRecord(): List[CalendarRecord] ={
    val listCalendarRecords: List[CalendarRecord] = List(
      CalendarRecord("2021-12-31 00:00:00","000H",2021,"O",null,true),
      CalendarRecord("2021-12-30 00:00:00","0009",2021,"O",null,true),
      CalendarRecord("2021-12-29 00:00:00","0011",2021,"O",null,true),
      CalendarRecord("2021-01-01 00:00:00","0007",2021,"C","Festivo",true),
      CalendarRecord("2021-09-27 00:00:00","000Y",2021,"C","Festivo",true))

    listCalendarRecords
  }

}

