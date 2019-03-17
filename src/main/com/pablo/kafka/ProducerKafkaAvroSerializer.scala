package com.carrefour.bigdata.ingestion.avro

import java.util.{Properties, UUID}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}


class ProducerKafkaAvroSerializer(val topic:String, val kafka:String) {

  private val props = new Properties()

  props.put("bootstrap.servers", kafka)
  props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
  props.put("value.serializer",classOf[KafkaAvroSerializer].getCanonicalName)
  props.put("client.id", UUID.randomUUID().toString())

  //Confluent Local - Docker
  val schemaRegistryUrl="http://localhost:8081"
  props.put("schema.registry.url",schemaRegistryUrl)
  props.put("auto.register.schemas", (false: java.lang.Boolean))

  private val producer =   new KafkaProducer[String,GenericRecord](props)



  //Read avro schema
  val schemaReg = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
  val schemaMeta = schemaReg.getLatestSchemaMetadata(topic + "-value")
  val schema= schemaMeta.getSchema
  val schemaParse =new Schema.Parser().parse(schema)


  def send(calendarList:List[CalendarRecord] ): Unit = {
    try {

      for( cal <- calendarList ){
        val genericRecord: GenericRecord =parseCalendar(schemaParse,cal)
        producer.send(new ProducerRecord[String, GenericRecord](topic, null, genericRecord))

      }

    } catch {
      case ex: Exception =>
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
    }
    producer.close()
  }




  def parseCalendar(schemaParse:Schema,calendar:CalendarRecord ): GenericRecord ={

    val genericCalendar: GenericRecord = new GenericData.Record(schemaParse)
    genericCalendar.put("DATE_DAY", calendar.DATE_DAY)
    genericCalendar.put("WERKS", calendar.WERKS)
    genericCalendar.put("YEAR", calendar.YEAR)
    genericCalendar.put("STATE", calendar.STATE)
    genericCalendar.put("DESCRIPTION", calendar.DESCRIPTION)
    genericCalendar.put("ACTIVE", calendar.ACTIVE)
    genericCalendar
  }


}
