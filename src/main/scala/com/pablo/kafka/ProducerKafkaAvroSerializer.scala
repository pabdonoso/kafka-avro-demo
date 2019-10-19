package main.com.pablo.kafka.scala

import java.util.{Properties, UUID}

import com.pablo.records.{CalendarRecord, OperationsRecord}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


class ProducerKafkaAvroSerializer(val topic:String, val kafka:String, val schemaRegistryUrl:String){

  private val props = new Properties()
  props.put("bootstrap.servers", kafka)
  props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
  props.put("value.serializer",classOf[KafkaAvroSerializer].getCanonicalName)
  props.put("client.id", UUID.randomUUID().toString())
  props.put("schema.registry.url",schemaRegistryUrl)
  props.put("auto.register.schemas", (false: java.lang.Boolean))
  private val producer =   new KafkaProducer[String,GenericRecord](props)

  private val schemaReg = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
  private val schemaMeta = schemaReg.getLatestSchemaMetadata(topic+ "-value")
  private val schema= schemaMeta.getSchema
  private val schemaParse =new Schema.Parser().parse(schema)
  private val operationsRecords=new OperationsRecord


  def sendCalendarRecordList(calendarList:List[CalendarRecord] ): Unit = {
    try {

      for( cal <- calendarList ){
        val genericRecord: GenericRecord =operationsRecords.parseCalendarToProduce(schemaParse,cal)
        producer.send(new ProducerRecord[String, GenericRecord](topic, null, genericRecord))

      }

    } catch {
      case ex: Exception =>
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
    }
    producer.close()
  }

  def sendStockRecordList(): Unit = {
    try {

      val stockList=operationsRecords.createStockRecord

      for( stock <- stockList ){
        val genericRecord: GenericRecord =operationsRecords.parseStockToProduce(schemaParse,stock)
        producer.send(new ProducerRecord[String, GenericRecord](topic, null, genericRecord))

      }

    } catch {
      case ex: Exception =>
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
    }
    producer.close()
  }


   def sendStockRecordListFromFile(fileName:String): Unit = {
    try {

      val stockList=operationsRecords.createStockRecordFromFile(fileName)

      for( stock <- stockList ){
        val genericRecord: GenericRecord =operationsRecords.parseStockToProduce(schemaParse,stock)
        producer.send(new ProducerRecord[String, GenericRecord](topic, null, genericRecord))

      }

    } catch {
      case ex: Exception =>
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
    }
    producer.close()
  }



}
