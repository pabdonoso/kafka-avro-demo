package main.com.pablo.kafka.scala

import java.util.{Collections, Properties}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.StringDeserializer

class ConsumerKafkaAvroDeserializer(val topic:String, val kafkaServer:String, val schemaRegistryUrl:String) {

  private val props = new Properties()
  val groupId = "calendar-test-gid"
  props.put("bootstrap.servers", kafkaServer)

  val schemaReg = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
  val schemaMeta = schemaReg.getLatestSchemaMetadata(topic + "-value")
  val schema= schemaMeta.getSchema
  val schemaParse =new Schema.Parser().parse(schema)

  var shouldRun : Boolean = true

  props.put("group.id", groupId)
  props.put("auto.commit.interval.ms", "10000")
  props.put("session.timeout.ms", "30000")
  props.put("consumer.timeout.ms", "120000")
  props.put("key.deserializer", classOf[StringDeserializer].getCanonicalName)
  props.put("value.deserializer",classOf[KafkaAvroDeserializer].getCanonicalName)
  //props.put("auto.offset.reset", "earliest")
  props.put("auto.offset.reset", "latest")
  props.put("enable.auto.commit", false: java.lang.Boolean)
  props.put("schema.registry.url",schemaRegistryUrl)

  //With Kerberos
 /* props.put("security.protocol","SASL_PLAINTEXT")
  props.put("sasl.kerberos.service.name", "kafka")*/


  private val consumer = new KafkaConsumer[String, GenericRecord](props)

  def startCalendar() = {
    try {

      consumer.subscribe(Collections.singletonList(topic))
      while (shouldRun) {

        val records: ConsumerRecords[String,  GenericRecord] = consumer.poll(10)
        val it = records.iterator()
        while(it.hasNext()) {
          val record: ConsumerRecord[String,  GenericRecord] = it.next()
          parseCalendar(record.value())
          println(""+record.value())
          consumer.commitSync
        }
      }
    }
    catch {
      case timeOutEx: TimeoutException =>
        println("Timeout ")
        false
      case ex: Exception => ex.printStackTrace()
        println("Got error when reading message ")
        false
    }
  }

  def startStock() = {
    try {

      consumer.subscribe(Collections.singletonList(topic))
      while (shouldRun) {

        val records: ConsumerRecords[String,  GenericRecord] = consumer.poll(10)
        val it = records.iterator()
        while(it.hasNext()) {
          val record: ConsumerRecord[String,  GenericRecord] = it.next()
          parseStock(record.value())
          println(""+record.value())
          consumer.commitSync
        }
      }
    }
    catch {
      case timeOutEx: TimeoutException =>
        println("Timeout ")
        false
      case ex: Exception => ex.printStackTrace()
        println("Got error when reading message ")
        false
    }
  }



  private def parseCalendar(message: GenericRecord): CalendarRecord= {

    CalendarRecord(
      message.get("DATE_DAY").toString,
      message.get("WERKS").toString,
      message.get("YEAR").toString.toInt,
      message.get("STATE").toString,
      if (message.get("DESCRIPTION") != null) {
        message.get("DESCRIPTION").toString
      }else{null},

      message.get("ACTIVE").toString.toBoolean
    )
  }


  private def parseStock(message: GenericRecord): StockRecord= {
    StockRecord(
      message.get("MATNR").toString,
      message.get("WERKS").toString,
      message.get("ID_STORAGE_LOC").toString,
      message.get("UNIDAD_MEDIDA").toString,
      message.get("FECHA_CREACION_SAP").toString,
      message.get("COSTE_UNITARIO_LIBRE_DISPOSICION").toString.toDouble,
      message.get("COSTE_UNITARIO_BLOQUEADO").toString.toDouble,
      message.get("COSTE_UNITARIO_INSPECCION").toString.toDouble,
      message.get("MONEDA").toString,
      message.get("UNIDADES_LIBRE_DISPOSICION").toString.toDouble,
      message.get("UNIDADES_BLOQUEADO").toString.toDouble,
      message.get("UNIDADES_INSPECCION").toString.toDouble
    )
  }


  def close(): Unit = shouldRun = false

}
