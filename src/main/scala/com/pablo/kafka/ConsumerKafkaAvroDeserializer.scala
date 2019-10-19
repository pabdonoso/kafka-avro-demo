package main.com.pablo.kafka.scala

import java.util.{Collections, Properties}
import com.pablo.records.OperationsRecord
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.StringDeserializer

class ConsumerKafkaAvroDeserializer(val topic:String, val kafkaServer:String, val schemaRegistryUrl:String) {

  private val props = new Properties()
  private val groupId = "calendar-test-gid"
  props.put("bootstrap.servers", kafkaServer)

  private val schemaReg = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
  private val schemaMeta = schemaReg.getLatestSchemaMetadata(topic + "-value")
  private val schema= schemaMeta.getSchema
  private val schemaParse =new Schema.Parser().parse(schema)

  private var shouldRun : Boolean = true

  props.put("group.id", groupId)
  props.put("auto.commit.interval.ms", "10000")
  props.put("session.timeout.ms", "30000")
  props.put("consumer.timeout.ms", "120000")
  props.put("key.deserializer", classOf[StringDeserializer].getCanonicalName)
  props.put("value.deserializer",classOf[KafkaAvroDeserializer].getCanonicalName)
  props.put("auto.offset.reset", "latest")
  props.put("enable.auto.commit", false: java.lang.Boolean)
  props.put("schema.registry.url",schemaRegistryUrl)

  //With Kerberos
 /* props.put("security.protocol","SASL_PLAINTEXT")
  props.put("sasl.kerberos.service.name", "kafka")*/


  private val consumer = new KafkaConsumer[String, GenericRecord](props)
  private val operationsRecords=new OperationsRecord

  def readCalendarTopic() = {
    try {

      consumer.subscribe(Collections.singletonList(topic))
      while (shouldRun) {

        val records: ConsumerRecords[String,  GenericRecord] = consumer.poll(10)
        val it = records.iterator()
        while(it.hasNext()) {
          val record: ConsumerRecord[String,  GenericRecord] = it.next()
          operationsRecords.parseCalendarToConsume(record.value())
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

  def readStockTopic() = {
    try {

      consumer.subscribe(Collections.singletonList(topic))
      while (shouldRun) {

        val records: ConsumerRecords[String,  GenericRecord] = consumer.poll(10)
        val it = records.iterator()
        while(it.hasNext()) {
          val record: ConsumerRecord[String,  GenericRecord] = it.next()
          operationsRecords.parseStockToConsume(record.value())
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




  def close(): Unit = shouldRun = false

}
