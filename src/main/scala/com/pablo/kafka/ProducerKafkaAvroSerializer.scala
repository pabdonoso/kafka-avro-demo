package main.com.pablo.kafka.scala

import java.util.{Properties, UUID}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


class ProducerKafkaAvroSerializer(val topic:String, val kafka:String) {

  private val props = new Properties()

  /*config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer]);
config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer]);*/

  props.put("bootstrap.servers", kafka)
  props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
  props.put("value.serializer",classOf[KafkaAvroSerializer].getCanonicalName)
  props.put("client.id", UUID.randomUUID().toString())

  //Confluent Local - Docker
  //val schemaRegistryUrl="http://localhost:8081"

  //Confluent DESA
 val schemaRegistryUrl="http://ld6cf03.es.wcorp.carrefour.com:8081"
  props.put("schema.registry.url",schemaRegistryUrl)
  props.put("auto.register.schemas", (false: java.lang.Boolean))

  private val producer =   new KafkaProducer[String,GenericRecord](props)




  //Read avro schema
  val schemaReg = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
  val schemaMeta = schemaReg.getLatestSchemaMetadata(topic+ "-value")
  val schema= schemaMeta.getSchema
  val schemaParse =new Schema.Parser().parse(schema)


  def sendCalendar(calendarList:List[CalendarRecord] ): Unit = {
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


  def sendStock(stockList:List[StockRecord] ): Unit = {
    try {

      for( stock <- stockList ){
        val genericRecord: GenericRecord =parseStock(schemaParse,stock)
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

  def parseStock(schemaParse:Schema,stock:StockRecord ): GenericRecord ={

    val genericStock: GenericRecord = new GenericData.Record(schemaParse)

    genericStock.put("MATNR",stock.MATNR)
    genericStock.put("WERKS",stock.WERKS)
    genericStock.put("ID_STORAGE_LOC",stock.ID_STORAGE_LOC)
    genericStock.put("UNIDAD_MEDIDA",stock.UNIDAD_MEDIDA)
    genericStock.put("FECHA_CREACION_SAP",stock.FECHA_CREACION_SAP)
    genericStock.put("COSTE_UNITARIO_LIBRE_DISPOSICION",stock.COSTE_UNITARIO_LIBRE_DISPOSICION)
    genericStock.put("COSTE_UNITARIO_BLOQUEADO",stock.COSTE_UNITARIO_BLOQUEADO)
    genericStock.put("COSTE_UNITARIO_INSPECCION",stock.COSTE_UNITARIO_INSPECCION)
    genericStock.put("MONEDA",stock.MONEDA)
    genericStock.put("UNIDADES_LIBRE_DISPOSICION",stock.UNIDADES_LIBRE_DISPOSICION)
    genericStock.put("UNIDADES_BLOQUEADO",stock.UNIDADES_BLOQUEADO)
    genericStock.put("UNIDADES_INSPECCION",stock.UNIDADES_INSPECCION)
    genericStock
  }


}
