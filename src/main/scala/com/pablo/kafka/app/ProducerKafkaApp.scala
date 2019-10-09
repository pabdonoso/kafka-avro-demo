package main.com.pablo.kafka.scala.app


import main.com.pablo.kafka.scala.{CalendarRecord, ProducerKafkaAvroSerializer, StockRecord}

import scala.collection.mutable.ListBuffer
import scala.io.Source


object ProducerKafkaApp extends App {

  //Confluent Local - Docker
  val kafkaConfluent="localhost:29092"
  val topic="topic"

  val configPath = getClass.getClassLoader.getResource(".").getPath



  val producer = new ProducerKafkaAvroSerializer(topic,kafkaConfluent)

  producer.sendStock(createStockRecordFromFile)
  //producer.sendStock(createStockRecord)

  System.out.println("Producer kafka OK")

  def createCalendarRecord(): List[CalendarRecord] ={
    val listCalendarRecords: List[CalendarRecord] = List(
      CalendarRecord("2021-12-31 00:00:00","000H",2021,"O",null,true),
      CalendarRecord("2021-12-30 00:00:00","0009",2021,"O",null,true),
      CalendarRecord("2021-12-29 00:00:00","0011",2021,"O",null,true),
      CalendarRecord("2021-01-01 00:00:00","0007",2021,"C","Festivo",true),
      CalendarRecord("2021-09-27 00:00:00","000Y",2021,"C","Festivo",true))

    listCalendarRecords
  }


  def createStockRecord(): List[StockRecord] ={

    val listStockRecords: List[StockRecord] =
      List(
        StockRecord("2782036","6103","0001","EA","2019-05-30 00:00:00",0.0,0.0,0.0,"EUR",0.0,0.0,0.0),
        StockRecord("3122024","6103","0001","EA","2019-05-30 00:00:00",0.0,0.0,0.0,"EUR",0.0,0.0,0.0),
        StockRecord("3172010","6103","0001","EA","2019-05-30 00:00:00",0.0,0.0,0.0,"EUR",0.0,0.0,0.0),
        StockRecord("3172018","6103","0001","EA","2019-05-30 00:00:00",0.0,0.0,0.0,"EUR",0.0,0.0,0.0),
        StockRecord("3232021","6103","0001","EA","2019-05-30 00:00:00",0.0,0.0,0.0,"EUR",0.0,0.0,0.0))

    listStockRecords
  }




  def createStockRecordFromFile(): List[StockRecord] ={

    var listaStock = new ListBuffer[StockRecord]()

      val events:List[String] = Source.fromFile(configPath+"store_stock_matrix.txt").getLines().toList
      //val events:List[String] = Source.fromFile(configPath+"10_events_store_stock_matrix.txt").getLines().toList
      //val events:List[String] = Source.fromFile(configPath+"4_events_test.txt").getLines().toList


    events.foreach(x=>
      listaStock+=StockRecord(x.split(",")(0).toString,x.split(",")(1).toString,x.split(",")(2).toString,x.split(",")(3).toString,
        x.split(",")(4).toString,x.split(",")(5).toDouble,x.split(",")(6).toDouble,x.split(",")(7).toDouble,
        x.split(",")(8).toString,x.split(",")(9).toDouble,x.split(",")(10).toDouble,x.split(",")(11).toDouble)
    )
    listaStock.toList

  }


}

