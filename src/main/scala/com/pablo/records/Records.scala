package com.pablo.records

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.collection.mutable.ListBuffer
import scala.io.Source


case class StockRecord(
                        MATNR: String,
                        WERKS: String,
                        ID_STORAGE_LOC: String,
                        UNIDAD_MEDIDA: String,
                        FECHA_CREACION_SAP: String,
                        COSTE_UNITARIO_LIBRE_DISPOSICION: Double,
                        COSTE_UNITARIO_BLOQUEADO: Double,
                        COSTE_UNITARIO_INSPECCION: Double,
                        MONEDA: String,
                        UNIDADES_LIBRE_DISPOSICION: Double,
                        UNIDADES_BLOQUEADO: Double,
                        UNIDADES_INSPECCION: Double
                      )

case class CalendarRecord (
                            DATE_DAY: String,
                            WERKS: String,
                            YEAR: Integer,
                            STATE: String,
                            DESCRIPTION: String,
                            ACTIVE: Boolean
                          )


trait Records {

  def createCalendarRecord():List[CalendarRecord]
  def createStockRecord(): List[StockRecord]
  def createStockRecordFromFile(fileName:String):List[StockRecord]
  def parseCalendarToProduce(schemaParse:Schema,calendar:CalendarRecord):GenericRecord
  def parseStockToProduce(schemaParse:Schema,stock:StockRecord):GenericRecord
  def parseCalendarToConsume(message: GenericRecord):CalendarRecord
  def parseStockToConsume(message: GenericRecord):StockRecord
}



class OperationsRecord extends Records{

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


  def createStockRecordFromFile(fileName:String): List[StockRecord] ={

    var listaStock = new ListBuffer[StockRecord]()
    val events:List[String] = Source.fromFile(fileName).getLines().toList
    events.foreach(x=>
      listaStock+=StockRecord(x.split(",")(0).toString,x.split(",")(1).toString,x.split(",")(2).toString,x.split(",")(3).toString,
        x.split(",")(4).toString,x.split(",")(5).toDouble,x.split(",")(6).toDouble,x.split(",")(7).toDouble,
        x.split(",")(8).toString,x.split(",")(9).toDouble,x.split(",")(10).toDouble,x.split(",")(11).toDouble)
    )
    listaStock.toList
  }


  def parseCalendarToProduce(schemaParse:Schema,calendar:CalendarRecord ): GenericRecord ={
    val genericCalendar: GenericRecord = new GenericData.Record(schemaParse)
      genericCalendar.put("DATE_DAY", calendar.DATE_DAY)
      genericCalendar.put("WERKS", calendar.WERKS)
      genericCalendar.put("YEAR", calendar.YEAR)
      genericCalendar.put("STATE", calendar.STATE)
      genericCalendar.put("DESCRIPTION", calendar.DESCRIPTION)
      genericCalendar.put("ACTIVE", calendar.ACTIVE)
      genericCalendar
  }

  def parseStockToProduce(schemaParse:Schema,stock:StockRecord ): GenericRecord ={

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


   def parseCalendarToConsume(message: GenericRecord): CalendarRecord= {

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


   def parseStockToConsume(message: GenericRecord): StockRecord= {
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

}