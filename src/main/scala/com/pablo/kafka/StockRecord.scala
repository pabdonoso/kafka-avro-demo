package main.com.pablo.kafka.scala

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
