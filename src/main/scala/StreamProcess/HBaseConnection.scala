package StreamProcess

import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client._

/**
  * Created by kufu on 16-1-25.
  */
class HBaseConnection(tableName:String, families:List[String]) {

  val conf = HBaseConfiguration.create()
  var table:HTable = null

  def openOrCreateTable() = {
    //There must be an available table name.
    if(tableName==null || tableName.length==0)
      throw new Exception("No available table name.")

    val hbaseAdmin = new HBaseAdmin(conf)
    try{
      if(hbaseAdmin.tableExists(tableName)){
        //get table if it existed.
        table = new HTable(conf, tableName)
      }else{
        //try to create table
        if(families == null || families.isEmpty){
          throw new Exception("No available families names to create table.")
        }else{
          val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
          families.foreach(family=>descriptor.addFamily(new HColumnDescriptor(family)))
          hbaseAdmin.createTable(descriptor)
        }
      }
    } finally {
      //What ever, close table
      hbaseAdmin.close()
    }
  }

  def checkTable(): Unit = {
    if(table==null)
      throw new Exception("Try to get or create table firstly.")
  }

  def scan(scan:Scan): ResultScanner ={
    checkTable()
    table.getScanner(scan)
  }

  def deleteList(dels: List[Delete]) = {
    val list = new java.util.LinkedList[Delete]()
    dels.foreach(list.add)
    table.delete(list)
  }

  def close(): Unit ={
    if(table!=null) {
      table.close()
      table = null
    }
  }
}
