package StreamProcess.tools.hbaseCleaner

import StreamProcess.{HTableData, HBaseConnection}
import org.apache.hadoop.hbase.client.{Delete, Scan}

import scala.collection.mutable.ListBuffer

/**
  * Created by kufu on 16-1-27.
  */
object HBaseTableCleaner {
  def main(args: Array[String]) {
    var connection:HBaseConnection = null
    val argsList = new ListBuffer[String]
    args.foreach(argsList+=_)

    argsList.toList match {
      case Nil => println("usage: cleaner table family...")
      case x::Nil => println("number of arguments are not enough:")
        println("usage: cleaner table family...")
      case head::tail => connection = new HBaseConnection(head, tail)
    }
    if(connection!=null){
      connection.openOrCreateTable()
      val list:List[HTableData]= HTableData.getHTableData(
        connection.scan(new Scan()))
      val dels:List[Delete] = HTableData.getDeletes(list)
      connection.deleteList(dels)
      connection.close()
    }
  }
}
