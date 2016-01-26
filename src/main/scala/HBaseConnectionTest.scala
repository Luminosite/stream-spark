import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}

/**
  * Created by kufu on 16-1-13.
  */
class HBaseConnectionTest {

  /**
    * Admin and tables must be closed after connection, or there're exceptions after execution.
    */
  def run(): Unit ={
    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "localhost")
    val tableName:String = "MyTestTable"
    val families:Array[String] = Array("f1","f2","f3")

    val admin = new HBaseAdmin(conf)
    if(!admin.tableExists(tableName)){
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      families.foreach(family=>tableDescriptor.addFamily(new HColumnDescriptor(family)))
      admin.createTable(tableDescriptor)
      admin.close()
    }

    def put(table_name:String,
            row:String,
            family:String,
            column:String,
            data:String): Unit ={
      val table = new HTable(conf, table_name)
      val put = new Put(Bytes.toBytes(row))
      put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(data))
      table.put(put)
      table.close()
    }

    put(tableName, "row1", families(0), "c1", "data1")
    put(tableName, "row2", families(0), "c1", "data2")
    put(tableName, "row3", families(0), "c1", "data3")
    put(tableName, "row1", families(1), "c1", "data1_f2")

  }
}
