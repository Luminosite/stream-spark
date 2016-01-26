package StreamProcess

import java.io.IOException

import org.apache.commons.logging.{LogFactory, Log}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Increment}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.{Reporter, RecordWriter, JobConf, FileOutputFormat}
import org.apache.hadoop.util.Progressable

/**
  * Created by kufu on 16-1-25.
  */
class TableIncrementFormat extends FileOutputFormat[ImmutableBytesWritable, Increment]{

  val LOG:Log = LogFactory.getLog(TableIncrementFormat.getClass)

  override def getRecordWriter(fileSystem: FileSystem,
                               jobConf: JobConf,
                               s: String,
                               progressable: Progressable)
                              : RecordWriter[ImmutableBytesWritable, Increment] = {

    val tableName = jobConf.get(TableIncrementFormat.OUTPUT_TABLE)

    var hTable: HTable = null
    try{
      hTable = new HTable(HBaseConfiguration.create(jobConf), tableName)
    }catch{
      case e:IOException =>
        LOG.error(e)
        e.printStackTrace()
        throw e
    }

    hTable.setAutoFlush(false, true)

    new TableIncrementWriter(hTable)
  }

  class TableIncrementWriter(table:HTable) extends RecordWriter[ImmutableBytesWritable, Increment]{

    override def write(k: ImmutableBytesWritable, v: Increment): Unit = {
      table.increment(v)
    }

    override def close(reporter: Reporter): Unit = {
      table.close()
    }
  }
}

object TableIncrementFormat{
  val OUTPUT_TABLE = "hbase.mapred.outputtable"
}