package StreamProcess

import java.io.IOException

import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

import scala.collection.JavaConversions._

/**
  * Created by kufu on 16-1-25.
  */
case class HTableData(row: Array[Byte],
                      family: Array[Byte],
                      qualifier: Array[Byte],
                      key: Long,
                      value: Array[Byte]) extends Comparable[HTableData]{

  def genPut(data: HTableData): Put = {
    val put: Put = new Put(data.row, data.key)
    put.add(data.family, data.qualifier, data.value)
    put
  }

  def compareTo(testData: HTableData): Int = {
    var ret: Int = 0
    ret = Bytes.BYTES_COMPARATOR.compare(row, testData.row)
    if (ret == 0) {
      ret = Bytes.BYTES_COMPARATOR.compare(family, testData.family)
      if (ret == 0) {
        ret = Bytes.BYTES_COMPARATOR.compare(qualifier, testData.qualifier)
        if (ret == 0) {
          ret = Bytes.BYTES_COMPARATOR.compare(value, testData.value)
          if (ret != 0) {
            printBytes(value)
            printBytes(testData.value)
          }
          else {
          }
        }
      }
    }
    ret
  }

  private def printBytes(byteArray: Array[Byte]) {
    if (byteArray.length == 8) {
      println(Bytes.toLong(byteArray))
    }
    else {
      print("[")
      var i: Int = 0
      byteArray.foreach(byte =>{
        if (i != 0) System.out.print(",\t")
        print(byte)
        i+=1
      })
      println("]")
    }
  }

//  def allCompareTo(testData: HTableData): Int = {
//    var ret: Int = 0
//    if ((({
//      ret = Bytes.BYTES_COMPARATOR.compare(row, testData.row); ret
//    })) == 0) {
//      if ((({
//        ret = Bytes.BYTES_COMPARATOR.compare(family, testData.family); ret
//      })) == 0) {
//        if ((({
//          ret = Bytes.BYTES_COMPARATOR.compare(qualifier, testData.qualifier); ret
//        })) == 0) {
//          if ((({
//            ret = (key - testData.key).toInt; ret
//          })) == 0) {
//            ret = Bytes.BYTES_COMPARATOR.compare(value, testData.value)
//            if (ret != 0) {
//              System.out.println("o:" + Bytes.toString(value))
//              System.out.println("c:" + Bytes.toString(testData.value))
//            }
//          }
//          System.out.println("o:" + key)
//          System.out.println("c:" + key)
//        }
//      }
//    }
//    return ret
//  }

  def genGet(data: HTableData, maxVersion: Int): Get = {
    val get: Get = new Get(data.row)
    try {
      get.setMaxVersions(maxVersion)
    }
    catch {
      case e: IOException => e.printStackTrace()
    }
    get
  }

  def getClone: HTableData = {
    new HTableData(this.row, this.family, this.qualifier, this.key, this.value)
  }

//  def genPut(data: HTableData, families: Array[String], others: Array[String]): Put = {
//    val put: Put = new Put(data.row)
//    {
//      var i: Int = 0
//      var j: Int = 0
//      while (i < families.length && j < others.length) {
//        {
//          put.add(Bytes.toBytes(families(i)), data.qualifier, data.value)
//          put.add(Bytes.toBytes(others(i)), data.qualifier, data.value)
//        }
//        ({
//          i += 1; i - 1
//        })
//        ({
//          j += 1; j - 1
//        })
//      }
//    }
//    return put
//  }
//
//  def genPut(data: HTableData, families: Array[String]): Put = {
//    val put: Put = new Put(data.row)
//    {
//      var i: Int = 0
//      while (i < families.length) {
//        {
//          put.add(Bytes.toBytes(families(i)), data.qualifier, data.value)
//        }
//        ({
//          i += 1; i - 1
//        })
//      }
//    }
//    return put
//  }
//
//  private var commonRandom: Random = null
//
//  def genValueString: String = {
//    if (commonRandom == null) commonRandom = new Random(System.currentTimeMillis)
//    val ret: Array[Byte] = new Array[Byte](8)
//    randomChars(ret, commonRandom)
//    return Bytes.toString(ret)
//  }
//
//  def show {
//    System.out.println("row:\t" + Bytes.toString(row) + ", \tfamily:\t" + Bytes.toString(family) + ",\t qualifier:\t" + Bytes.toString(qualifier) + ",\ttime:\t" + key + ",\tvalue:" + getValueStrign)
//  }

  override def toString: String = {
    "row:\t" + Bytes.toString(row) + ", \tfamily:\t" + Bytes.toString(family) + ",\t qualifier:\t" + Bytes.toString(qualifier) + ",\ttime:\t" + key + ",\tvalue:" + getValueString
  }

  def getValueString: String = {
    if (value.length == 8) {
      Bytes.toLong(value) + ""
    }
    else {
      val sb: StringBuffer = new StringBuffer()
      sb.append("[")

      var i = true
      value.foreach(v=> {
        if (i)
          i=false
        else
          sb.append(",\t")
        sb.append(v)
      })
      sb.append("]")
      sb.toString
    }
  }
}

object HTableData{
  def getHTableData(result: Result): List[HTableData] = {
    val retData = new ListBuffer[HTableData]
    val row: Array[Byte] = result.getRow
    val familyMap= result.getMap
    familyMap.keySet().foreach(family=>{
      val qualifiers = familyMap.get(family)
      qualifiers.keySet().foreach(qualifier=>{
        val timestamps = qualifiers.get(qualifier)
        timestamps.navigableKeySet().foreach(timestamp=>{
          val data = new HTableData(row,
            family, qualifier, timestamp, timestamps.get(timestamp))
          retData+=data
        })
      })
    })
    retData.toList
  }
}
