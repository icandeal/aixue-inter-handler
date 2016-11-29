package com.etiantian

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger

/**
  * Created by sniper on 16-10-25.
  */
class HBaseUtils(hMaster:String, quorum:String, zkPort:String) extends Serializable{
  val logger = Logger.getLogger("HBaseUtils")

  var connection = getConnection()
  var admin = connection.getAdmin

  def getConnection(): Connection = {
    val conf = new Configuration()
    conf.set("hbase.master", hMaster)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    conf.set("hbase.zookeeper.quorum", quorum)
    ConnectionFactory.createConnection(conf)
  }

  def createTableIfNotExist(tableName: String, familyName: String*): Unit = {
    if (this.admin == null || this.connection == null) {
      this.connection = getConnection()
      this.admin = connection.getAdmin
    }

    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
      for (family <- familyName) {
        descriptor.addFamily(new HColumnDescriptor(family))
      }
      admin.createTable(new HTableDescriptor(descriptor))
    }
  }

  def insert(tableName: String, put: Put): Unit = {
    if (this.admin == null || this.connection == null) {
      this.connection = getConnection()
      this.admin = connection.getAdmin
    }
    getConnection().getTable(TableName.valueOf(tableName)).put(put)

  }

  def insert(tableName: String, put:java.util.List[Put]) : Unit ={
    if (this.admin == null || this.connection == null) {
      this.connection = getConnection()
      this.admin = connection.getAdmin
    }
    getConnection().getTable(TableName.valueOf(tableName)).put(put)
  }

  def close(): Unit = {
    try {
      if (this.admin != null) {
        this.admin.close()
      }
      if (this.connection != null) {
        this.connection.close()
      }
    } catch {
      case e: Exception => {
        logger.error(e)
      }
    }
  }
}
