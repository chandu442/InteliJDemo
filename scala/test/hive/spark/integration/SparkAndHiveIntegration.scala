package test.hive.spark.integration

import org.apache.spark.sql.{SparkSession, hive}

object SparkAndHiveIntegration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark hive integration")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:8020//home/orienit/spark/warehouse")
      //.config("hive.metastore.uris", "jdbc:mysql://localhost/metastore")
      //.config("hive.metastore.warehouse.dir", "hdfs://localhost:8020//home/orienit/spark/warehouse")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/hive_spark_mysql_db?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "root")
      .config("javax.jdo.option.ConnectionPassword", "hadoop")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show()

    //spark.sql("create database if not exists one")

    //spark.sql("show databases").show()

    //spark.sql("select * from k_c_r.orders").show()
    import spark.implicits._
    //import spark.sql
    //spark.catalog.listDatabases().show(false)
    //spark.sql("show databases").show()


  }
}
