package com.nehme

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LoadHBase {
  def main(args: Array[String]) {

    val inputdata = args(0)
    val hbase_table = args(1)
    val hfile_name = args(2)
    val columnFamily = args(3)

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    val tableName = hbase_table
    val table = new HTable(conf, tableName)

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(conf)

    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])

    HFileOutputFormat2.configureIncrementalLoad (job, table)

    val originalRDD = sc.textFile(inputdata)
        .map(line => {
            val arr = line.split(",")
            (arr(0), arr(1))
        })

    val result = originalRDD.distinct().sortByKey(numPartitions = 1).map(x => {
        val ssn = x._1
        val msdn = x._2
        val kv: KeyValue = new KeyValue(Bytes.toBytes(ssn), Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes(msdn))
        (new ImmutableBytesWritable(Bytes.toBytes(ssn)), kv)
    })

    result.saveAsNewAPIHadoopFile(hfile_name, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)

    val bulkLoader = new LoadIncrementalHFiles(conf)
    val t0 = System.currentTimeMillis()
    bulkLoader.doBulkLoad(new Path(hfile_name + t0 ), table)
 }
}

