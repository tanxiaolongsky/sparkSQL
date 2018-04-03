package con.txl.sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RDDDataFrameReflection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdddatafromareflection")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val fileRDD = sc.textFile("hdfs://192.168.21.30:9000/tmp/students.txt")
    val lineRDD = fileRDD.map(line => line.split(","))
    //将RDD和case class关联
    val studentsRDD = lineRDD.map(x => Students(x(0).toInt,x(1),x(2).toInt))
    //在scala中使用反射方式，进行rdd到dataframe的转换，需要手动导入一个隐式转换
    import sqlContext.implicits._
    val studentsDF = studentsRDD.toDF()
    //注册表
    studentsDF.registerTempTable("students")
    val df = sqlContext.sql("select * from students")
    df.rdd.foreach(row => println(row(0)+","+row(1)+","+row(2)))
    df.rdd.saveAsTextFile("hdfs://192.168.21.30:9000/tmp/studentsout")


  }

}
//放到外面
case class Students(id:Int,name:String,age:Int)