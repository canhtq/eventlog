
import org.apache.spark.sql.Row
 import org.apache.spark.sql.SparkSession
 import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.text.ParseException
import java.util.Locale
import java.util.ArrayList
class SparkT {
  
  def readFiles(){
    val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.sql.orc.filterPushdown", "true").enableHiveSupport.getOrCreate()

    import spark.implicits._
    import spark.sql

    
    val dates = List("2016-10-03", "2016-10-04", "2016-10-05", "2016-10-06", "2016-10-07", "2016-10-08", "2016-10-09", "2016-10-10", "2016-10-11", "2016-10-12")
    val rand = scala.util.Random
    dates.foreach { d=>; 
        val events = (1 to 20).map { i =>;
            val gc="bc"
            var action="login"
            
            val serverId= i%2+1;
            val id= i%2+1;
            var os = "ios"
            var m = Map("device_name"->"samsung","device_id"->s"$action$i")
            
            val r =rand.nextInt(10000)
            if(r%2 ==0){
              action="logout"
              os="android"
              
            }else if(r%3 ==0){
              action="pay"
              
            }
            if(action == "pay"){
              m = m + ("net"->s"$r")
              if(i%2==0){
                os = "ios"
              }else{
                os = "android"
              }
            }
            m = m + ("os"->s"$os")
            val t=s"$d 12:12:12"
            Event(t, action, gc, s"user_$id", s"$serverId", m)
          }
         sc.parallelize(events).toDF().coalesce(1).write.mode("overwrite").format("parquet").save(s"tmp/a-schema/bc/$d/events")
      }
    val dsEvents = spark.read.format("parquet").load("tmp/a-schema/bc/*/events").as[Event]
    
    dsEvents.select("*").where("props.os = 'android'").show
    
    dsEvents.select("props.net", "props.os").where("action='pay'").groupBy("os").agg(sum($"net")).show
    
    
}