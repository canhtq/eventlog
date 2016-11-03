
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.text.ParseException
import java.util.Locale
import java.util.ArrayList
import org.apache.spark.sql.types.{StructType, StructField, StringType};

class SparkT {
  
 //path:.../events/yyyy-MM-dd (builded by "etl process")
  case class Event(
      time: String
      ,action:String //login,logout,create,pay...
      ,game_code: String
      ,user_id: String
      ,server_id:String
      ,props: scala.collection.immutable.Map[String,String]
      )
  //path:.../user_id/yyyy-MM-dd (builded by "stats process")
  case class UserId(
          game_code: String
          ,user_id: String
      )
  //path:.../new-events/yyyy-MM-dd (builded by "stats process")
  // structure Event
  
  def makeEventDataset(df: org.apache.spark.sql.DataFrame, cnfCols: Map[String,String]):org.apache.spark.sql.Dataset[Event]={
    var tmpDf = df
    cnfCols foreach {(cnf) =>
     tmpDf =  tmpDf.withColumnRenamed(cnf._1, cnf._2)
    }
    val stsCols = Set("time", "action", "user_id", "server_id","game_code","props")
    val ds = tmpDf.map ( e=> {
      var props:Map[String,String] = Map()
      val schema = e.schema
      for( i <- 0 to schema.length-1){
        val colName = schema.apply(i).name
        if(!stsCols(colName))
        {
          props = props + (colName->e.getAs[String](colName))
        }
      }
      Event(e.getAs[String]("time"), "login", e.getAs[String]("game_code"), e.getAs[String]("user_id"), e.getAs[String]("server_id"), props)
    }
    )
    ds
  }
  def makeEvents(gameCode:String, date:String, mapFiels: Map[String,String]):org.apache.spark.sql.Dataset[Event]={
    
    //game etl implement here:
    val path:String = s"/user/fairy/testdata/login-$date.txt"
    //read data, join, filter...
    val df = spark.read.option("delimiter",",").option("header", "true").csv(path)
    
    
    //finally ==> map fields
    makeEventDataset(df,mapFiels)
  }
  
  
  def readOldUsersDs(gameCode:String, date:String):org.apache.spark.sql.Dataset[UserId]={
    try {
      val dst = spark.read.format("orc").load("tmp/bc/users").as[UserId]
      dst
    } catch {
      case _: Throwable => {
        spark.emptyDataset[UserId]
      }
    }
  }
  
  def makeProfile(gameCode:String, eventDate:String, events:org.apache.spark.sql.Dataset[Event]):org.apache.spark.sql.Dataset[Event]={
    val users = readOldUsersDs(gameCode,eventDate)
    //val newLogin = events.joinWith(pps,events("user_id")===pps("user_id"),"left_outer").select("_1").where("_2 is null")
    //val newDs = events.joinWith(pps,events("user_id")===pps("user_id"),"left_outer").select("_1").where("_2 is null").select("_1.time","_1.action","_1.game_code","_1.user_id","_1.server_id","_1.props").as[Event]
    val profileDs = events.joinWith(users,events("user_id")===users("user_id"),"left_outer").where("_2 is null").select("_1.time","_1.action","_1.game_code","_1.user_id","_1.server_id","_1.props").as[Event]
    //val newIdLogins = events.select("user_id").where("action='login'").joinWith(pps,events("user_id")===pps("user_id"),"left_outer").select("_1.user_id").where("_2.logins is null")
    //val newLogins = events.joinWith(newIdLogins,events("user_id")===newIdLogins("user_id")).select("_1.time","_1.action","_1.game_code","_1.user_id","_1.server_id","_1.props").as[Event]
    
    //val newIdPayments = events.select("user_id").where("action='pay'").joinWith(pps,events("user_id")===pps("user_id"),"left_outer").select("_1.user_id").where("_2.payments is null")
    
    //val newPayments = events.joinWith(newIdPayments,events("user_id")===newIdPayments("user_id")).select("_1.time","_1.action","_1.game_code","_1.user_id","_1.server_id","_1.props").as[Event]
        
    profileDs
  }
  
  
  def makeNewDs(gameCode:String, eventDate:String, events:org.apache.spark.sql.Dataset[Event]):org.apache.spark.sql.Dataset[Event]={
    val users = readOldUsersDs(gameCode,eventDate)
    val newDs = events.joinWith(users,events("user_id")===users("user_id"),"left_outer").where("_2 is null").select("_1.time","_1.action","_1.game_code","_1.user_id","_1.server_id","_1.props").as[Event]
    newDs
  }
  
  def apply(){
      val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.sql.orc.filterPushdown", "true").enableHiveSupport.getOrCreate()
  
      import spark.implicits._
      import spark.sql
      var mapFiels = Map("log_date"->"time","game_code"->"game_code","user_id"->"user_id","server_id"->"server_id","os"->"os_type")
      val events = makeEvents("bc","2016-10-01",mapFiels)
      events.show
      events.printSchema
      val newDs = makeNewDs("bc","2016-10-01",events)
      newDs.show
      newDs.printSchema
  }
  
}