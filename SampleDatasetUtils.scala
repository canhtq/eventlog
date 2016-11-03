

class SampleDatasetUtils {
  
  def makeUserIdDs(){
    val users = (1 to 2).map { i =>;
        val g="bc"
        UserId(g, s"user$i")
      }
      sc.parallelize(users).toDF().coalesce(1).write.mode("overwrite").format("orc").save("tmp/bc/users")
  }
  
  def makeEventDs(){
    val login = (1 to 2).map { i =>;
        val d="2016-10-03"
        val g="bc"
        val e="login"
        val t=s"$d 12:12:12"
        val m = Map("os"->s"android","device_name"->"samsung","device_id"->s"$e$i")
        Event(t, e, g, s"user$i", s"server_$i", m)
      }
    
      val pay = (1 to 2).map { i =>;
        val g="bc"
        val e="pay"
        val d="2016-10-03"
        val t=s"$d 12:12:12"
        val pm = Map("os"->"android","device_name"->"samsung","device_id"->s"$e$i","net"->"1000") 
        Event(t, e, g, s"user$i", s"server_$i", pm)
      }
      val pay2 = (1 to 2).map { i =>;
        
        val g="bc"
        val e="pay"
        val d="2016-10-03"
        val t=s"$d 12:12:12"
        val pm = Map("os"->"ios","device_name"->"samsung","device_id"->s"$e$i","net"->"1500")
        Event(t, e, g, s"user$i", s"server_$i", pm)
      }
      val logs = login.union(pay).union(pay2)
      sc.parallelize(logs).toDF().coalesce(1).write.mode("overwrite").format("orc").save("tmp/bc/events")
  }
  
}