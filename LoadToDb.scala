%scala
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.SaveMode

def LoadFtomDataBrickstoDb(databasesource: String,
                           tablenamesource: String,
                           databasetarget: String,
                           tablenametarget: String,
                           servername : String,
                           user : String,
                           password : String,
                           sc: SparkContext): String = 
try{
  
  
    val totalnametarget=databasetarget + "." + tablenametarget
  
    val config = Config(Map(
        "url"          -> servername,
        "databaseName" -> databasetarget,
        "user"         -> user,
        "password"     ->password,
        "dbTable"      -> totalnametarget
        ))

    val metadataonly = spark.sql("""select * from """ +  databasesource + """.""" + tablenamesource + """ where 1=0""")

    metadataonly.write.mode(SaveMode.Append).sqlDB(config)
  
  
    val bulkCopyConfig = Config(Map(
        "url"          -> servername,
        "databaseName" -> databasetarget,
        "user"         -> user,
        "password"     ->password,
        "dbTable"      -> totalnametarget
        //This option can be setup to increase performance.
        "bulkCopyBatchSize" -> "100000",
        "bulkCopyTableLock" -> "true",
        "bulkCopyTimeout"   -> "600"
            ))  
  
    val mydf1 = spark.sql("""select * from """ +  databasesource + """.""" + tablenamesource)
    mydf1.bulkCopyToSqlDB(bulkCopyConfig)
    return("ok")    
    } 
        catch {
        case e: Exception => {
            println("Error executing " + e.getMessage)
            throw e
        }
    }
