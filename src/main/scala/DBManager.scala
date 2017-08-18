import java.io.{File, FileInputStream, InputStream}
import java.sql.Connection
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * Created by Jun on 2017/8/1.
  */
class DBManager() extends Serializable{
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource()
  private val prop = new Properties()
  //private val in: InputStream = getClass.getResourceAsStream("/conf/c3p0.properties")
  private val in: FileInputStream = new FileInputStream(new File("conf/c3p0.properties"))

  try {
    prop.load(in);
    cpds.setJdbcUrl(prop.getProperty("jdbcUrl"))
    cpds.setDriverClass(prop.getProperty("driverClass"))
    cpds.setUser(prop.getProperty("user"))
    cpds.setPassword(prop.getProperty("password"))
  } catch {
    case ex: Exception => ex.printStackTrace()
  }



  def getConnection:Connection={
    try {
      return cpds.getConnection()
    } catch {
      case ex:Exception => ex.printStackTrace()
        null
    }
  }
}

object DBManager{
  var mdbManager:DBManager=_
  def getMDBManager():DBManager={
    synchronized{
      if(mdbManager==null){
        mdbManager = new DBManager()
      }
    }
    mdbManager
  }
}
