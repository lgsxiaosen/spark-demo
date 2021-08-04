package utils

import java.sql.{Connection, DriverManager}
import java.util.Objects

object MysqlConnection {

  /**
   * 获取mysql连接
   * @return
   */
  def getConnection: Connection = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?useSSL=false&useUnicode=true&characterEncoding=UTF-8", "root", "root")
  }

  def releaseConnection(conn: Connection): Unit = {
    if (Objects.nonNull(conn)){
      conn.close()
    }
  }



}
