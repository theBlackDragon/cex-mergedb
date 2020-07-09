package be.lair.conan.mergedb.db

import java.io.File
import java.sql.{Connection, DriverManager}

import org.sqlite.JDBC

object SaveDatabase {

  private val dbUrlBase = "jdbc:sqlite:"

  DriverManager.registerDriver(new JDBC)

  def open(db: File): SaveDatabase = {
    open(db.getAbsolutePath)
  }

  def open(absolutePath: String): SaveDatabase = {
    new SaveDatabase(dbUrlBase + absolutePath)
  }
}

class SaveDatabase(dbUrl: String) {
  def connection: Connection = {
    val conn = DriverManager.getConnection(dbUrl)

    conn.setAutoCommit(false)

    conn
  }
}
