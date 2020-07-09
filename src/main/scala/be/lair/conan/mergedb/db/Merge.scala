package be.lair.conan.mergedb.db

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.sql.Connection

import grizzled.slf4j.Logging

import scala.collection.immutable.TreeMap

object Merge extends Logging {

  private val tablesToCopy = Vector(
    //"account"

    "actor_position"

    ,"buildable_health"
    ,"building_instances"
    ,"buildings" // maps buildings to owners, if we ever want to change owners, this would be where to do it

    ,"character_stats"
    ,"characters"

    // metadata about game/database versions, probably a bad idea to mess with this
    //,"dw_settings"

    ,"follower_markers"

    ,"game_events"

    ,"guilds"

    ,"item_inventory"
    ,"item_properties"

    ,"mod_controllers"

    ,"properties"

    ,"purgescores"

    // TODO figure out what these are for, they're not always empty, so they are actually used for...something.
    //,"static_buildables"
  )

  def merge(masterDatabase: File,
            childDatabase: File,
            outputDatabase: File): Unit = {
    createOutputDatabase(masterDatabase, outputDatabase) match {
      case None => logger.error("Output database exists, refusing to overwrite.")
      case Some(_) =>
        // Create temporary database for our child database, so we can safely remap identifiers
        val tmpChildDb = File.createTempFile("cex-mergedb", "childDb.db")
        tmpChildDb.deleteOnExit()
        Files.copy(childDatabase.toPath, new FileOutputStream(tmpChildDb))

        val outputConnection = SaveDatabase.open(outputDatabase).connection
        val childConnection = SaveDatabase.open(tmpChildDb).connection

        logger.info("Remapping object IDs")
        mergeActorPosition(childConnection, outputConnection)

        logger.info("Remapping mod controller IDs")
        mergeModControllers(childConnection, outputConnection)

        logger.info("Copying tables")
        tablesToCopy.foreach(tableName => copyTable(tableName, childConnection, outputConnection))

        childConnection.close()
        outputConnection.close()
    }
  }

  /**
    * Copy the master database to the output location to use as basis of the merge operation
    *
    * @param masterDatabase the database file to server as master
    * @param outputDatabase a File object pointing to the desired output location of the merged database
    * @return the File object pointing to the outputDatabse, or None if the copy operation failed
    */
  private def createOutputDatabase(masterDatabase: File, outputDatabase: File): Option[File] = {
    if (outputDatabase.exists()) {
      None
    } else {
      val outputDbStream = new FileOutputStream(outputDatabase)
      Files.copy(masterDatabase.toPath, outputDbStream)

      Some(outputDatabase)
    }
  }

  /** Blindly copy the data from the table in the "from" connection to the same table in the "to" connection.
    *
    * Any adjustments that need to be made to the data being copied should be made prior to running this method.
    */
  private def copyTable(tableName: String, from: Connection, to: Connection): Unit = {
    val statement = from.createStatement()
    val resultSet = statement.executeQuery("select * from " + tableName)

    val columnCount = resultSet.getMetaData.getColumnCount
    val columnMapping: TreeMap[Int, String] = TreeMap.from(
      (1 to columnCount).map(columnIndex =>
        columnIndex -> resultSet.getMetaData.getColumnName(columnIndex)))
    logger.trace(s"columnMapping for $tableName: $columnMapping")


    val insertSql = "insert into " + tableName + s" (${columnMapping.values.mkString(",")}) values (${List.fill(columnCount)("?").mkString(",")})"
    logger.trace(s"INSERT statement: $insertSql")
    val insertStatement = to.prepareStatement(insertSql)

    var batchCounter = 0
    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).foreach { rs =>
      columnMapping.foreach { cm =>
        // Game doesn't like it when the types don't match, just using object appears to dodge having to manually
        // manage types appears to work though.
        insertStatement.setObject(cm._1, rs._1.getObject(cm._2))
      }
      insertStatement.addBatch()

      batchCounter+=1
      if(batchCounter % 1000 == 0) {
        insertStatement.executeBatch()
        logger.debug(s"'$tableName' table: executed $batchCounter insert statements")
      }
    }
    logger.debug(s"'$tableName' table: executed $batchCounter insert statements")
    insertStatement.executeBatch()
    to.commit()
  }

  /** Remap actor reference IDs that are not mod controllers
    *
    * Need to avoid duplicate IDs pointing to different objects, so remapping everything to IDs past the largest found
    * in our target database is the safest bet.
    *
    * T.e object_id values in other columns refer to the id column in actor_position, so should be updated as well.
    */
  private def mergeActorPosition(from: Connection, to: Connection): Unit ={
    val maxId = getMaxId(to)
    logger.debug(s"Target DB maxId $maxId")

    val statement = from.createStatement()
    val resultSet = statement.executeQuery("select id from actor_position " +
      "where id not in (select * from mod_controllers)")

    val apStatement = from.prepareStatement("update actor_position set id = ? where id = ?")

    val biStatement = from.prepareStatement("update building_instances set object_id = ? where object_id = ?")
    val bhStatement = from.prepareStatement("update buildable_health set object_id = ? where object_id = ?")
    val bStatement = from.prepareStatement("update buildings set object_id = ? where object_id = ?")
    val bOwnerStatement = from.prepareStatement("update buildings set owner_id = ? where owner_id = ?")

    val cStatement = from.prepareStatement("update characters set id = ? where id = ?")
    val csStatement = from.prepareStatement("update character_stats set char_id = ?  where char_id = ?")

    val fmStatement = from.prepareStatement("update follower_markers set owner_id = ?  where owner_id = ?")

    val gStatement = from.prepareStatement("update guilds set owner = ?  where owner = ?")
    val geStatement = from.prepareStatement("update game_events set objectId = ? where objectId = ?")
    val geOwnerStatement = from.prepareStatement("update game_events set ownerId = ? where ownerId = ?")

    val iiStatement = from.prepareStatement("update item_inventory set owner_id = ? where owner_id = ?")
    val ipStatement = from.prepareStatement("update item_properties set owner_id = ? where owner_id = ?")

    val pStatement = from.prepareStatement("update properties set object_id = ? where object_id = ?")
    val purgeScoresStatement = from.prepareStatement("update purgescores set purgeid = ? where purgeid = ?")

    var batchCounter = 0
    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).foreach { rs =>
      val oldId = rs._1.getInt(1)
      val newId = oldId + maxId
      logger.debug(s"Remapping objectId $oldId to $newId")

      // actor_position
      apStatement.setInt(1, newId)
      apStatement.setInt(2, oldId)
      apStatement.addBatch()

      // building_instances
      biStatement.setInt(1, newId)
      biStatement.setInt(2, oldId)
      biStatement.addBatch()

      // buildable_health
      bhStatement.setInt(1, newId)
      bhStatement.setInt(2, oldId)
      bhStatement.addBatch()

      // buildings
      bStatement.setInt(1, newId)
      bStatement.setInt(2, oldId)
      bStatement.addBatch()

      // buildings (owner_id)
      bOwnerStatement.setInt(1, newId)
      bOwnerStatement.setInt(2, oldId)
      bOwnerStatement.addBatch()

      // update characters table
      cStatement.setInt(1, newId)
      cStatement.setInt(2, oldId)
      cStatement.addBatch()

      // character_stats
      csStatement.setInt(1, newId)
      csStatement.setInt(2, oldId)
      csStatement.addBatch()

      // follower_markers (owner_id)
      fmStatement.setInt(1, newId)
      fmStatement.setInt(2, oldId)
      fmStatement.addBatch()

      // game_events
      geStatement.setInt(1, newId)
      geStatement.setInt(2, oldId)
      geStatement.addBatch()

      // game_events (ownerId)
      geOwnerStatement.setInt(1, newId)
      geOwnerStatement.setInt(2, oldId)
      geOwnerStatement.addBatch()

      // guilds (owner)
      gStatement.setInt(1, newId)
      gStatement.setInt(2, oldId)
      gStatement.addBatch()

      // item_inventory (owner_id)
      iiStatement.setInt(1, newId)
      iiStatement.setInt(2, oldId)
      iiStatement.addBatch()

      // item_properties (owner_id)
      ipStatement.setInt(1, newId)
      ipStatement.setInt(2, oldId)
      ipStatement.addBatch()

      // purgescores (id matches character id)
      purgeScoresStatement.setInt(1, newId)
      purgeScoresStatement.setInt(2, oldId)
      purgeScoresStatement.addBatch()

      // properties
      pStatement.setInt(1, newId)
      pStatement.setInt(2, oldId)
      pStatement.addBatch()

      batchCounter+=1
      if(batchCounter % 1000 == 0) {
        apStatement.executeBatch()

        biStatement.executeBatch()
        bhStatement.executeBatch()
        bStatement.executeBatch()
        bOwnerStatement.executeBatch()

        cStatement.executeBatch()
        csStatement.executeBatch()

        fmStatement.executeBatch()

        gStatement.executeBatch()
        geStatement.executeBatch()
        geOwnerStatement.executeBatch()

        pStatement.executeBatch()
        purgeScoresStatement.executeBatch()

        iiStatement.executeBatch()
        ipStatement.executeBatch()

        logger.debug(s"Inserted $batchCounter updated object_ids")
      }
    }

    apStatement.executeBatch()
    biStatement.executeBatch()
    bhStatement.executeBatch()
    bStatement.executeBatch()
    bOwnerStatement.executeBatch()
    cStatement.executeBatch()
    csStatement.executeBatch()
    fmStatement.executeBatch()
    gStatement.executeBatch()
    geStatement.executeBatch()
    geOwnerStatement.executeBatch()

    pStatement.executeBatch()
    purgeScoresStatement.executeBatch()

    iiStatement.executeBatch()
    ipStatement.executeBatch()

    from.commit()
    logger.info(s"Updated $batchCounter object_ids")
  }

  private def getMaxId(connection: Connection): Int = {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select max(id) from actor_position")
    resultSet.next()
    resultSet.getInt(1)
  }

  /** Renumber the mod controllers and update references
    *
    * ModControllers point to the installed DLC and mods, the numbering on these appears to be on a first-come
    * first-serve basis, meaning they'll likely be numbered differently between savegames.
    */
  private def mergeModControllers(from: Connection, to: Connection): Unit = {
    val fromControllers = listControllers(from)
    val toControllers = listControllers(to)

    logger.trace(s"fromControllers: $fromControllers")
    logger.trace(s"toControllers: $toControllers")

    fromControllers.foreach(tuple => toControllers.get(tuple._1) match {
      case Some(targetId) =>
        logger.trace(s"${tuple._1} exists in target db, source id: ${tuple._2}, target id: $targetId")
        updateControllerId(from, tuple._2, targetId)
      case None =>
        logger.trace(s"${tuple._1} not present in target db, copying unchanged.")
    })
  }

  private def updateControllerId(connection: Connection, oldId: String, newId: String): Unit = {
    logger.debug(s"Updating controller with ID $oldId to new ID $newId")

    val modConStatement = connection.prepareStatement("delete from mod_controllers where id = ?")
    modConStatement.setString(1, oldId)
    modConStatement.executeUpdate()

    val apStatement = connection.prepareStatement("update actor_position set id = ? where id = ?")
    apStatement.setString(1, newId)
    apStatement.setString(2, oldId)
    apStatement.executeUpdate()

    connection.commit()
  }

  private def listControllers(connection: Connection): Map[String, String] = {
    val statement = connection.prepareStatement("select ap.class, ap.id from actor_position ap join mod_controllers mc on ap.id = mc.id")
    val resultSet = statement.executeQuery()

    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).map(tuple =>
      tuple._1.getString(1) -> tuple._1.getString(2)).toMap
  }


}
