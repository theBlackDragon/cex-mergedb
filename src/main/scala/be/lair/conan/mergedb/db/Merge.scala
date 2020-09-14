package be.lair.conan.mergedb.db

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.sql.{Connection, PreparedStatement, SQLException}

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

    // these appear to be interactables in the world (eg. the forges in dungeons)
    ,"static_buildables"
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

        try {
          // remap conflicts
          // need to do this here since we'll be increasing the max(objectid) in the target database
          resolveCharacterConflicts(childConnection, outputConnection)

          // remap non-conflicts
          logger.info("Remapping mod controller IDs")
          mergeModControllers(childConnection, outputConnection)

          logger.info("Remapping static_buildables")
          mergeStaticBuildables(childConnection, outputConnection)

          logger.info("Remapping object IDs")
          mergeActorPosition(childConnection, outputConnection)

          // copy data
          logger.info("Copying tables")
          tablesToCopy.foreach(tableName => copyTable(tableName, childConnection, outputConnection))

          // verify result
          logger.info("Running sanity checks on merged database...")
          if (sanityCheck(outputConnection)) {
            logger.info("Sanity check passed.")
          } else {
            logger.error("Sanity check failed!")
            System.exit(1)
          }
        } catch {
          case e: SQLException => logger.error(e.getMessage, e)
        } finally {
          childConnection.close()
          outputConnection.close()

          if(!tmpChildDb.delete()) {
            logger.warn(s"Unable to remove temporary database at ${tmpChildDb.getAbsolutePath}")
          }
        }
    }
  }

  /** Resolve character objectId conflicts between databases
    *
    * Remapping character objectIds results in loss of ownership of Thralls.
    * It seems Thrall ownership is stored within the binary blob holding their properties.
    * Instead of trying to work out how to rewrite those we simply avoid remapping character objectIds in the source,
    * as was initially done, instead preferring to remap conflicting IDs in the target database.
    *
    * This does not handle the case where characters in both databases have the same ID.
    * These characters are simply ignored during the merge, preferring the target characters.
    * TODO remap these conflicting characters but show a warning about what is going on so the user is aware.
    */
  private def resolveCharacterConflicts(from: Connection, to: Connection): (List[Int], List[Int]) = {
    // select character IDs from source database
    val fromCharacterIds = listCharacterIds(from)
    logger.debug(s"fromCharacterIds: $fromCharacterIds")

    // see if they conflict with character IDs in target
    val toCharacterIds = listCharacterIds(to)
    logger.debug(s"toCharacterIds: $toCharacterIds")
    val conflictingCharacterIds = fromCharacterIds.intersect(toCharacterIds)
    logger.info(s"Character IDs present in both databases: ${conflictingCharacterIds.mkString(",")}")

    if(conflictingCharacterIds.nonEmpty) {
      logger.error("Character object_id conflicts detected, remapping character IDs will result in remapped " +
        "characters losing access to their Thralls.")
      // remove the conflicts from the source
      removeConflictingCharacters(to, conflictingCharacterIds)
    }


    // see if they conflict with any other IDs in target
    // make sure we don't accidentally pull in duplicate characters here, hence the diff
    val conflictingObjectIds = findConflictingObjectIds(to, fromCharacterIds).diff(conflictingCharacterIds)
    logger.info(s"Character IDs from source clashing with object_ids in target database: ${conflictingObjectIds.mkString(",")}")
    mergeConflictIds(to, conflictingObjectIds)

    (conflictingCharacterIds, conflictingObjectIds)
  }

  private val characterRemovalStatements: List[String] = List(
    "delete from actor_position where id = ?",

    "delete from buildings where owner_id = ?",

    "delete from characters where id = ?",
    "delete from character_stats where char_id = ?",

    "update follower_markers set owner_id = ? where owner_id = ?",

    "delete from guilds where owner = ?",
    //"update game_events set objectId = ? where objectId = ?",
    "delete from game_events where causerId = ?",
    "delete from game_events where ownerId = ?",

    "delete from item_inventory where owner_id = ?",
    "delete from item_properties where owner_id = ?",

    "delete from properties where object_id = ?",
    "delete from purgescores where purgeid = ?"
  )

  /** Attempts to remove all references to the characters passed in
    *
    * @param connection connection to the database to operate on
    * @param charIds the character IDs to remove from the database
    */
  private def removeConflictingCharacters(connection: Connection, charIds: List[Int]): Unit = {
    logger.info(s"Removing conflicting characters $charIds from the source database...")
    val deleteStatements = characterRemovalStatements.map(connection.prepareStatement)

    for{charId <- charIds
        statement <- deleteStatements} {
      logger.debug(s"Removing character $charId...")
      statement.setInt(1, charId)
      statement.addBatch()
    }

    logger.debug("Finalizing removal process...")
    deleteStatements.foreach(_.executeBatch())
    connection.commit()
  }

  /** Return a list of IDs that conflict with the passed in IDs
    *
    * @param connection connection to look for conflicts on
    * @param ids list of IDs we want to check duplicates for
    */
  private def findConflictingObjectIds(connection: Connection, ids: List[Int]): List[Int] = {
    val statement = connection.prepareStatement(
      s"select id from actor_position where id in (${List.fill(ids.length)("?").mkString(",")})")
    for(i <- ids.indices) {
      statement.setInt(i+1, ids(i))
    }
    val resultSet = statement.executeQuery()

    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).map(resultTuple =>
      resultTuple._1.getInt(1)).toList
  }

  /** Return a list of all character object_ids
    *
    * @param connection the connection to list the characters from
    * @return a list of character object_ids as Int values
    */
  private def listCharacterIds(connection: Connection): List[Int] = {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select id from characters")

    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).map(resultTuple =>
      resultTuple._1.getInt(1)).toList
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

  private val objectIdUpdateStatements: List[String] = List(
    "update actor_position set id = ? where id = ?",

    "update building_instances set object_id = ? where object_id = ?",
    "update buildable_health set object_id = ? where object_id = ?",
    "update buildings set object_id = ? where object_id = ?",
    "update buildings set owner_id = ? where owner_id = ?",

    "update characters set id = ? where id = ?",
    "update character_stats set char_id = ?  where char_id = ?",

    "update follower_markers set owner_id = ?  where owner_id = ?",

    "update guilds set owner = ?  where owner = ?",
    "update game_events set objectId = ? where objectId = ?",
    "update game_events set causerId = ? where causerId = ?",
    "update game_events set ownerId = ? where ownerId = ?",

    "update item_inventory set owner_id = ? where owner_id = ?",
    "update item_properties set owner_id = ? where owner_id = ?",
    "update mod_controllers set id = ? where id = ?",
    "update properties set object_id = ? where object_id = ?",
    "update purgescores set purgeid = ? where purgeid = ?",

    "update static_buildables set id = ? where id = ?"
  )

  /** Remap actor reference IDs that are not mod controllers
    *
    * Need to avoid duplicate IDs pointing to different objects, so remapping everything to IDs past the largest found
    * in our target database is the safest bet.
    *
    * T.e object_id values in other columns refer to the id column in actor_position, so should be updated as well.
    */
  private def mergeActorPosition(from: Connection, to: Connection): Unit ={
    val maxId = {
      List(getMaxId(to),getMaxId(from)).max
    }
    // Using the largest object_id from the databases involved to avoid id duplication
    logger.debug(s"maxId $maxId based on largest table")

    val statement = from.createStatement()
    val resultSet = statement.executeQuery("select id from actor_position "
      + "where id not in (select id from static_buildables)" // ignore static_buildables
      + "and id not in (select id from characters)") // avoid updating character IDs

    val updateStatements = objectIdUpdateStatements.map(from.prepareStatement)

    var batchCounter = 0
    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).foreach { rs =>
      val oldId = rs._1.getInt(1)
      val newId = oldId + maxId
      logger.debug(s"Remapping objectId $oldId to $newId")

      updateObjectId(updateStatements, oldId, newId)

      batchCounter+=1
    }
    updateStatements.foreach(_.executeBatch())

    from.commit()
    logger.info(s"Updated $batchCounter object_ids")
  }

  /** Remap the specified object_ids to avoid duplicates
    *
    * Reassign new objectIds past the end of the max(objectId) in the connection.
    */
  private def mergeConflictIds(to: Connection, conflictIds: List[Int]): Unit ={
    logger.debug(s"Remapping conflictIds $conflictIds")
    val maxId = getMaxId(to)
    // Using the largest object_id from the databases involved to avoid id duplication
    logger.debug(s"maxId $maxId based on target table")

    val updateStatements = objectIdUpdateStatements.map(to.prepareStatement)

    var batchCounter = 0
    conflictIds.foreach{ oldId =>
      batchCounter+=1

      val newId = maxId + batchCounter
      logger.debug(s"Remapping objectId $oldId to $newId")

      updateObjectId(updateStatements, oldId, newId)
    }
    updateStatements.foreach(_.executeBatch())

    to.commit()
    logger.info(s"Updated $batchCounter conflicting object_ids")
  }

  private def updateObjectId(updateStatements: List[PreparedStatement], oldId: Int, newId: Int): Unit = {
    updateStatements.foreach{statement =>
      statement.setInt(1, newId)
      statement.setInt(2, oldId)
      statement.addBatch()
    }
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

    fromControllers.foreach(fromTuple => toControllers.get(fromTuple._1) match {
      case Some(targetId) =>
        logger.trace(s"${fromTuple._1} exists in target db, source id: ${fromTuple._2}, target id: $targetId")
        updateControllerId(from, fromTuple._2, targetId)
      case None =>
        logger.trace(s"${fromTuple._1} not present in target db, copying unchanged.")
    })
  }

  private def updateControllerId(connection: Connection, oldId: String, targetId: String): Unit = {
    logger.debug(s"Updating controller with ID $oldId to new ID $targetId")

    // Remove the mod controllers we already have in the target from actor_position so they are not copied later in
    // the process.
    val modConStatement = connection.prepareStatement("delete from mod_controllers where id = ?")
    modConStatement.setString(1, oldId)
    modConStatement.executeUpdate()

    val apStatement = connection.prepareStatement("delete from actor_position where id = ?")
    apStatement.setString(1, oldId)
    apStatement.executeUpdate()

    // we repoint the existing records to the controller ID we found in the target database
    val pStatement = connection.prepareStatement("update properties set object_id = ? where object_id = ?")
    pStatement.setString(1, targetId)
    pStatement.setString(2, oldId)
    pStatement.executeUpdate()

    // TODO check if mod controller references pop up in any other tables

    connection.commit()
  }

  /** List all the mod controllers present in a game db
    *
    * @return a Map containing the controller class and id, respectively
    */
  private def listControllers(connection: Connection): Map[String, String] = {
    val statement = connection.prepareStatement("select ap.class, ap.id from actor_position ap join mod_controllers mc on ap.id = mc.id")
    val resultSet = statement.executeQuery()

    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).map(tuple =>
      tuple._1.getString(1) -> tuple._1.getString(2)).toMap
  }

  private def mergeStaticBuildables(from: Connection, to: Connection): Unit = {
    val fromBuildables = listStaticBuildables(from)
    val toBuildables = listStaticBuildables(to)

    fromBuildables.foreach(fromTuple => toBuildables.get(fromTuple._1) match {
      case Some(targetId) =>
          updateStaticBuildableId(from, fromTuple._1, fromTuple._2, targetId)
      case None =>
        logger.trace(s"${fromTuple._1} not present in target db, copying unchanged.")
    })
  }

  private def updateStaticBuildableId(connection: Connection,
                                      sbClass: String,
                                      oldId: String,
                                      targetId: String): Unit = {
    logger.debug(s"Updating static_buildable with ID $oldId to new ID $targetId")

    // Remove the static_buildables we already have in the target from actor_position so they are not copied later in
    // the process.
    val modConStatement = connection.prepareStatement("delete from static_buildables where id = ?")
    modConStatement.setString(1, oldId)
    modConStatement.executeUpdate()

    val apStatement = connection.prepareStatement("delete from actor_position where id = ? and class = ?")
    apStatement.setString(1, oldId)
    apStatement.setString(2, sbClass)
    apStatement.executeUpdate()

    // we repoint the existing records to the controller ID we found in the target database
    val pStatement = connection.prepareStatement("update properties set object_id = ? where object_id = ? ")
    pStatement.setString(1, targetId)
    pStatement.setString(2, oldId)
    pStatement.executeUpdate()

    connection.commit()
  }

  private def listStaticBuildables(connection: Connection): Map[String, String] = {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select name, id from static_buildables")
    Iterator.continually((resultSet, resultSet.next())).takeWhile(_._2).map(tuple =>
      tuple._1.getString(1) -> tuple._1.getString(2)).toMap
  }

  /** Perform a database sanity check
    *
    * @return true if sane, false otherwise
    */
  private def sanityCheck(connection: Connection): Boolean = {
    // check for duplicate entries
    val validationQueries = List(
      // check for duplicate object_ids
      "select class from actor_position group by id having count(*) > 1",
      "select object_id from building_instances group by object_id, instance_id having count(*) > 1",
      "select object_id from buildings group by object_id having count(*) > 1",

      // Check for IDs that can't be mapped back to actor_position
      "select * from buildable_health bh " +
        "left outer join actor_position ap " +
        "on bh.object_id = ap.id " +
        "where ap.id is null",
      "select * from building_instances bi " +
        "left outer join actor_position ap " +
        "on bi.object_id = ap.id " +
        " where ap.id is null",
      "select * from buildings b " +
        "left outer join actor_position ap " +
        "on b.object_id = ap.id " +
        "where ap.id is null",
      //if owner is 0 we're probably dealing with a record from static_buildables
      "select * from (select * from buildings b " +
        "left outer join actor_position ap " +
        "on b.owner_id = ap.id " +
        "where ap.id is null) ids " +
        "join static_buildables sb " +
        "on ids.object_id = sb.id " +
        "where ids.id is not null",
      // todo validate?
//      "select * from character_stats cs " +
//        "left outer join actor_position ap " +
//        "on cs.char_id = ap.id " +
//        "where ap.id is null",
      "select * from characters c " +
        "left outer join actor_position ap " +
        "on c.id = ap.id " +
        "where ap.id is null",
      "select * from follower_markers fm " +
        "left outer join actor_position ap " +
        "on fm.owner_id = ap.id " +
        "where ap.id is null",
      "select * from follower_markers fm " +
        "left outer join actor_position ap " +
        "on fm.follower_id = ap.id " +
        "where ap.id is null",
      "select * from game_events ge " +
        "left outer join actor_position ap " +
        "on (ge.ownerId = ap.id or ge.ownerId == 0) " +
        "where ap.id is null",
      "select * from game_events ge " +
        "left outer join actor_position ap " +
        "on (ge.causerId = ap.id or ge.causerId == 0) " +
        "where ap.id is null",
      "select * from guilds g " +
        "left outer join actor_position ap " +
        "on g.owner = ap.id " +
        "where ap.id is null",
      // non-existing owner_id, lootbag?
//      "select * from item_inventory ii " +
//        "left outer join actor_position ap " +
//        "on ii.owner_id = ap.id " +
//        "where ap.id is null",
//      "select * from item_properties ip " +
//        "left outer join actor_position ap " +
//        "on ip.owner_id = ap.id " +
//        "where ap.id is null",
      "select * from mod_controllers mc " +
        "left outer join actor_position ap " +
        "on mc.id = ap.id " +
        "where ap.id is null",
      "select * from properties p " +
        "left outer join actor_position ap " +
        "on p.object_id = ap.id " +
        "where ap.id is null",
      "select * from purgescores ps " +
        "left outer join actor_position ap " +
        "on ps.purgeid = ap.id " +
        "where ap.id is null",

      // check whether all Thralls have their inventories
      "select * from item_inventory ii " +
        "left outer join actor_position ap " +
        "on ap.id = ii.owner_id " +
        "where ap.class like '/Game/Characters/NPCs/%' " +
        "and ap.id is null"
    )
    validationQueries.forall(isValid(connection, _))
  }

  /** Run a validation query
    *
    * The result is considered valid of the query doesn't return any records.
    *
    * @param connection the connection to run the query on
    * @param query the query to run
    * @return true if valid, false if invalid
    */
  private def isValid(connection: Connection, query: String): Boolean = {
    logger.trace(s"Running query $query")
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)

    if(resultSet.next()) {
      logger.error(s"Issues exist for validation query: $query")
      false
    } else {
      true
    }
  }
}
