package org.sunbird.content.util

import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder}
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{LocalDate, Session}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import org.apache.commons.lang.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.cache.impl.RedisCache
import org.sunbird.cassandra.CassandraConnector
import org.sunbird.common.Platform
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ErrorCodes, ResponseCode, ServerException}
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.external.store.ExternalStore
import org.sunbird.graph.nodes.DataNode
import org.sunbird.kafka.client.KafkaClient
import org.sunbird.managers.HierarchyManager
import org.sunbird.telemetry.logger.TelemetryManager
import org.sunbird.util.RequestUtil

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}
import java.util
import java.util.{Date, UUID}
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.concurrent.{ExecutionContext, Future, Promise}
import com.datastax.driver.core.{LocalDate => CassandraLocalDate}

object ExtendedRetireManager {
  val finalStatus: util.List[String] = util.Arrays.asList("Flagged", "Live", "Unlisted")
  private val kfClient = new KafkaClient
  private val retirementRequestKeyspace: String =
    Platform.getString(ContentConstants.SUNBIRD_COURSE_KEYSPACE, "sunbird_courses")

  private val retirementRequestTable: String =
    Platform.getString(ContentConstants.CONTENT_RETIREMENT_RQST_TABLE, "content_retirement_requests")

  private val retirementRequestStore =
    new ExternalStore(
      retirementRequestKeyspace,
      retirementRequestTable,
      util.Arrays.asList(ContentConstants.RETITEMENT_PRIMARY_KEY)
    )

  private val logger: Logger = LoggerFactory.getLogger("ExtendedRetireManager")

  def scheduleRetirement(request: Request)
                        (implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Response] = {
    System.out.println("Inside scheduleRetirement method of ExtendedRetireManager::")
    validateRequestForContentRetirement(request)
    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get(ContentConstants.RQST))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Request body is missing."
      ))
    reqMap.put(ContentConstants.USER_ID_RAISED, extractUserId(request))
    val contentId = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      ))
    for {
      _ <- validateNoParentCollection(request)
      _ <- validateNoCbPlanForContent(contentId)
      _ <- validateMultilingualRetirement(contentId)
      _ <- insertRetirementRequest(contentId, reqMap)
      resp <- markContentPendingRetirement(request)
    } yield resp
  }

  private def validateRequestForContentRetirement(request: Request): Unit = {
    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get(ContentConstants.RQST))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Request body is missing."
      ))

    val contentId = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      ))

    val reason = Option(reqMap.get(ContentConstants.REASON))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REASON,
        ContentConstants.ERR_MISSING_REASON
      ))

    val lastEnrollmentDateStr = Option(reqMap.get(ContentConstants.LAST_ENROLLMENT_DATE))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_LAST_ENROLLMENT_DATE,
        ContentConstants.ERR_MISSING_LAST_ENROLLMENT_DATE
      ))

    val retirementDateStr = Option(reqMap.get(ContentConstants.RETIREMENT_DATE))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_RETIREMENT_DATE,
        ContentConstants.MISSING_RETIREMENT_DATE
      ))
    val lastEnrollmentInstant =
      try {
        Instant.parse(lastEnrollmentDateStr)
      } catch {
        case _: DateTimeParseException =>
          throw new ClientException(
            ContentConstants.ERR_LAST_ENROLLMENT_DATE,
            s"Invalid lastEnrollmentDate format: $lastEnrollmentDateStr"
          )
      }

    val retirementInstant =
      try {
        Instant.parse(retirementDateStr)
      } catch {
        case _: DateTimeParseException =>
          throw new ClientException(
            ContentConstants.ERR_INVALID_RETIREMENT_DATE,
            s"Invalid retirementDate format: $retirementDateStr"
          )
      }
    val lastEnrollmentDate = lastEnrollmentInstant.atZone(ZoneOffset.UTC).toLocalDate
    val retirementDate = retirementInstant.atZone(ZoneOffset.UTC).toLocalDate
    val daysBetween = ChronoUnit.DAYS.between(lastEnrollmentDate, retirementDate)
    val minGapDays =
      Option(Platform.getString(ContentConstants.MIN_RETIREMENT_GAP_DAYS, "1"))
        .map(_.toInt)
        .getOrElse(1)
    val maxGapDays =
      Option(Platform.getString(ContentConstants.MAX_RETIREMENT_GAP_DAYS, "60"))
        .map(_.toInt)
        .getOrElse(60)
    if (daysBetween < minGapDays || daysBetween > maxGapDays) {
      throw new ClientException(
        ContentConstants.ERR_INVALID_DATE_ORDER,
        s"Retirement date must be between $minGapDays and $maxGapDays days after lastEnrollmentDate."
      )
    }
  }

  private def extractUserId(req: Request): String = {
    val fromContext = Option(req.getContext.get("X-Authenticated-Userid"))
      .map(_.toString)
      .filter(StringUtils.isNotBlank)
    if (fromContext.isDefined) return fromContext.get
    val params = req.getParams
    if (params != null && params.getUid != null)
      return params.getUid
    ""
  }

  private def validateNoParentCollection(request: Request)
                                        (implicit ec: ExecutionContext,
                                         oec: OntologyEngineContext): Future[Unit] = {

    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get("request"))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Request body is missing."
      ))

    val contentId = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      ))

    val readReq = new Request()
    readReq.setContext(new util.HashMap[String, AnyRef]() {
      {
        put("graph_id", "domain")
        put("version", "1.0")
        put("objectType", "Content")
        put("schemaName", "content")
      }
    })
    readReq.put("identifier", contentId)
    readReq.setObjectType("Content")
    readReq.put(ContentConstants.MODE, "read")

    DataNode.read(readReq).map { node =>
      if (node == null) {
        throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT_ID,
          "Content is not found."
        )
      }

      val metadata = node.getMetadata
      metadata.get("parentCollections") match {
        case list: java.util.Collection[_] if !list.isEmpty =>
          throw new ClientException(
            ContentConstants.ERR_CONTENT_PART_OF_COLLECTION,
            ContentConstants.ERR_CONTENT_PART_OF_COLLECTION_MSG
          )
        case _ =>
          ()
      }
    }
  }

  private def validateNoCbPlanForContent(contentId: String)
                                        (implicit ec: ExecutionContext): Future[Unit] = {

    // no columns needed just to check existence
    val externalProperties: List[String] = Nil
    val propertyTypeMapping: Map[String, String] = Map.empty

    read(contentId, externalProperties, propertyTypeMapping).map { resp =>
      val code = resp.getResponseCode // assuming Response has this
      code match {
        case ResponseCode.OK =>
          throw new ClientException(
            ContentConstants.ERR_CONTENT_HAS_ACTIVE_PLAN,
            "Content has cbPlan mappings and cannot be retired."
          )
        case ResponseCode.RESOURCE_NOT_FOUND =>
          ()
        case other =>
          throw new ServerException(
            ErrorCodes.ERR_SYSTEM_EXCEPTION.name,
            s"Error while checking cbPlan lookup. Response code: $other"
          )
      }
    }
  }

  private def validateMultilingualRetirement(contentId: String)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Unit] = {
    val readReq = new Request()
    readReq.setContext(new java.util.HashMap[String, AnyRef]() {
      {
        put("graph_id", "domain")
        put("version", ContentConstants.SCHEMA_VERSION)
        put("objectType", ContentConstants.CONTENT_OBJECT_TYPE)
        put("schemaName", ContentConstants.CONTENT_SCHEMA_NAME)
      }
    })
    readReq.put(ContentConstants.IDENTIFIER, contentId)
    readReq.put(ContentConstants.MODE, "read")

    DataNode.read(readReq).map { node =>
      val metadata = Option(node.getMetadata)
        .getOrElse(throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT,
          "Content metadata is missing."
        ))

      val langMapOpt = Option(metadata.get(ContentConstants.LANGUAGE_MAP_V1))
        .map(_.asInstanceOf[java.util.Map[String, java.util.Map[String, AnyRef]]])

      if (langMapOpt.isDefined) {
        val langMap = langMapOpt.get
        val entries = langMap.values().asScala.toList
        val retiringEntryOpt = entries.find(e => Option(e.get("id")).map(_.toString) == Some(contentId))
        retiringEntryOpt match {
          case Some(retiringEntry) =>
            val isBase = Option(retiringEntry.get(ContentConstants.IS_BASE_LANG)).exists(_.toString.toBoolean)
            if (isBase && entries.size > 1) {
              // Check if all non-base language versions are retired
              val nonBaseEntries = entries.filterNot(_ == retiringEntry)
              val allNonBaseRetired = nonBaseEntries.forall(e =>
                Option(e.get(ContentConstants.STATUS)).exists(_.toString.equalsIgnoreCase(ContentConstants.RETIRED))
              )
              if (!allNonBaseRetired) {
                throw new ClientException(
                  ContentConstants.ERR_ACTIVE_MULTILINGUAL_COURSE,
                  ContentConstants.ERR_ACTIVE_MULTILINGUAL_COURSE_MSG
                )
              }
            } else if (!isBase) {
              logger.info(s"Retiring non-base language content: $contentId")
            }
          case None =>
            logger.warn(s"Content $contentId not found in language map during multilingual validation, allowing retirement")
            ()
        }
      }
    }
  }

  private def insertRetirementRequest(contentId: String, reqMap: java.util.Map[String, AnyRef])
                                     (implicit ec: ExecutionContext): Future[Response] = {
    val rowMap = createRetirementRequestRow(contentId, reqMap)
    val propsMapping: Map[String, String] = Map.empty
    val auditMap = new util.HashMap[String, AnyRef](rowMap)
    retirementRequestStore.insert(rowMap, propsMapping)
    createRetirementAuditLog(auditMap)
  }

  private def createRetirementRequestRow(contentId: String,
                                         reqMap: java.util.Map[String, AnyRef]
                                        ): util.Map[String, AnyRef] = {

    val retirementMap = new util.HashMap[String, AnyRef]()

    // ExternalStore.insert uses "identifier" to set primaryKey(0) -> content_id
    retirementMap.put(ContentConstants.IDENTIFIER, contentId)

    // Table columns
    retirementMap.put(ContentConstants.RQST_ID, UUID.randomUUID().toString)

    // Reason & user
    val reason = Option(reqMap.get(ContentConstants.REASON)).map(_.toString).getOrElse("")
    val userIdRaised = Option(reqMap.get(ContentConstants.USER_ID_RAISED)).map(_.toString).getOrElse("")
    retirementMap.put(ContentConstants.RSN_FOR_RETIREMENT, reason)
    retirementMap.put(ContentConstants.USER_ID_RAISED_FIELD, userIdRaised)

    // Parse dates from ISO string to Cassandra `date`
    val lastEnrollmentStr = Option(reqMap.get(ContentConstants.LAST_ENROLLMENT_DATE)).map(_.toString)
    val retirementStr = Option(reqMap.get(ContentConstants.RETIREMENT_DATE)).map(_.toString)

    val isoFormatter = DateTimeFormatter.ISO_INSTANT

    def toLocalDateOpt(sOpt: Option[String]): Option[LocalDate] =
      sOpt.map { s =>
        val instant = Instant.from(isoFormatter.parse(s))
        LocalDate.fromMillisSinceEpoch(instant.toEpochMilli)
      }

    toLocalDateOpt(lastEnrollmentStr).foreach(date => retirementMap.put(ContentConstants.LST_ENR_DATE, date))
    toLocalDateOpt(retirementStr).foreach(date => retirementMap.put(ContentConstants.RET_DATE, date))
    val now = new Date()
    val cassandraDate: CassandraLocalDate =
      CassandraLocalDate.fromMillisSinceEpoch(now.getTime)

    retirementMap.put(ContentConstants.CREATED_DATE, cassandraDate)

    retirementMap.put(ContentConstants.CREATED_AT, now)
    retirementMap.put(ContentConstants.UPDATED_AT, now)
    retirementMap.put(ContentConstants.STATUS, ContentConstants.PENDING)
    retirementMap.put(ContentConstants.APPROVED, Boolean.box(false))
    retirementMap.put(ContentConstants.CREATED_DATE, cassandraDate)
    retirementMap
  }

  def isRetirementScheduled(request: Request)
                           (implicit ec: ExecutionContext,
                            oec: OntologyEngineContext): Future[Response] = {
    logger.info("Inside isRetirementScheduled method of RetireManager::")

    val outerMap = request.getRequest
    val reqMap: java.util.Map[String, AnyRef] =
      Option(outerMap.get(ContentConstants.RQST))
        .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
        .getOrElse {
          val contentMap = new java.util.HashMap[String, AnyRef]()
          val id =
            Option(request.getContext.get(ContentConstants.IDENTIFIER))
              .orElse(Option(outerMap.get(ContentConstants.IDENTIFIER)))
              .map(_.toString.trim)
              .filter(StringUtils.isNotBlank)
              .getOrElse(throw new ClientException(
                ContentConstants.ERR_INVALID_CONTENT_ID,
                ContentConstants.ERR_CONTENT_ID_MISSING
              ))

          contentMap.put(ContentConstants.CONTENT_ID, id)
          outerMap.put(ContentConstants.RQST, contentMap)
          contentMap
        }

    val contentId = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      ))

    // Chain validations as before
    val validationsF: Future[Unit] = for {
      _ <- validateNoParentCollection(request)
      _ <- validateNoCbPlanForContent(contentId)
      _ <- validateMultilingualRetirement(contentId)
    } yield ()
    validationsF.map { _ =>
      val resp = ResponseHandler.OK()
      val params = resp.getParams
      params.setErr(null)
      params.setStatus(ContentConstants.SUCCESS)
      params.setErrmsg(null)
      resp.put(ContentConstants.IDENTIFIER, contentId)
      resp.put(ContentConstants.IS_VALID, java.lang.Boolean.TRUE)
      resp.put(ContentConstants.MESSAGES, null)
      resp
    }.recover {
      case clientException: ClientException =>
        val resp = ResponseHandler.OK()
        val params = resp.getParams
        params.setErr(clientException.getErrCode)
        params.setStatus(ContentConstants.FAILED)
        params.setErrmsg(clientException.getMessage)

        resp.put(ContentConstants.IDENTIFIER, contentId)
        resp.put(ContentConstants.IS_VALID, java.lang.Boolean.FALSE)
        resp.put(ContentConstants.MESSAGES, clientException.getMessage)
        resp

      case ex: Exception =>
        val resp = ResponseHandler.OK()
        val params = resp.getParams
        params.setErr(ErrorCodes.ERR_SYSTEM_EXCEPTION.name)
        params.setStatus("failed")
        params.setErrmsg(ex.getMessage)

        resp.put("identifier", contentId)
        resp.put("isValid", java.lang.Boolean.FALSE)
        resp.put("messages", ex.getMessage)
        resp
    }
  }

  def markContentPendingRetirement(request: Request)(implicit ec: ExecutionContext,
                                   oec: OntologyEngineContext): Future[Response] = {
    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get("request"))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Request body is missing."
      ))

    val id = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      ))

    val readReq = new Request()
    readReq.setContext(new util.HashMap[String, AnyRef]() {{
      put("graph_id", "domain")
      put("version", "1.0")
      put("objectType", "Content")
      put("schemaName", "content")
    }})
    readReq.put("identifier", id)
    readReq.setObjectType("Content")
    readReq.put(ContentConstants.MODE, "read")
    RedisCache.delete(id)
    DataNode.read(readReq).flatMap { node =>
      if (node == null)
        throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT_ID,
          s"Content is not found for identifier: $id"
        )
      val metadata = node.getMetadata
      val status   = Option(metadata.get("status")).map(_.toString).getOrElse("")
      if (StringUtils.isBlank(status))
        throw new ClientException(
          "ERR_METADATA_ISSUE",
          "Content metadata error, status is blank for identifier: " + node.getIdentifier
        )
      request.setContext(new java.util.HashMap[String, AnyRef]() {{
        put("graph_id", "domain")
        put("version", "1.0")
        put("objectType", "Collection")
        put("schemaName", "collection")
      }})
      request.getRequest.put(ContentConstants.CONTENT_RETIREMENT_STS, ContentConstants.PENDING_APPROVAL)
      request.getRequest.put("versionKey", metadata.get("versionKey"))
      RequestUtil.restrictProperties(request)
      request.getContext.put(ContentConstants.IDENTIFIER, id)
      val nodeList = new util.ArrayList[Node]()
      nodeList.add(node)
      val objectType =
        Option(request.getContext.get("objectType"))
          .map(_.asInstanceOf[String])
          .getOrElse("Content")
      val updateFut =
        if (objectType.toLowerCase.equals("collection"))
          DataNode.systemUpdate(request, nodeList, "content", Option(HierarchyManager.getHierarchy))
        else
          DataNode.systemUpdate(request, nodeList, "", None)
      updateFut.map { updatedNode =>
        val identifier: String = updatedNode.getIdentifier.replace(".img", "")
        logger.info("Marked content as PendingRetirement for identifier: " + identifier)
        ResponseHandler.OK
          .put("node_id", identifier)
          .put("identifier", identifier)
      }
    }
  }

  def createRetirementAuditLog(rowMap: java.util.Map[String, AnyRef])
                              (implicit ec: ExecutionContext): Future[Response] = {

    val contentId = rowMap.get(ContentConstants.IDENTIFIER).toString
    val id: String = UUIDs.timeBased().toString

    // Reuse request_id if present, else generate new
    val requestId: String =
      Option(rowMap.get(ContentConstants.RQST_ID))
        .map(_.toString)
        .getOrElse(UUIDs.timeBased().toString)

    val nowTs = new java.sql.Timestamp(System.currentTimeMillis())

    val auditRow = new java.util.HashMap[String, AnyRef]()

    auditRow.put(ContentConstants.IDENTIFIER, contentId)
    auditRow.put(ContentConstants.ID, id)
    auditRow.put(ContentConstants.RQST_ID, requestId)

    // Directly reuse fields from rowMap (NO NEW VALIDATION)
    auditRow.put(ContentConstants.USER_ID_RAISED_FIELD, rowMap.get(ContentConstants.USER_ID_RAISED_FIELD))
    auditRow.put(ContentConstants.RSN_FOR_RETIREMENT, rowMap.get(ContentConstants.RSN_FOR_RETIREMENT))
    auditRow.put(ContentConstants.LST_ENR_DATE, rowMap.get(ContentConstants.LST_ENR_DATE))
    auditRow.put(ContentConstants.RET_DATE, rowMap.get(ContentConstants.RET_DATE))

    // Reviewed fields are usually NULL for first request
    auditRow.put("reviewed_by", rowMap.getOrDefault(ContentConstants.REVIEWED_BY, null))
    auditRow.put("reviewed_at", rowMap.getOrDefault(ContentConstants.REVIEWED_AT, null))
    auditRow.put("reviewed_comment", rowMap.getOrDefault(ContentConstants.REVIEWED_COMMENT, null))

    auditRow.put(ContentConstants.CREATED_AT, nowTs)
    auditRow.put(ContentConstants.UPDATED_AT, nowTs)

    // Status is already set in rowMap
    auditRow.put(ContentConstants.STATUS, rowMap.get(ContentConstants.STATUS))

    val primaryKeys = java.util.Arrays.asList("content_id", "id")

    val auditStore = new ExternalStore(
      Platform.config.getString("cassandra.keyspace.course.content"),
      Platform.config.getString("content.retirement.requests.audit"),
      primaryKeys
    )
    auditStore.insert(auditRow, Map.empty).map { _ =>
      ResponseHandler.OK()
    }
  }

  def read(identifier: String, externalProperties: List[String], propertyTypeMapping: Map[String, String])(implicit ec: ExecutionContext): Future[Response] = {
    val select = QueryBuilder.select()
    if(null != externalProperties && !externalProperties.isEmpty){
      externalProperties.foreach(prop => {
        if("blob".equalsIgnoreCase(propertyTypeMapping.getOrElse(prop, "")))
          select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop)
        else
          select.column(prop).as(prop)
      })
    }
    val keyspace = Platform.getString(ContentConstants.SUNBIRD__KEYSPACE, "sunbird")
    val table    = Platform.getString(ContentConstants.CB_PLAN_LOOKUP_TABLE, "cb_plan_v2_content_lookup")
    val selectQuery = select.from(keyspace, table)
    val clause: Clause = QueryBuilder.eq(ContentConstants.CONTENT_ID, identifier)
    selectQuery.where.and(clause)
    try {
      val session: Session = CassandraConnector.getSession
      session.executeAsync(selectQuery).asScala.map(resultSet => {
        if (resultSet.iterator().hasNext) {
          val row = resultSet.one()
          val externalMetadataMap = externalProperties.map(prop => prop -> row.getObject(prop)).toMap
          val response = ResponseHandler.OK()
          import scala.collection.JavaConverters._
          response.putAll(externalMetadataMap.asJava)
          response
        } else {
          TelemetryManager.error("Entry is not found in cassandra for content with identifier: " + identifier)
          ResponseHandler.ERROR(ResponseCode.RESOURCE_NOT_FOUND, ResponseCode.RESOURCE_NOT_FOUND.code().toString, "Entry is not found in cassandra for content with identifier: " + identifier)
        }
      })
    } catch {
      case e: Exception =>
        e.printStackTrace()
        TelemetryManager.error("Exception Occurred While Reading The Record. | Exception is : " + e.getMessage, e)
        throw new ServerException(ErrorCodes.ERR_SYSTEM_EXCEPTION.name, "Exception Occurred While Reading The Record. Exception is : " + e.getMessage)
    }
  }

  implicit class RichListenableFuture[T](lf: ListenableFuture[T]) {
    def asScala : Future[T] = {
      val p = Promise[T]()
      Futures.addCallback(lf, new FutureCallback[T] {
        def onFailure(t: Throwable): Unit = p failure t
        def onSuccess(result: T): Unit    = p success result
      }, MoreExecutors.directExecutor())
      p.future
    }
  }

}
