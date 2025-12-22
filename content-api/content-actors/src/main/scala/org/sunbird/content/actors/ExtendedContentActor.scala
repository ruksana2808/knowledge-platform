package org.sunbird.content.actors

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.actor.core.BaseActor
import org.sunbird.cassandra.CassandraConnector
import org.sunbird.cloudstore.StorageService
import org.sunbird.common.{JsonUtils, Platform}
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ResponseCode}
import org.sunbird.content.util.{ContentConstants, RetireManager}
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.external.store.ExternalStore
import org.sunbird.graph.nodes.DataNode
import org.sunbird.util.RequestUtil

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import javax.inject.Inject
import scala.collection.Map
import scala.concurrent.{ExecutionContext, Future, Promise}

class ExtendedContentActor @Inject() (implicit oec: OntologyEngineContext, ss: StorageService) extends BaseActor{


  private val logger: Logger = LoggerFactory.getLogger("ContentActor")
  val excludedCategories: Set[String] = Set(ContentConstants.LEARNING_RESOURCE)
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

  override def onReceive(request: Request): Future[Response] = {
    request.getOperation match {
      case "createVersionContent" => createNewVersionOfContent(request)
      case "scheduleRetirement" => scheduleRetirement(request)
      case "isRetirementScheduled" => isRetirementScheduled(request)
      case "decideRetirementRequest" => decideRetirementRequest(request)
      case "getRetirementStatus" => getRetirementStatus(request)
      case _ => ERROR(request.getOperation)
    }
  }

  def scheduleRetirement(request: Request): Future[Response] = {
    RetireManager.scheduleRetirement(request)
  }

  def createNewVersionOfContent(request: Request)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Response] = {
    val sourceCollectionId = request.getRequest.get(ContentConstants.SOURCE_COLLECTION_ID).asInstanceOf[String]
    val createdBy = request.getRequest.get(ContentConstants.CREATED_BY).asInstanceOf[String]
    val creator = request.getRequest.get(ContentConstants.CREATOR).asInstanceOf[String]
    val createdFor = request.getRequest.get(ContentConstants.CREATED_FOR).asInstanceOf[java.util.List[String]]
    val organisation = request.getRequest.get(ContentConstants.ORGANISATION).asInstanceOf[java.util.List[String]]
    val creatorContacts = request.getRequest.get(ContentConstants.CREATOR_CONTACTS).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    val channel = request.getRequest.get(ContentConstants.CHANNEL).asInstanceOf[String]
    if (StringUtils.isBlank(sourceCollectionId))
      throw new ClientException("ERR_INVALID_REQUEST", "sourceCollectionId is required")

    val readReq = new Request()
    readReq.setContext(new java.util.HashMap[String, AnyRef]() {
      {
        put("graph_id", "domain")
        put("version", ContentConstants.SCHEMA_VERSION)
        put("objectType", ContentConstants.CONTENT_OBJECT_TYPE)
        put("schemaName", ContentConstants.CONTENT_SCHEMA_NAME)
      }
    })
    readReq.put(ContentConstants.IDENTIFIER, sourceCollectionId)
    readReq.put(ContentConstants.MODE, "read")
    DataNode.read(readReq).flatMap { oldNode =>
      val oldMeta = oldNode.getMetadata
      val status = oldMeta.getOrDefault(ContentConstants.STATUS, "").asInstanceOf[String]
      val name = oldMeta.getOrDefault(ContentConstants.NAME, "").asInstanceOf[String]
      val newName: String =
        Option(request.getRequest.get(ContentConstants.NAME))
          .map(_.toString.trim)
          .filter(_.nonEmpty)
          .getOrElse(name)
      val sourceLangList = oldMeta.getOrDefault(ContentConstants.LANGUAGE, new util.ArrayList[String]()).asInstanceOf[java.util.List[String]]
      val baseLang = if (CollectionUtils.isNotEmpty(sourceLangList)) sourceLangList.get(0).toLowerCase else throw new ClientException("ERR_MISSING_LANGUAGE", "Source content must have one language")
      val contentType = oldMeta.getOrDefault(ContentConstants.CONTENT_TYPE, "").asInstanceOf[String]
      val primaryCategory = oldMeta.getOrDefault(ContentConstants.PRIMARY_CATEGORY, "").asInstanceOf[String]
      val mimeType = oldMeta.getOrDefault(ContentConstants.MIME_TYPE, "").asInstanceOf[String]
      val posterImage = oldMeta.getOrDefault(ContentConstants.POSTER_IMAGE, "").asInstanceOf[String]
      val appIcon = oldMeta.getOrDefault(ContentConstants.APP_ICON, "").asInstanceOf[String]
      val creatorLogo = oldMeta.getOrDefault(ContentConstants.CREATOR_LOGO, "").asInstanceOf[String]

      if (!StringUtils.equalsIgnoreCase(status, "Live"))
        throw new ClientException("ERR_INVALID_CONTENT_STATUS", s"Content $sourceCollectionId must be in Live status")

      // DETERMINE NEXT VERSION
      val oldVersion = Option(oldMeta.get(ContentConstants.CONTENT_VERSION)).map(_.toString).getOrElse("v1")
      var nextVersionNum = extractVersionNumber(oldVersion) + 1

      // check contentVersionInfo list
      val versionInfoObj = oldMeta.getOrDefault(ContentConstants.CONTENT_VERSION_INFO, new java.util.ArrayList[java.util.Map[String, AnyRef]]())
      val versionList = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
      versionInfoObj match {
        case list: java.util.List[_] => list.asScala.foreach(i => versionList.add(i.asInstanceOf[java.util.Map[String, AnyRef]]))
        case map: java.util.Map[_, _] => versionList.add(map.asInstanceOf[java.util.Map[String, AnyRef]])
        case s: String =>
          try {
            val parsed = JsonUtils.deserialize(s, classOf[java.util.List[java.util.Map[String, AnyRef]]])
            if (parsed != null) parsed.asScala.foreach(i => versionList.add(i))
          } catch {
            case _: Throwable =>
          }
        case _ =>
      }
      val highestExisting = versionList.asScala.toList.map { entry =>
        val v = Option(entry.get("version")).map(_.toString).getOrElse("v1")
        extractVersionNumber(v)
      }.foldLeft(0)((acc, n) => Math.max(acc, n))

      if (highestExisting >= nextVersionNum)
        nextVersionNum = highestExisting + 1
      val nextVersion = s"v$nextVersionNum"
      val contentMap = new java.util.HashMap[String, AnyRef]()
      contentMap.put(ContentConstants.NAME, newName)
      contentMap.put(ContentConstants.CREATED_BY, createdBy)
      contentMap.put(ContentConstants.CREATED_FOR, createdFor)
      contentMap.put(ContentConstants.CREATOR, creator)
      contentMap.put(ContentConstants.ORGANISATION, organisation)
      contentMap.put(ContentConstants.CHANNEL, channel)
      contentMap.put(ContentConstants.CREATOR_CONTACTS, creatorContacts)
      contentMap.put(ContentConstants.CONTENT_TYPE, contentType)
      contentMap.put(ContentConstants.PRIMARY_CATEGORY, primaryCategory)
      contentMap.put(ContentConstants.MIME_TYPE, mimeType)
      contentMap.put(ContentConstants.APP_ICON, appIcon)
      contentMap.put(ContentConstants.POSTER_IMAGE, posterImage)
      contentMap.put(ContentConstants.COURSE_CATEGORY, oldMeta.get(ContentConstants.COURSE_CATEGORY))
      contentMap.put(ContentConstants.CODE, scala.util.Random.nextInt(900000000) + 1000000000 toString)
      contentMap.put(ContentConstants.LANGUAGE, util.Arrays.asList(baseLang.capitalize))

      if (StringUtils.isNotBlank(creatorLogo)) {
        contentMap.put(ContentConstants.CREATOR_LOGO, creatorLogo)
      }
      contentMap.put(ContentConstants.PREVIOUS_VERSION_COURSE_ID, sourceCollectionId)
      contentMap.put(ContentConstants.CONTENT_VERSION, nextVersion)

      val createReq = new Request()
      createReq.setOperation("createContent")
      createReq.setRequest(contentMap)
      createReq.setContext(new java.util.HashMap[String, AnyRef]() {{
        put("graph_id", "domain")
        put("version", "1.0")
        put("objectType", "Collection")
        put("schemaName", "collection")
      }})
      create(createReq).flatMap { createResp =>
        val newCourseId = createResp.get(ContentConstants.IDENTIFIER).asInstanceOf[String]
        copyAccessSettingsForNewCourse(sourceCollectionId, newCourseId)
          .map { _ =>
            val response = ResponseHandler.OK()
            response.put("newVersionId", newCourseId)
            response.put("previousVersionId", sourceCollectionId)
            response.put(ContentConstants.CONTENT_VERSION, nextVersion)
            response
          }
      }

    }
  }

  private def extractVersionNumber(v: String): Int = {
    if (v == null) return 1
    val s = v.toLowerCase.trim
    val VersionRegex = ".*?v?\\s*(\\d+)(?:\\.\\d+)?$".r
    s match {
      case VersionRegex(n) => try {
        n.toInt
      } catch {
        case _: Throwable => 1
      }
      case _ => 1
    }
  }

  private def createRetirementAudit(request: Request): Future[Response] = {
    val req = request.getRequest.asInstanceOf[java.util.Map[String, Object]]

    def getOrError(key: String): String = {
      val v = req.get(key)
      if (v == null) throw new ClientException("ERR_INVALID_REQUEST", s"$key is required")
      v.toString
    }

    val contentId = getOrError(ContentConstants.CONTENT_ID)
    val userId = getOrError(ContentConstants.USER_ID_RAISED)
    val reason = req.get(ContentConstants.REASON_FOR_RETIREMENT)

    // Handle LAST/RETIREMENT DATE as LocalDate (Cassandra date)
    val lastEnrollment = parseToCassandraDate(req.get(ContentConstants.LAST_ENROLLMENT_DATE).toString)
    val retirementDate = parseToCassandraDate(req.get(ContentConstants.RETIREMENT_DATE).toString)
    val allowedStatuses = ContentConstants.VALID_RETIREMENT_STATUSES

    val status = Option(req.get(ContentConstants.STATUS))
      .map(_.toString.trim)
      .filter(_.nonEmpty)
      .map(_.toUpperCase)
      .getOrElse(throw new ClientException("ERR_INVALID_REQUEST", "status is required"))

    if (!allowedStatuses.contains(status)) {
      throw new ClientException("ERR_INVALID_STATUS",
        s"Invalid status: $status. Allowed: PENDING, APPROVED, REJECTED, RETIRED")
    }
    val reviewedBy: AnyRef = req.get(ContentConstants.REVIEWED_BY)
    val reviewedAt: AnyRef = parseToCassandraTimestamp(req.get(ContentConstants.REVIEWED_AT))
    val reviewedComment: AnyRef = req.get(ContentConstants.REVIEWED_COMMENT)

    import com.datastax.driver.core.utils.UUIDs
    val requestId = Option(req.get(ContentConstants.REQUEST_ID)).map(_.toString).getOrElse(UUIDs.timeBased().toString)
    val id = UUIDs.timeBased().toString
    val nowTs = new java.sql.Timestamp(System.currentTimeMillis())

    val row = new java.util.HashMap[String, Object]()
    row.put("identifier", contentId)
    row.put("id", id)
    row.put("request_id", requestId)
    row.put("user_id_raised", userId)
    row.put("reason_for_retirement", reason)
    row.put("last_enrollment_date", lastEnrollment)
    row.put("retirement_date", retirementDate)
    row.put("reviewed_by", reviewedBy)
    row.put("reviewed_at", reviewedAt)
    row.put("reviewed_comment", reviewedComment)
    row.put("created_at", nowTs)
    row.put("updated_at", nowTs)
    row.put("status", status)

    val primaryKeys = java.util.Arrays.asList("content_id", "id")

    val auditStore = new ExternalStore(
      Platform.config.getString("cassandra.keyspace.course.content"),
      Platform.config.getString("content.retirement.requests.audit"),
      primaryKeys
    )
    auditStore.insert(row, Map.empty[String, String]).map { _ =>
      val resp = ResponseHandler.OK()
      resp.put("contentId", contentId)
      resp.put("id", id)
      resp.put("requestId", requestId)
      resp.put("status", status)
      resp
    }
  }

  private def parseToCassandraDate(date: String): com.datastax.driver.core.LocalDate = {
    val parts = date.split("-")
    com.datastax.driver.core.LocalDate.fromYearMonthDay(
      parts(0).toInt, parts(1).toInt, parts(2).toInt
    )
  }

  private def parseToCassandraTimestamp(v: Any): java.util.Date = {
    if (v == null) return null
    val s = v.toString.trim

    try {
      // If full ISO timestamp → parse directly
      return java.util.Date.from(java.time.Instant.parse(s))
    } catch {
      case _: Throwable =>
        try {
          val localDate = java.time.LocalDate.parse(s)
          val instant = localDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant
          return java.util.Date.from(instant)
        } catch {
          case _: Throwable =>
            throw new ClientException("ERR_INVALID_DATE_FORMAT",
              s"Invalid date format: $s. Expected ISO timestamp or yyyy-MM-dd")
        }
    }
  }


  def isRetirementScheduled(request: Request): Future[Response] = {
    RetireManager.isRetirementScheduled(request)
  }

  def decideRetirementRequest(
                               request: Request
                             )(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Response] = {
    logger.info("Inside decideRetirementRequest method of RetireManager")
    validateDecideRetirementRequest(request)
    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get(ContentConstants.RQST))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(
        throw new ClientException(
          ContentConstants.ERR_INVALID_REQUEST,
          "Request body is missing."
        )
      )
    val contentId = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(org.apache.commons.lang3.StringUtils.isNotBlank)
      .getOrElse(
        throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT_ID,
          ContentConstants.ERR_CONTENT_ID_MISSING
        )
      )
    val action = Option(reqMap.get(ContentConstants.ACTION))
      .map(_.toString.trim.toUpperCase)
      .getOrElse(
        throw new ClientException(
          ContentConstants.ERR_INVALID_REQUEST,
          "Action is missing"
        )
      )
    val externalProperties: List[String] =
      Platform.config.getStringList(ContentConstants.RETIREMENT_READ_COLUMNS)
        .asScala
        .toList
    val propertyTypeMapping =
      scala.collection.immutable.Map.empty[String, String]
    for {
      _ <- RetireManager.isRetirementScheduled(request).map(_ => ())

      readResp <- retirementRequestStore.read(
        contentId,
        externalProperties,
        propertyTypeMapping
      )

      _ <- {
        if (readResp.getResponseCode != ResponseCode.OK) {
          Future.successful(())
        } else {
          val result =
            readResp.getResult.asInstanceOf[java.util.Map[String, AnyRef]]
          val requestId =
            result.get(ContentConstants.RQST_ID).toString
          updateRetirementRequestByCompositeKey(
            contentId = contentId,
            requestId = requestId,
            approvedBy = extractUserId(request),
            keySpace = Platform.getString(
              ContentConstants.SUNBIRD_COURSE_KEYSPACE,
              "sunbird_courses"
            ),
            table = Platform.getString(
              ContentConstants.CONTENT_RETIREMENT_RQST_TABLE,
              "content_retirement_requests"
            ),
            action = action
          ).flatMap { _ =>
            markContentPendingRetirement(
              request = request,
              retirementResult = result,
              action = action
            ).map { resp =>
              logger.info(
                s"[RETIRE-DECIDE][CONTENT-UPDATE][SUCCESS] contentId=$contentId, action=$action"
              )
              resp
            }
          }
        }
      }
    } yield {
      logger.info(
        s"[RETIRE-DECIDE][SUCCESS] contentId=$contentId, action=$action"
      )
      ResponseHandler.OK
        .put("node_id", contentId)
        .put("identifier", contentId)
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

  private def validateDecideRetirementRequest(request: Request): Unit = {
    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get(ContentConstants.RQST))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Request body is missing."
      ))
    Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      ))
    val action = Option(reqMap.get(ContentConstants.ACTION))
      .map(_.toString.trim.toUpperCase)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Action is mandatory."
      ))
    if (!Set("APPROVE", "REJECT").contains(action)) {
      throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        s"Invalid action: $action. Allowed values are APPROVE or REJECT."
      )
    }
    Option(reqMap.get(ContentConstants.COMMENT))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Comment is mandatory."
      ))
  }

  private def updateRetirementRequestByCompositeKey(
                                                     keySpace: String,
                                                     table: String,
                                                     contentId: String,
                                                     requestId: String,
                                                     approvedBy: String,
                                                     action : String
                                                   )(implicit ec: ExecutionContext): Future[Response] = {

    val update = QueryBuilder.update(keySpace, table)
    update.where
      .and(QueryBuilder.eq(ContentConstants.RETITEMENT_PRIMARY_KEY, contentId))
      .and(QueryBuilder.eq(ContentConstants.RQST_ID, requestId))
    val (approvedFlag, statusValue) =
      action match {
        case ContentConstants.APPROVE =>
          (java.lang.Boolean.TRUE, ContentConstants.APPROVED_KEY)
        case ContentConstants.REJECT =>
          (java.lang.Boolean.FALSE, ContentConstants.REJECTED)
        case _ =>
          throw new ClientException(
            ContentConstants.ERR_INVALID_REQUEST,
            s"Invalid action for retirement decision: $action"
          )
      }
    update
      .`with`(QueryBuilder.set(ContentConstants.APPROVED, java.lang.Boolean.TRUE))
      .and(QueryBuilder.set(ContentConstants.APPROVED_BY_RQST, approvedBy))
      .and(QueryBuilder.set(ContentConstants.APPROVED_AT, new java.util.Date()))
      .and(QueryBuilder.set(ContentConstants.STATUS, statusValue))
      .and(QueryBuilder.set(ContentConstants.APPROVED_COMMENT, action))

    CassandraConnector.getSession
      .executeAsync(update)
      .asScala
      .map { _ =>
        logger.info(
          s"[RETIRE-DECIDE][UPDATE-SUCCESS] contentId=$contentId, requestId=$requestId, approvedBy=$approvedBy"
        )
        ResponseHandler.OK()
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

  def markContentPendingRetirement(
                                    request: Request,
                                    retirementResult: java.util.Map[String, AnyRef],
                                    action: String
                                  )(
                                    implicit ec: ExecutionContext,
                                    oec: OntologyEngineContext
                                  ): Future[Response] = {

    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get("request"))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(
        throw new ClientException(
          ContentConstants.ERR_INVALID_REQUEST,
          "Request body is missing."
        )
      )

    val id = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(
        throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT_ID,
          ContentConstants.ERR_CONTENT_ID_MISSING
        )
      )

    val lastEnrollmentDate =
      retirementResult.get(ContentConstants.LAST_ENROLLMENT_DATE_RQST)

    val retirementDate =
      retirementResult.get(ContentConstants.RETIREMENT_DATE_RQST)

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
    DataNode.read(readReq).flatMap { node =>
      if (node == null)
        throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT_ID,
          s"Content is not found for identifier: $id"
        )
      val metadata = node.getMetadata
      val status = Option(metadata.get("status")).map(_.toString).getOrElse("")
      if (StringUtils.isBlank(status))
        throw new ClientException(
          "ERR_METADATA_ISSUE",
          s"Content metadata error, status is blank for identifier: ${node.getIdentifier}"
        )
      action match {
        case ContentConstants.APPROVE =>
          request.getRequest.put(
            ContentConstants.CONTENT_RETIREMENT_STS,
            ContentConstants.PENDING_RETIREMENT
          )
          if (lastEnrollmentDate != null) {
            request.getRequest.put(
              ContentConstants.LAST_ENROLLMENT_DATE,
              toOffsetTimestamp(lastEnrollmentDate)
            )
          }
          if (lastEnrollmentDate != null && retirementDate != null) {
            val lastEnrollmentdateFetched = toLocalDate(lastEnrollmentDate)
            val retirementDateFetched  = toLocalDate(retirementDate)
            val retirementGapInDays =
              ChronoUnit.DAYS.between(lastEnrollmentdateFetched, retirementDateFetched)
            val newRetirementDate =
              LocalDate.now().plusDays(retirementGapInDays)
            request.getRequest.put(
              ContentConstants.RETIREMENT_DATE,
              toOffsetTimestamp(newRetirementDate)
            )
            logger.info(
              s"[RETIRE-DECIDE][RETIREMENT-DATE-RECALC] " + s"lastEnrollment=$lastEnrollmentdateFetched, " +
                s"oldRetirement=$retirementDateFetched, " + s"diffDays=$retirementGapInDays, " + s"newRetirement=$newRetirementDate"
            )
          }
        case ContentConstants.REJECT =>
          request.getRequest.put(
            ContentConstants.CONTENT_RETIREMENT_STS,
            ContentConstants.REJECTED
          )
      }
      request.setContext(new java.util.HashMap[String, AnyRef]() {{
        put("graph_id", "domain")
        put("version", "1.0")
        put("objectType", "Collection")
        put("schemaName", "collection")
      }})
      request.getRequest.put("versionKey", metadata.get("versionKey"))
      RequestUtil.restrictProperties(request)
      request.getContext.put(ContentConstants.IDENTIFIER, id)
      systemUpdate(request).map { updatedResp =>
        logger.info(
          s"[RETIRE-DECIDE][CONTENT-UPDATE] action=$action, contentId=$id"
        )
        ResponseHandler.OK
          .put("identifier", id)
          .put("status", action)
      }
    }
  }

  private def toOffsetTimestamp(value: AnyRef): String = {
    val formatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    value match {
      case date: java.util.Date =>
        ZonedDateTime
          .ofInstant(date.toInstant, ZoneId.systemDefault())
          .format(formatter)
      case date: java.time.LocalDate =>
        date.atStartOfDay(ZoneId.systemDefault())
          .format(formatter)
      case str: String =>
        str
      case _ =>
        ZonedDateTime
          .now(ZoneId.systemDefault())
          .format(formatter)
    }
  }

  def getRetirementStatus(request: Request)(implicit ec: ExecutionContext): Future[Response] = {
    logger.info("[RETIREMENT-STATUS] Inside getRetirementStatus")
    import scala.collection.JavaConverters._
    val reqMap: java.util.Map[String, AnyRef] =
      Option(request.getRequest)
        .getOrElse(
          throw new ClientException(
            ContentConstants.ERR_INVALID_REQUEST,
            "Request body is missing"
          )
        )
    val contentIds: List[String] =
      Option(reqMap.get("contentId"))
        .map(_.asInstanceOf[java.util.List[String]].asScala.toList)
        .getOrElse(
          throw new ClientException(
            ContentConstants.ERR_INVALID_REQUEST,
            "contentId is missing"
          )
        )
    logger.info(
      s"[RETIREMENT-STATUS][REQUEST] contentIds=${contentIds.mkString(",")}"
    )
    val externalProperties: List[String] =
      Platform.config
        .getStringList(ContentConstants.RETIREMENT_READ_COLUMNS)
        .asScala
        .toList
    val propertiesMapping: scala.collection.immutable.Map[String, String] =
      scala.collection.immutable.Map.empty
    retirementRequestStore
      .read(contentIds, externalProperties, propertiesMapping)
      .map { resp: Response =>
        logger.info(
          s"[RETIREMENT-STATUS][SUCCESS] responseCode=${resp.getResponseCode}"
        )
        val fetchedData =
          resp.getResult.asInstanceOf[java.util.Map[String, AnyRef]]
        val contentList = buildRetirementStatusResponse(fetchedData)
        ResponseHandler.OK
          .put("content", contentList)
      }
  }

  private def buildRetirementStatusResponse(fetchedData: java.util.Map[String, AnyRef]): java.util.List[java.util.Map[String, AnyRef]] = {
    import scala.collection.JavaConverters._
    fetchedData.asScala.map {
      case (contentId: String, rowAny: AnyRef) =>
        val retirementData =
          rowAny.asInstanceOf[java.util.Map[String, AnyRef]]
        val updatedData: java.util.Map[String, AnyRef] =
          new java.util.HashMap[String, AnyRef]()
        // always present
        updatedData.put(ContentConstants.CONTENT_ID, contentId)
        Option(retirementData.get(ContentConstants.LAST_ENROLLMENT_DATE_RQST))
          .foreach { v =>
            updatedData.put(
              ContentConstants.LAST_ENROLLMENT_DATE,
              toIsoDate(v)
            )
          }
        Option(retirementData.get(ContentConstants.RETIREMENT_DATE_RQST))
          .foreach { v =>
            updatedData.put(
              ContentConstants.RETIREMENT_DATE,
              toIsoDate(v)
            )
          }
        Option(retirementData.get(ContentConstants.STATUS))
          .foreach { result =>
            updatedData.put(
              ContentConstants.STATUS,
              result.toString
            )
          }
        Option(retirementData.get(ContentConstants.RSN_FOR_RETIREMENT))
          .foreach { result =>
            updatedData.put(
              ContentConstants.REASON,
              result
            )
          }
        Option(retirementData.get(ContentConstants.USER_ID_RAISED_FIELD))
          .foreach { result =>
            updatedData.put(
              ContentConstants.USER_ID_RAISED,
              result
            )
          }
        updatedData
    }.toList.asJava
  }

  import java.time.{LocalDate => JLocalDate, ZoneOffset}
  import java.time.format.DateTimeFormatter

  private val ISO_FORMATTER =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

  private def toIsoDate(value: AnyRef): String = {
    value match {
      case date: com.datastax.driver.core.LocalDate =>
        JLocalDate
          .of(date.getYear, date.getMonth, date.getDay)
          .atStartOfDay()
          .atZone(ZoneOffset.UTC)
          .format(ISO_FORMATTER)
      case date: java.util.Date =>
        date.toInstant
          .atZone(ZoneOffset.UTC)
          .format(ISO_FORMATTER)
      case _ =>
        null
    }
  }

  import java.time._

  private val OFFSET_FORMATTER =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

  private def toLocalDate(value: AnyRef): LocalDate = {
    value match {
      case date: com.datastax.driver.core.LocalDate =>
        LocalDate.of(date.getYear, date.getMonth, date.getDay)
      case dateString: String =>
        LocalDate.parse(dateString)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported date type: $value")
    }
  }

  private def toOffsetTimestamp(date: LocalDate): String = {
    date
      .atStartOfDay()
      .atZone(ZoneId.systemDefault())
      .format(OFFSET_FORMATTER)
  }

  private def copyAccessSettingsForNewCourse(oldCourseId: String, newCourseId: String)(implicit ec: ExecutionContext): Future[Unit] = {
    val keySpace = Platform.config.getString(ContentConstants.SUNBIRD_COURSE_KEYSPACE)
    val table = Platform.config.getString(ContentConstants.ACCESS_SETTING_RULES_V2_TABLE)
    val accessRuleStore = new ExternalStore(
      keySpace = keySpace,
      table = table,
      primaryKey = java.util.Arrays.asList(ContentConstants.CONTEXT_ID, ContentConstants.CONTEXT_ID_TYPE)
    )
    val readColumns = List(
      ContentConstants.CONTEXT_ID_TYPE,
      ContentConstants.CONTEXT_DATA,
      ContentConstants.IS_ARCHIVED
    )
    val propsMapping: scala.collection.immutable.Map[String, String] =
      scala.collection.immutable.Map(ContentConstants.CONTEXT_DATA -> "string")
    val sourceId: String = oldCourseId

    accessRuleStore
      .read(
        identifier = sourceId,
        extProps = readColumns,
        propsMapping = propsMapping
      )
      .flatMap { readResp =>
        if (readResp.getResponseCode == ResponseCode.OK) {

          val insertMap = new java.util.HashMap[String, AnyRef]()

          // Primary key for new row
          insertMap.put(ContentConstants.IDENTIFIER, newCourseId)

          // Copy remaining fields as-is
          val contextIdType: String =
            Option(readResp.get(ContentConstants.CONTEXT_ID_TYPE))
              .map(_.toString)
              .filter(_.nonEmpty)
              .getOrElse("Course")
          insertMap.put(ContentConstants.CONTEXT_ID_TYPE, contextIdType)

          val contextDataRaw = readResp.get(ContentConstants.CONTEXT_DATA)

          val updatedContextDataString: String = try {
            val parsed: java.util.Map[String, AnyRef] =
              contextDataRaw match {
                case s: String =>
                  JsonUtils.deserialize(s, classOf[java.util.Map[String, AnyRef]])
                case m: java.util.Map[_, _] =>
                  m.asInstanceOf[java.util.Map[String, AnyRef]]
                case _ => null
              }

            if (parsed != null) {
              parsed.put(ContentConstants.CONTENT_ID, newCourseId)
              JsonUtils.serialize(parsed)
            } else {
              contextDataRaw.toString
            }
          } catch {
            case e: Exception =>
              logger.warn("[ACCESS-SETTINGS] Failed to parse contextdata, using raw string", e)
              contextDataRaw.toString
          }
          insertMap.put(ContentConstants.CONTEXT_DATA, updatedContextDataString)
          insertMap.put(ContentConstants.IS_ARCHIVED, readResp.get(ContentConstants.IS_ARCHIVED))

          accessRuleStore.insert(insertMap, propsMapping).map(_ => ())
        } else {
          // No record exists → nothing to copy
          Future.successful(())
        }
      }
      .recover {
        case e: Exception =>
          logger.error(
            s"[ACCESS-SETTINGS][COPY-FAILED] oldCourseId=$oldCourseId, newCourseId=$newCourseId",
            e
          )
          ()
      }
  }

}
