package controllers.v3

import akka.actor.{ActorRef, ActorSystem}
import com.google.inject.Singleton
import controllers.BaseController
import org.apache.commons.lang.StringUtils
import org.sunbird.common.exception.ClientException
import org.sunbird.content.util.ContentConstants
import play.api.mvc.ControllerComponents
import play.mvc.Action
import utils.{ActorNames, ApiId}

import javax.inject.{Inject, Named}
import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

@Singleton
class ExtendedContentController @Inject()(@Named(ActorNames.EXTENDED_CONTENT_ACTOR) contentActor: ActorRef, @Named(ActorNames.COLLECTION_ACTOR) collectionActor: ActorRef, cc: ControllerComponents, actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends BaseController(cc) {

  val objectType = "Content"
  val schemaName: String = "content"
  val version = "1.0"

  def scheduleRetirement = Action.async { implicit request =>
    val headers = commonHeaders()
    val wrapper = body()
    wrapper.putAll(headers)
    val contentRequest = getRequest(wrapper, headers, "scheduleRetirement")
    setRequestContext(contentRequest, version, objectType, schemaName)
    val userIdFromHeader =
      request.headers.get("X-Authenticated-Userid")
        .orElse(request.headers.get("x-authenticated-userid"))
        .getOrElse("")
    contentRequest.getContext.put("X-Authenticated-Userid", userIdFromHeader)
    getResult(ApiId.RETIRE_SCHEDULER_V1, contentActor, contentRequest)
  }

  def isRetirementScheduled(identifier: String) = Action.async { implicit request =>
    if (StringUtils.isBlank(identifier)) {
      throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      )
    }
    val headers = commonReadHeaders()
    val content = new java.util.HashMap().asInstanceOf[java.util.Map[String, Object]]
    content.putAll(headers)
    content.putAll(Map("identifier" -> identifier).asJava)
    val readRequest = getRequest(content, headers, "isRetirementScheduled")
    setRequestContext(readRequest, version, objectType, schemaName)
    getResult(ApiId.VALIDATE_RETIREMENT, contentActor, readRequest, true)
  }

  def decideRetirementRequest = Action.async { implicit request =>
    val headers = commonHeaders()
    val wrapper = body()
    wrapper.putAll(headers)
    val contentRequest = getRequest(wrapper, headers, "decideRetirementRequest")
    setRequestContext(contentRequest, version, objectType, schemaName)
    // (Optional) pass authenticated user to context if needed downstream
    val userIdFromHeader =
      request.headers.get("X-Authenticated-Userid")
        .orElse(request.headers.get("x-authenticated-userid"))
        .getOrElse("")
    contentRequest.getContext.put("X-Authenticated-Userid", userIdFromHeader)
    getResult(ApiId.RETIREMENT_REQUEST_DECIDE_V1, contentActor, contentRequest)
  }

  def getRetirementStatus = Action.async { implicit request =>
    val headers = commonHeaders()
    val wrapper = requestBody()
    wrapper.putAll(headers)
    val contentRequest =
      getRequest(wrapper, headers, "getRetirementStatus")
    setRequestContext(contentRequest, version, objectType, schemaName)
    val userId =
      request.headers.get("x-authenticated-userid")
        .orElse(request.headers.get("X-Authenticated-Userid"))
        .getOrElse("")
    contentRequest.getContext.put("X-Authenticated-Userid", userId)
    getResult(
      ApiId.RETIREMENT_STATUS_V1,
      contentActor,
      contentRequest
    )
  }


}
