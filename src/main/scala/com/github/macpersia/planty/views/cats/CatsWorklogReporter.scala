package com.github.macpersia.planty.views.cats

import java.io.{File, PrintStream}
import java.net.URI
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ofPattern
import java.util
import java.util.Collections._
import java.util._
import java.util.concurrent.TimeUnit.MINUTES

import com.github.macpersia.planty.views.cats.CatsWorklogReporter.{DATE_FORMATTER, TS_FORMATTER}
import com.github.macpersia.planty.worklogs.WorklogReporting
import com.github.macpersia.planty.worklogs.model.{WorklogEntry, WorklogFilter}
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json.{JsError, JsSuccess}
import play.api.libs.ws.WS
import play.api.libs.ws.ning.NingWSClient
import resource.managed

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.collection.immutable

case class ConnectionConfig(
                             baseUri: URI,
                             username: String,
                             password: String,
                             customerId: String = "YOUR-CUSTOMER-ID",
                             customerKey: String = "YOUR-CUSTOMER-KEY") {
  val baseUriWithSlash = {
    val baseUriStr = baseUri.toString
    if (baseUriStr.endsWith("/")) baseUriStr
    else s"$baseUriStr/"
  }
}

object CatsWorklogReporter extends LazyLogging {
  val DATE_FORMATTER = DateTimeFormatter.ISO_DATE
  val TS_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV")
}

import com.github.macpersia.planty.views.cats.model._

class CatsWorklogReporter(connConfig: ConnectionConfig, filter: WorklogFilter)
                         (implicit execContext: ExecutionContext)
  extends LazyLogging with WorklogReporting {

  val zoneId = filter.timeZone.toZoneId
  lazy val cacheManager = CacheManager.instance
  implicit val sslClient = NingWSClient()

  val dateFormatter: DateTimeFormatter = ofPattern("yyyyMMdd")

  override def close(): Unit = {
    if (sslClient != null) sslClient.close()
  }

  type BasicIssue = String

  class WorklogComparator(worklogsMap: util.Map[CatsWorklog, BasicIssue])
    extends Comparator[CatsWorklog] {

    def compare(w1: CatsWorklog, w2: CatsWorklog) = {
      Ordering[(Long, String)].compare(
        (w1.date.toEpochDay, w1.comment),
        (w2.date.toEpochDay, w2.comment)
      )
    }
  }

  def printWorklogsAsCsv(outputFile: Option[File]) {
    for (csvPrintStream <- managed(
      if (outputFile.isDefined) new PrintStream(outputFile.get)
      else Console.out)) {
      for (entry <- retrieveWorklogs())
        printWorklogAsCsv(entry, csvPrintStream, DATE_FORMATTER)
    }
  }

  private def printWorklogAsCsv(entry: WorklogEntry, csvPs: PrintStream, formatter: DateTimeFormatter) {
    val date = formatter format entry.date
    csvPs.println(s"$date, ${entry.description}, ${entry.duration}")
  }

  override def retrieveWorklogs(): Seq[WorklogEntry] = {

    logger.debug(s"Searching the CATS at ${connConfig.baseUriWithSlash} as ${connConfig.username}")

    val reqTimeout = Duration(1, MINUTES)

    val nonce = ZonedDateTime.now()
    val sessionId = login(reqTimeout, nonce)

    val fromDateFormatted: String = dateFormatter.format(filter.fromDate)
    val toDateFormatted: String = dateFormatter.format(filter.toDate)

    val searchUrl = connConfig.baseUriWithSlash + "api/times"
    val searchReq = WS.clientUrl(searchUrl)
      .withHeaders(
        "Accept-Language" -> "en",
        "sid" -> sessionId,
        "Content-Type" -> "application/json; charset=utf-8",
        "Accept" -> "application/json",
        "Timestamp" -> nonce.format(TS_FORMATTER),
        "Consumer-Id" -> connConfig.customerId,
        "Consumer-Key" -> connConfig.customerKey,
        "Version" -> "1.0"
      ).withQueryString(
      "from" -> fromDateFormatted,
      "to" -> toDateFormatted,
      "_" -> s"${nonce.toEpochSecond}"
    )
    val searchFuture = searchReq.get()
    val searchResp = Await.result(searchFuture, reqTimeout)
    logger.debug("The search response JSON: " + searchResp.json)
    searchResp.json.validate[CatsSearchResult] match {
      case JsSuccess(searchResult, path) =>
        val worklogsMap: util.Map[CatsWorklog, BasicIssue] = extractWorklogs(searchResult)
        cacheManager.updateWorklogs(worklogsMap.keys.to[immutable.Seq])
        return toWorklogEntries(worklogsMap)
      case JsError(errors) =>
        for (e <- errors) logger.error(e.toString())
        logger.debug("The body of search response: \n" + searchResp.body)
        throw new RuntimeException("Search Failed!")
    }
  }

  def login(reqTimeout: FiniteDuration, nonce: ZonedDateTime): String = {
    val loginUrl = connConfig.baseUriWithSlash + "api/users"
    val loginReq = WS.clientUrl(loginUrl)
      .withHeaders(
        "Accept-Language" -> "en",
        "User" -> connConfig.username,
        "Password" -> connConfig.password,
        "Content-Type" -> "application/json; charset=utf-8",
        "Accept" -> "application/json",
        "Timestamp" -> nonce.format(TS_FORMATTER),
        "Consumer-Id" -> connConfig.customerId,
        "Consumer-Key" -> connConfig.customerKey,
        "Version" -> "1.0"
      ).withQueryString(
      "_" -> s"${nonce.toEpochSecond}"
    )
    val loginFuture = loginReq.get()
    val loginResp = Await.result(loginFuture, reqTimeout)
    loginResp.json.validate[CatsUser] match {
      case JsSuccess(loginResult, path) =>
        val loginResult = loginResp.json.validate[CatsUser].get
        val sessionId = loginResult.sessionId.get
        logger.debug("Current user's session ID: " + sessionId)
        sessionId
      case JsError(errors) =>
        for (e <- errors) logger.error(e.toString())
        logger.debug("The body of login response: \n" + loginResp.body)
        throw new RuntimeException("Login Failed!")
    }
  }

  def updateWorklogHours(issueKey: String, worklogDate: LocalDate, hoursSpent: Double): Int = {
    val worklog = Await.result(
      cacheManager.listWorklogs(connConfig.baseUriWithSlash, issueKey), Duration(30, SECONDS)
    ).find(w => w.date == worklogDate)
    updateWorklogHours(
      issueKey, worklog.get.id, hoursSpent,
      worklog.get.date, worklog.get.activityId, worklog.get.orderId, worklog.get.suborderId)
  }

  def updateWorklogHours(issueKey: String, worklogId: String, hoursSpent: Double,
                         worklogDate: LocalDate, activityId: String, orderId: String, suborderId: Option[String]): Int = {

    logger.debug(s"Updating the CATS entry on ${worklogDate} for suborder ID: ${suborderId}...")
    val reqTimeout = Duration(1, MINUTES)
    val nonce = ZonedDateTime.now()
    val sessionId = login(reqTimeout, nonce)

    val updateUrl = s"${connConfig.baseUriWithSlash}api/times/${worklogId}"
    val updateReq = WS.clientUrl(updateUrl)
      .withHeaders(
        "Accept-Language" -> "en",
        "sid" -> sessionId,
        "Content-Type" -> "application/json; charset=utf-8",
        "Accept" -> "application/json",
        "Timestamp" -> nonce.format(TS_FORMATTER),
        "Consumer-Id" -> connConfig.customerId,
        "Consumer-Key" -> connConfig.customerKey,
        "Version" -> "1.0"
      )
    //    val updateFuture = updateReq.put(
    //      f"""
    //         |  {
    //         |    "date": "${dateFormatter.format(date)}",
    //         |    "workingHours": "${hoursSpent}%1.2f",
    //         |    "comment": "${issueKey}",
    //         |    "orderid": "${orderId}",
    //         |    "suborderid": "${suborderId}",
    //         |    "activityid": "${activityId}",
    //         |    "standarddescriptionid": null,
    //         |    "standarddescriptionfrontsystem": null
    //         |  }
    //      """.stripMargin)

    val updateFuture = updateReq.put(
      f"""
         |  {
         |    "date": "${dateFormatter.format(worklogDate)}",
         |    "workingHours": "${hoursSpent}%1.2f",
         |    "comment": "${issueKey}",
         |    "activityid": "${activityId}",
         |    "orderid": "${orderId}",
         |    "suborderid": "${suborderId.getOrElse("")}"
         |  }
      """.stripMargin)

    val updateResp = Await.result(updateFuture, reqTimeout)
    logger.debug("The update response JSON: " + updateResp.body)
    updateResp.status
  }

  def createWorklog(issueKey: String, worklogDate: LocalDate, zone: ZoneId, hoursSpent: Double,
                    activityId: String, orderId: String, suborderId: Option[String]): Int = {

    logger.debug(s"Creating a CATS entry on ${worklogDate} for suborder ID: ${suborderId}...")
    val reqTimeout = Duration(1, MINUTES)
    val nonce = ZonedDateTime.now()
    val sessionId = login(reqTimeout, nonce)

    val createUrl = s"${connConfig.baseUriWithSlash}api/times"
    val createReq = WS.clientUrl(createUrl)
      .withHeaders(
        "Accept-Language" -> "en",
        "sid" -> sessionId,
        "Content-Type" -> "application/json; charset=utf-8",
        "Accept" -> "application/json",
        "Timestamp" -> nonce.format(TS_FORMATTER),
        "Consumer-Id" -> connConfig.customerId,
        "Consumer-Key" -> connConfig.customerKey,
        "Version" -> "1.0"
      )
    //    val updateFuture = updateReq.put(
    //      f"""
    //         |  {
    //         |    "date": "${dateFormatter.format(date)}",
    //         |    "workingHours": "${hoursSpent}%1.2f",
    //         |    "comment": "${issueKey}",
    //         |    "orderid": "${orderId}",
    //         |    "suborderid": "${suborderId}",
    //         |    "activityid": "${activityId}",
    //         |    "standarddescriptionid": null,
    //         |    "standarddescriptionfrontsystem": null
    //         |  }
    //      """.stripMargin)

    val createFuture = createReq.post(
      f"""
         |  {
         |    "date": "${dateFormatter.format(worklogDate.atStartOfDay(zone).plusHours(12))}",
         |    "workingHours": "${hoursSpent}%1.2f",
         |    "comment": "${issueKey}",
         |    "activityid": "${activityId}",
         |    "orderid": "${orderId}",
         |    "suborderid": "${suborderId.getOrElse("")}"
         |  }
      """.stripMargin)

    val createResp = Await.result(createFuture, reqTimeout)
    logger.debug("The creation response JSON: " + createResp.body)
    createResp.status
  }

  def toWorklogEntries(worklogsMap: util.Map[CatsWorklog, BasicIssue]): Seq[WorklogEntry] = {
    if (worklogsMap.isEmpty)
      return Seq.empty
    else {
      val sortedWorklogsMap: util.SortedMap[CatsWorklog, BasicIssue] = new util.TreeMap(new WorklogComparator(worklogsMap))
      sortedWorklogsMap.putAll(worklogsMap)
      val worklogEntries =
        for (worklog <- sortedWorklogsMap.keySet.iterator)
          yield toWorklogEntry(sortedWorklogsMap, worklog)

      return worklogEntries.toSeq
    }
  }

  def toWorklogEntry(sortedReverseMap: util.SortedMap[CatsWorklog, BasicIssue], worklog: CatsWorklog) = {
    val issueKey = sortedReverseMap.get(worklog)
    val hoursPerLog = worklog.workingHours
    new WorklogEntry(
      date = worklog.date,
      description = issueKey,
      duration = hoursPerLog)
  }

  def extractWorklogs(searchResult: CatsSearchResult)
  : util.Map[CatsWorklog, BasicIssue] = {

    val worklogsMap: util.Map[CatsWorklog, BasicIssue] = synchronizedMap(new util.HashMap)
    val myWorklogs: util.List[CatsWorklog] = synchronizedList(new util.LinkedList)

    val baseUrlOption = Option(connConfig.baseUriWithSlash)
    for (worklog <- searchResult.times.map(_.copy(baseUrl = baseUrlOption)).par) {
      myWorklogs.add(worklog)
      worklogsMap.put(worklog, worklog.comment)
    }
    return worklogsMap
  }
}
