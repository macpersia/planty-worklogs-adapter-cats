package com.github.macpersia.planty.views.cats

import java.io.{File, PrintStream}
import java.net.URI
import java.time.ZonedDateTime
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
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}


case class ConnectionConfig(
                             baseUri: URI,
                             username: String,
                             password: String,
                             customerId: String,
                             customerKey: String) {
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
  implicit val sslClient = NingWSClient()

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
         else Console.out )) {
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

    val dateFormatter: DateTimeFormatter = ofPattern("yyyyMMdd")
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
    val loginResult = loginResp.json.validate[CatsUser].get
    val sessionId = loginResult.sessionId.get
    logger.debug("Current user's session ID: " + sessionId)
    sessionId
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
    for (worklog  <- searchResult.times.map(_.copy(baseUrl = baseUrlOption)).par) {
       myWorklogs.add(worklog)
       worklogsMap.put(worklog, worklog.comment)
    }
    return worklogsMap
  }

//  private def retrieveWorklogsFromRestAPI(issue: BasicIssue, username: String, password: String): ParSeq[CatsWorklog] = {
//
//    val worklogsUrl = s"${connConfig.baseUriWithSlash}rest/api/2/issue/${issue.key}/worklog"
//    val reqTimeout = Duration(2, MINUTES)
//    val worklogsReq = WS.clientUrl(worklogsUrl)
//                    .withAuth(connConfig.username, connConfig.password, BASIC)
//                    .withHeaders("Content-Type" -> "application/json")
//                    .withQueryString("maxResults" -> "1000")
//    val respFuture = worklogsReq.get()
//    val resp = Await.result(respFuture, reqTimeout)
//
//    resp.json.validate[IssueWorklogs] match {
//      case JsSuccess(issueWorklogs, path) =>
//        val baseUrl = connConfig.baseUriWithSlash
//        val enhancedWorklogs = issueWorklogs.worklogs.map(_.map(w => w.copy(
//            issueKey = Option(issue.key), baseUrl = Option(baseUrl)
//        )))
//        val enhancedIssueWorklogs = issueWorklogs.copy(
//          baseUrl = Option(baseUrl), issueKey = Option(issue.key), worklogs = enhancedWorklogs
//        )
//        cacheManager.updateIssueWorklogs(enhancedIssueWorklogs) onSuccess {
//          case lastError => if (lastError.ok)
//            cacheManager.updateIssue(issue)
//        }
//        return (enhancedIssueWorklogs.worklogs getOrElse immutable.Seq.empty).par
//      case JsError(errors) =>
//        for (e <- errors) logger.error(e.toString())
//        logger.debug("The body of search response: \n" + resp.body)
//        throw new RuntimeException("Retrieving Worklogs Failed!")
//    }
//  }
//
//  def isLoggedBy(username: String, worklog: CatsWorklog): Boolean = {
//    worklog.author.name.equalsIgnoreCase(username)
//  }
//
//  def isWithinPeriod(fromDate: LocalDate, toDate: LocalDate, worklog: CatsWorklog): Boolean = {
//    val startDate = worklog.started.atStartOfDay(zoneId).toLocalDate
//    startDate.isEqual(fromDate) || startDate.isEqual(toDate) ||
//      (startDate.isAfter(fromDate) && startDate.isBefore(toDate))
//  }
//
//  def toFuzzyDuration(totalMinutes: Int): String = {
//    val hours = totalMinutes / 60
//    val minutes = totalMinutes % 60
//    if (minutes == 0)
//      s"$hours h"
//    else
//      s"$hours h, $minutes m"
//  }
}
