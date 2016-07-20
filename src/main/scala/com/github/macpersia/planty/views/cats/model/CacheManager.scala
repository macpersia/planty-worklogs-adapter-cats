package com.github.macpersia.planty.views.cats.model

import java.time._

import reactivemongo.api.MongoDriver
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson._
import reactivemongo.core.commands._
import reactivemongo.core.nodeset.Authenticate

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.{ExecutionContext, Future}

object CacheManager {
  private val ctxToCacheMap = synchronized(mutable.Map[ExecutionContext, CacheManager]())

  def instance(implicit execContext: ExecutionContext): CacheManager =
    ctxToCacheMap.getOrElse(execContext, {
      val newCache = new CacheManager()(execContext)
      ctxToCacheMap.put(execContext, newCache)
      newCache
    })
}

class CacheManager private(implicit execContext: ExecutionContext) {

  private val driver = new MongoDriver

  private val mongoDbHost = sys.props.get("mongodb.host").getOrElse("localhost")
  private val mongoDbPort = sys.props.get("mongodb.port").getOrElse(27017)
  private val servers = Seq(s"$mongoDbHost:$mongoDbPort")

  private val mongoDbName = sys.props.get("mongodb.name").getOrElse("diy")
  private val mongoDbUsername = sys.props.get("mongodb.username")
  private val mongoDbPassword = sys.props.get("mongodb.password")
  private val credentials = Seq(Authenticate(mongoDbName, mongoDbUsername.orNull, mongoDbPassword.orNull))

  private val connection =
    if (mongoDbUsername.isDefined)
      driver.connection(servers, authentications = credentials)
    else
      driver.connection(servers)

  private val db = connection(mongoDbName)(execContext)
  private val worklogsColl: BSONCollection = db("cats.worklogs")

  val utcZoneId = ZoneId.of("UTC")

  implicit object InstantHandler extends BSONHandler[BSONDateTime, Instant] {
    def read(bson: BSONDateTime): Instant = Instant.ofEpochMilli(bson.value)

    def write(t: Instant) = BSONDateTime(t.toEpochMilli)
  }

  implicit object LocalDateHandler extends BSONHandler[BSONDateTime, LocalDate] {
    val MILLIS_IN_A_DAY = 24 * 60 * 60 * 1000
    def read(bson: BSONDateTime): LocalDate = LocalDate.ofEpochDay(bson.value / MILLIS_IN_A_DAY)
    def write(t: LocalDate) = BSONDateTime(t.toEpochDay * MILLIS_IN_A_DAY)
  }

  // implicit val userWriter = Macros.writer[CatsUser]
  implicit object UserWriter extends BSONDocumentWriter[CatsUser] {
    def write(user: CatsUser) = BSONDocument(
      "baseUrl" -> user.baseUrl,
      "name" -> user.name,
      "prename" -> user.prename,
      "defaultActivity" -> user.defaultActivity,
      "sessionId" -> user.sessionId
    )
  }

  // implicit val userReader = Macros.reader[CatsUser]
  implicit object UserReader extends BSONDocumentReader[CatsUser] {
    def read(doc: BSONDocument) = CatsUser(
      doc.getAs[String]("baseUrl"),
      doc.getAs[String]("name").get,
      doc.getAs[String]("prename").get,
      doc.getAs[String]("defaultActivity"),
      doc.getAs[String]("sessionId")
    )
  }

  implicit val worklogWriter = Macros.writer[CatsWorklog]
  implicit val worklogReader = Macros.reader[CatsWorklog]

  def updateWorklog(w: CatsWorklog): Unit = {
    worklogsColl.update(worklogsSelector(w), w, upsert = true)
  }

  def updateWorklogs(worklogs: Seq[CatsWorklog]): Unit = {
    for (w <- worklogs.par) updateWorklog(w)
  }

  //  def updateIssueWorklogs(iw: IssueWorklogs) = {
  //    issueWorklogsColl.update(issueWorklogsSelector(iw), iw, upsert = true)
  //  }
  //
  ////  def listIssues(): Future[Seq[BasicIssue]] = {
  ////    issuesColl.find(BSONDocument()).cursor[BasicIssue].collect[Seq]()
  ////  }
  //
  //  def getIssueByBaseUrlAndId(baseUrl: String, id: String) = {
  //    // issuesColl.find(BSONDocument("id" -> BSONDocument("$eq" -> id))).one[BasicIssue]
  //    issuesColl.find(issueSelector(baseUrl, id)).one[BasicIssue]
  //  }
  //
  //  def issueSelector(issue: BasicIssue): BSONDocument =
  //    issueSelector(issue.baseUrl.get, issue.id)
  //
  //  def issueSelector(baseUrl: String, id: String): BSONDocument = {
  //    BSONDocument("$and" -> BSONArray(
  //      BSONDocument("baseUrl" -> BSONDocument("$in" -> BSONArray(baseUrl))),
  //      BSONDocument("id" -> BSONDocument("$in" -> BSONArray(id)))
  //    ))
  //  }
  //
  def worklogsSelector(w: CatsWorklog): BSONDocument =
    worklogsSelector(w.baseUrl.get, w.comment)

  def worklogsSelector(baseUrl: String, comment: String): BSONDocument = {
    BSONDocument("$and" -> BSONArray(
      BSONDocument("baseUrl" -> BSONDocument("$in" -> BSONArray(baseUrl))),
      BSONDocument("comment" -> BSONDocument("$in" -> BSONArray(comment)))
    ))
  }

  def listWorklogs(baseUrl: String, comment: String): Future[ParSeq[CatsWorklog]] = {
    worklogsColl
      .find(worklogsSelector(baseUrl, comment))
      .cursor[CatsWorklog]
      .collect[ParSeq]()
  }

  //  def latestIssueTimestamp(baseUrl: String): Future[Option[ZonedDateTime]] = {
  //
  //    val latestTsAlias = "latestTimestamp"
  //
  //    //    import issuesColl.BatchCommands.AggregationFramework._
  //    //    // issuesColl.aggregate(Group(BSONString("$state"))(
  //    //    val futureRes = issuesColl.aggregate(Group(BSONNull)(
  //    //      fieldAlias -> Max("updated")
  //    //    ))
  //
  //    val maxCommand = Aggregate(issuesColl.name, Seq(
  //     Match(BSONDocument("baseUrl" -> baseUrl)),
  //     Group(BSONNull)(latestTsAlias -> Max("updated"))
  //    ))
  //    val futureRes = issuesColl.db.command(maxCommand)
  //
  //    futureRes
  //      .map(_.map(doc => doc.getAs[Instant](latestTsAlias).map(_.atZone(utcZoneId))))
  //      .map(s => if(s.isEmpty) None else s.head)
  //  }
}
