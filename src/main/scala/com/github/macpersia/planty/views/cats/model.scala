package com.github.macpersia.planty.views.cats

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.immutable.Seq

package object model {

  case class CatsUser(baseUrl: Option[String],
                      name: String,
                      prename: String,
                      defaultActivity: Option[String],
                      sessionId: Option[String])

  case class CatsSearchResult(baseUrl: Option[String],
                              sessionId: String,
                              times: Seq[CatsWorklog],
                              count: Int)

  case class CatsWorklog(baseUrl: Option[String],
                         id: String,
                         date: LocalDate, // yyyyMMdd
                         orderId: String,
                         orderHref: String,
                         orderShortText: Option[String],
                         activityId: String,
                         activityHref: Option[String],
                         activityShortText: Option[String],
                         suborderId: Option[String],
                         suborderHref: Option[String],
                         suborderShortText: Option[String],
                         comment: String,
                         // additionalComment: Option[String],
                         creationDate: LocalDate, // yyyy-MM-dd
                         modifiedDate: LocalDate, // yyyy-MM-dd
                         // extSystem: String = "GUI4CATS",
                         // standardDescriptionFrontSystem: Option[String],
                         // standardDescriptionHref: Option[String],
                         // standardDescriptionId: Option[String],
                         // standardDescriptionShortText: Option[String],
                         // trackedTimeStatus: Option[String],
                         workingHours: Double)

  val catsDFormatter1 = DateTimeFormatter.ofPattern("yyyyMMdd")
  val catsDFormatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val readsLocalDateForCats1 = Reads[LocalDate](js =>
    js.validate[String].map[LocalDate]
      (dtString => LocalDate.parse(dtString, catsDFormatter1))
  )

  val writesLocalDateForCats1 = Writes[LocalDate](date =>
    JsString(catsDFormatter1.format(date))
  )

  val readsLocalDateForCats2 = Reads[LocalDate](js =>
    js.validate[String].map[LocalDate]
      (dtString => LocalDate.parse(dtString, catsDFormatter2))
  )

  val writesLocalDateForCats2 = Writes[LocalDate](date =>
    JsString(catsDFormatter2.format(date))
  )

  //  import play.api.libs.json._

  // implicit val catsUserReads = Json.reads[CatsUser]
  implicit val catsUserReads: Reads[CatsUser] = (
    (JsPath \ "baseUrl").readNullable[String] and
      (JsPath \ "name").read[String] and
      (JsPath \ "prename").read[String] and
      (JsPath \ "defaultActivity").readNullable[String] and
      (JsPath \ "meta" \ "sid").readNullable[String]
    ) (CatsUser.apply _)

  //  implicit val catsUserWrites: Writes[CatsUser] = (
  //    (JsPath \ "baseUrl").writeNullable[String] and
  //    (JsPath \ "name").write[String] and
  //    (JsPath \ "prename").write[String] and
  //    (JsPath \ "defaultActivity").writeNullable[String] and
  //    (JsPath \ "meta" \ "sid").writeNullable[String]
  //  )(unlift(CatsUser.unapply))

  //  implicit val catsWorklogReads = Json.reads[CatsWorklog]
  implicit val catsWorklogReads: Reads[CatsWorklog] = (
    (JsPath \ "baseUrl").readNullable[String] and
      (JsPath \ "id").read[String] and
      (JsPath \ "date").read(readsLocalDateForCats1) and
      (JsPath \ "orderid").read[String] and
      (JsPath \ "orderhref").read[String] and
      (JsPath \ "ordershorttext").readNullable[String] and
      (JsPath \ "activityid").read[String] and
      (JsPath \ "activityhref").readNullable[String] and
      (JsPath \ "activityshorttext").readNullable[String] and
      (JsPath \ "suborderid").readNullable[String] and
      (JsPath \ "suborderhref").readNullable[String] and
      (JsPath \ "subordershorttext").readNullable[String] and
      (JsPath \ "comment").read[String] and
      // (JsPath \ "additionalcomment").readNullable[String] and
      (JsPath \ "creationDate").read(readsLocalDateForCats2) and
      (JsPath \ "modifiedDate").read(readsLocalDateForCats2) and
      // (JsPath \ "extsystem").read[String] and
      // (JsPath \ "standarddescriptionfrontsystem").readNullable[String] and
      // (JsPath \ "standarddescriptionhref").readNullable[String] and
      // (JsPath \ "standarddescriptionid").readNullable[String] and
      // (JsPath \ "standarddescriptionshorttext").readNullable[String] and
      // (JsPath \ "trackedtimeStatus").readNullable[String] and
      (JsPath \ "workingHours").read[Double]
    ) (CatsWorklog.apply _)

  //  implicit val catsWorklogWrites = Json.writes[CatsWorklog]
  //  implicit val catsWorklogWrites: Writes[CatsWorklog] = (
  //    (JsPath \ "baseUrl").writeNullable[String] and
  //    (JsPath \ "id").write[String] and
  //    (JsPath \ "date").write(writesLocalDateForCats1) and
  //    (JsPath \ "orderid").write[String] and
  //    (JsPath \ "orderhref").write[String] and
  //    (JsPath \ "ordershorttext").writeNullable[String] and
  //    (JsPath \ "activityid").write[String] and
  //    (JsPath \ "activityhref").writeNullable[String] and
  //    (JsPath \ "activityshorttext").writeNullable[String] and
  //    (JsPath \ "suborderid").writeNullable[String] and
  //    (JsPath \ "suborderhref").writeNullable[String] and
  //    (JsPath \ "subordershorttext").writeNullable[String] and
  //    (JsPath \ "comment").write[String] and
  //    // (JsPath \ "additionalcomment").writeNullable[String] and
  //    (JsPath \ "creationDate").write(writesLocalDateForCats2) and
  //    (JsPath \ "modifiedDate").write(writesLocalDateForCats2) and
  //    // (JsPath \ "extsystem").writeNullable[String] and
  //    // (JsPath \ "standarddescriptionfrontsystem").writeNullable[String] and
  //    // (JsPath \ "standarddescriptionhref").writeNullable[String] and
  //    // (JsPath \ "standarddescriptionid").writeNullable[String] and
  //    // (JsPath \ "standarddescriptionshorttext").writeNullable[String] and
  //    // (JsPath \ "trackedtimeStatus").writeNullable[String] and
  //    (JsPath \ "workingHours").write[Double]
  //  )(unlift(CatsWorklog.unapply))

  //  implicit val searchResultReads = Json.reads[CatsSearchResult]
  implicit val searchResultReads: Reads[CatsSearchResult] = (
    (JsPath \ "baseUrl").readNullable[String] and
      (JsPath \ "meta" \ "sid").read[String] and
      (JsPath \ "times").read[Seq[CatsWorklog]] and
      (JsPath \ "count").read[Int]
    ) (CatsSearchResult.apply _)

  //  implicit val searchResultWrites: Writes[CatsSearchResult] = (
  //    (JsPath \ "baseUrl").writeNullable[String] and
  //    (JsPath \ "meta" \ "sid").write[String] and
  //    (JsPath \ "times").write[Seq[CatsWorklog]] and
  //    (JsPath \ "count").write[Int]
  //  )(unlift(CatsSearchResult.unapply))
}
