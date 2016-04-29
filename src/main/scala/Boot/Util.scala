package Boot

import play.api.libs.json._

import scalaj.http.Http

object Util {

  def convertToInfluxDBJson(serieName: String, values: Map[Long, Double]): String = {

    val json: JsValue = Json.arr(
      Json.obj(
        "name" -> serieName,
        "columns" -> Json.arr("time", "value"),
        "points" -> values.map { case (ts, value) => Json.arr(ts, value) }
      ))

    Json.prettyPrint(json)
  }

  def sendToInfluxDB(msg: String): Unit = {
    Http("http://localhost:8086/db/akkamon/series?u=root&p=root").postData(msg).header("content-type", "application/json").asString.code
  }
}
