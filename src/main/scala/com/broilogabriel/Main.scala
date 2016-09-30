package com.broilogabriel

import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.joda.time.DateTime
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.indices.IndexAlreadyExistsException

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * Created by broilogabriel on 29/09/16.
  */
object Main {

  def main(args: Array[String]): Unit = {

    val dateStart = DateTime.parse(args(0))
    val dateEnd = DateTime.parse(args(1))

    println(s"a-${dateStart.getWeekyear}-${dateStart.getWeekOfWeekyear}")
    println(s"a-${dateEnd.getWeekyear}-${dateEnd.getWeekOfWeekyear}")
    val indices = getIndices(dateStart, dateEnd)


    val from = Cluster.getCluster("localcluster", "localhost", 9300)
    val to = Cluster.getCluster("remotecluster", "localhost", 9301)

    //    Cluster.cloneIndices(from, to)

    val futures = indices.toStream.map(index => {
      Future {
        val res = from.prepareSearch(index)
          .setSearchType(SearchType.QUERY_AND_FETCH)
          .setScroll(TimeValue.timeValueMinutes(5))
          .setQuery(QueryBuilders.matchAllQuery)
          .setSize(10000)
          .execute().actionGet()
        println("Done:" + res.status())
        Thread.sleep(200)
        res
      }
    })

    val f = Future.sequence(futures)
    Await.ready(f, Duration.Inf)


    from.close()
    to.close()
  }


  //  val mappings = indices.getMappings // from.admin().indices().prepareGetMappings().execute().actionGet().getMappings
  //  val response = from.prepareSearch().setSearchType(SearchType.QUERY_AND_FETCH).
  //    setQuery(QueryBuilders.matchAllQuery()).execute().actionGet()
  //  println(s"--- Response: ${response.toString}")

  //  to.admin().indices().prepareCreate()

  @tailrec
  def getIndices(startDate: DateTime, endDate: DateTime, indices: List[String] = List.empty): List[String] = {
    if (startDate.getMillis > endDate.getMillis) {
      indices
    } else {
      getIndices(startDate.plusWeeks(1), endDate, indices :+ s"a-${startDate.getWeekyear}-${startDate.getWeekOfWeekyear}")
    }
  }

}


object Cluster {

  def getCluster(clusterName: String, address: String, port: Int) = {
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build()
    new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(address, port))
  }

  def cloneIndices(from: Client, to: Client): Unit = {
    // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-admin-indices.html

    val indices = from.admin().indices().prepareGetIndex().execute().get()
    indices.getSettings.keys().asScala.foreach(indexKey => {
      val maps = from.admin().indices().prepareGetMappings(indexKey.value).execute().get()
      val newIndex = to.admin().indices().prepareCreate(indexKey.value)
        .setSettings(indices.getSettings.get(indexKey.value))
      maps.mappings().asScala.flatMap(m => {
        m.value.asScala.map(o => (o.key, o.value))
      }).foreach(mapping => {
        newIndex.addMapping(mapping._1, mapping._2.source().string())
      })
      try {
        newIndex.get()
      } catch {
        case e: IndexAlreadyExistsException => println(s">>> Index already in the server: ${indexKey.value}")
      }
    })
  }

}


