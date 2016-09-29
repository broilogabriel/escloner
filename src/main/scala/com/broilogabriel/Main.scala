package com.broilogabriel

import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders

/**
  * Created by broilogabriel on 29/09/16.
  */
object Main extends App {

  val client = Cluster.getCluster("remotecluster", "localhost", 9301)
//  val mapping = client.admin() // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-admin-indices.html
  val response = client.prepareSearch().setSearchType(SearchType.QUERY_AND_FETCH).
    setQuery(QueryBuilders.matchAllQuery()).execute().actionGet()
  println(response.toString)
}

object Cluster {

  def getCluster(clusterName: String, address: String, port: Int) = {
    val inetAddress = new InetSocketTransportAddress(address, port)
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build()
    new TransportClient(settings).addTransportAddress(inetAddress)
  }

}


