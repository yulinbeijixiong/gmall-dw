package com.oujian.gmall.dw.common.uitl

import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

object MyEsUtil {
  private val ES_HOST = "http://hadoop100"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null
  def getClient:JestClient={
    if(factory==null) build()
    factory.getObject
  }
  def close(client:JestClient): Unit ={
    if(!Objects.isNull(client))
      try
      client.shutdownClient()
    catch{
      case e:Exception=>
        e.printStackTrace()
    }
  }
  private def build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST+":"+ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20).connTimeout(10000)
        .readTimeout(10000)
      .build())
  }
  def batchBulkInsert(indexName:String,iterator: Iterator[Any]): Unit ={
    val bulk: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for(itr<-iterator){
      val index = new Index.Builder(itr).build()
      bulk.addAction(index)
    }
    getClient.execute(bulk.build())
  }
}
