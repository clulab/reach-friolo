package edu.arizona.clulab.friolo

import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.*;

import org.elasticsearch.client.*
import org.elasticsearch.action.bulk.*
import org.elasticsearch.action.index.*
import org.elasticsearch.node.*

/**
 * Class to load REACH results documents in JSON format into an ElasticSearch engine.
 *   Written by: Tom Hicks. 9/10/2015.
 *   Last Modified: Initial merge of previous transformer/loader programs.
 */
class FrioLoader {

  static final Logger log = LogManager.getLogger(FrioLoader.class.getName());

  BulkProcessor bulker
  Client client
  String indexName
  String typeName
  Node node
  Map settings

  /** Public constructor taking a map of ingest option. */
  public FrioLoader (Map settings) {
    log.trace("(FrioLoader.init): settings=${settings}")
    this.settings = settings
    indexName = settings.get('indexName', 'results')
    typeName = settings.get('typeName', 'conn')

    def clusterName = settings.get('clusterName', 'reach')
    node = NodeBuilder.nodeBuilder().clusterName(clusterName).client(true).node()
    client = node.client()

    // initialize bulk loading, if requested
    // by default: bulkActions=1000, bulkSize=5mb, no flushInterval, concurrentRequests=1
    if (settings.get('bulkLoad')) {
      def concurrents = settings.get('bulkConcurrency', 0)
      bulker = BulkProcessor.builder(
        client,
        new BulkProcessor.Listener() {
          @Override
          public void beforeBulk (long executionId, BulkRequest request) { }

          @Override
          public void afterBulk (long executionId,
                                 BulkRequest request,
                                 BulkResponse response) { }

          @Override
          public void afterBulk (long executionId,
                                 BulkRequest request,
                                 Throwable failure) { }
        })
      .setConcurrentRequests(concurrents)
      .build()
    }
  }


  /** Add the given document to the established index & type with the given ID .*/
  def addToIndex (String aDoc) {
    log.trace("(FrioLoader.addToIndex): aDoc=${aDoc}")
    IndexRequest indexReq = new IndexRequest(indexName, typeName).source(aDoc)
    if (!bulker) {
      return client.index(indexReq).get()
    }
    else {
      bulker.add(indexReq)
      return true
    }
  }


  /** Shutdown and terminate this insertion node. */
  void exit () {
    log.trace("(FrioLoader.exit):")
    if (bulker) {
      def concurrents = settings.get('bulkConcurrency', 0)
      def waitTime = settings.get('bulkWaitMinutes', 1)
      if (concurrents > 0) {                // if concurrent requests enabled
        if (settings.verbose)
          log.info("(FrioLoader.close): Waiting ${waitTime} minute(s) for bulk loading to finish...")
          bulker.awaitClose(waitTime, TimeUnit.MINUTES) // give other requests some time to finish
      }
      else                                  // else flush and close bulk processor
        bulker.close()
    }
    node.close()
  }

}
