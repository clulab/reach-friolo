package edu.arizona.clulab.friolo

import java.net.InetAddress;
import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.*;

import org.elasticsearch.client.*
// import org.elasticsearch.action.admin.indices.mapping.put.*
import org.elasticsearch.action.bulk.*
import org.elasticsearch.action.index.*
import org.elasticsearch.common.settings.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.client.transport.*

/**
 * Class to load REACH results documents in JSON format into an ElasticSearch engine.
 *   Written by: Tom Hicks. 9/10/2015.
 *   Last Modified: Redo for ES 2.3.2.
 */
class FrioLoader {

  static final Logger log = LogManager.getLogger(FrioLoader.class.getName());

  static final String ES_MAPPING_PATH = "/es-mapping.json"
  static final String ES_SETTINGS_PATH = "/es-settings.json"

  BulkProcessor bulker
  Client client
  Map settings

  /** Public constructor taking a map of ingest option. */
  public FrioLoader (Map settings) {
    log.trace("(FrioLoader.init): settings=${settings}")

    this.settings = settings                // save incoming settings in global variable:
    if (settings.verbose)
      log.info("(FrioLoader): cluster=${settings.clusterName}, index=${settings.indexName}, type=${settings.typeName}")

    def esSettings = Settings.settingsBuilder()
    esSettings.put('client.transport.sniff', false)
    esSettings.put('cluster.name', settings.get('clusterName', 'reach'))
    esSettings.put('discovery.zen.ping.unicast.hosts', 'localhost')
    esSettings.build()

    client = TransportClient.builder().settings(esSettings).build()
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))

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

    recreateIndexAndMapping(client)
  }


  /** Add the given document to the established index & type with the given ID .*/
  def addToIndex (String aDoc) {
    log.trace("(FrioLoader.addToIndex): aDoc=${aDoc}")
    IndexRequest indexReq = new IndexRequest(settings.indexName, settings.typeName).source(aDoc)
    if (!bulker) {
      return client.index(indexReq).get()
    }
    else {
      bulker.add(indexReq)
      return true
    }
  }


  /** Delete any index with the given name and recreate it.
   *  NB: THIS DELETES ALL DATA IN THE INDEX! */
  def recreateIndexAndMapping (client) {
    def indexOps = client.admin().indices()

    def indexName = settings.indexName      // for convenience only

    // try to load our custom settings from a settings config file:
    def settingsJson = readJsonConfigFile(ES_SETTINGS_PATH)

    // check for and delete index, if it exists:
    def exists = indexOps.prepareExists(indexName).execute().actionGet().isExists()
    if (exists)
      indexOps.prepareDelete(indexName).execute().actionGet()

    // re-create the index with or without additional settings:
    if (settingsJson)
      indexOps.prepareCreate(indexName).setSettings(settingsJson).execute().actionGet()
    else
      indexOps.prepareCreate(indexName).execute().actionGet()

    // load our custom mappings from a mappings config file:
    def mappingJson = readJsonConfigFile(ES_MAPPING_PATH)
    if (mappingJson) {
      // this sets our custom mapping for our specific type in the index just created above
      indexOps.preparePutMapping(indexName).setSource(mappingJson).setType(settings.typeName).execute().actionGet()
    }
  }

  /** Read the JSON ES configuration file, on the classpath, and return its text content. */
  def readJsonConfigFile (filepath) {
    def inStream = this.getClass().getResourceAsStream(filepath);
    if (inStream)
      return inStream.getText()             // read mapping text
    return null                             // signal failure to read
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
    client.close()
  }

}
