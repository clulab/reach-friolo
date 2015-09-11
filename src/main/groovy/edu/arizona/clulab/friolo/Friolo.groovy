package edu.arizona.clulab.friolo

import java.io.*

import org.apache.commons.cli.*
import org.apache.logging.log4j.*

import groovy.util.CliBuilder

/**
 * Class to aggregate and transform REACH results files, in Hans JSON format, into
 * a format, ingestable by ElasticSearch, for searching entity and event interconnections.
 *
 *   Written by: Tom Hicks. 9/10/2015.
 *   Last Modified: Initial merge of previous transformer/loader programs.
 */
class Friolo implements FilenameFilter {

  static final Logger log = LogManager.getLogger(Friolo.class.getName());

  public static final def PART_TYPES = [ 'entities', 'events', 'sentences' ]
  // public static final def FILE_TYPES = [ 'entities.json', 'events.json', 'sentences.json' ]

  public boolean VERBOSE = false


  /** Main program entry point. */
  public static void main (String[] args) {
    // read, parse, and validate command line arguments
    def usage = 'friolo [-h] [-b N] [-c clusterName] [-i indexName] [-t typeName] directory'
    def cli = new CliBuilder(usage: usage)
    cli.width = 100                         // increase usage message width
    cli.with {
      h(longOpt:  'help',      'Show usage information.')
      b(longOpt:  'bulk',
        'Use bulk loading with N additional processes (default: no bulk loading).',
        args: 1, argName: 'N')
      c(longOpt:  'cluster',  'ElasticSearch cluster name (default: reach).', args: 1)
      i(longOpt:  'index',    'ElasticSearch index name (default: results).', args: 1)
      t(longOpt:  'type',     'ElasticSearch type name (default: conn).', args: 1)
      v(longOpt:  'verbose',   'Run in verbose mode (default: non-verbose).')
    }

    def options = cli.parse(args)           // parse command line

    // validate command line arguments
    if (!options) return                    // exit out on problem
    if (options.h || options.arguments().isEmpty()) {
      cli.usage()                           // show usage and exit on help
      return                                // exit out now
    }

    // instantiate this class and validate required directory argument
    def friolo = new Friolo(options)
    File directory = friolo.goodDirPath(options.arguments()[0])
    if (!directory) return                  // problem with directory: exit out now

    // create loader with the specified settings and begin to load files
    def settings = [ 'clusterName': options.c ?: 'reach',
                     'indexName':   options.i ?: 'results',
                     'typeName':    options.t ?: 'conn',
                     'bulkLoad':    options.hasOption('b') ? true : false,
                     'bulkConcurrency': (options.b ?: 0) as Integer,
                     'bulkWaitMinutes': 1,
                     'verbose': options.v ?: false ]

    // create instance of loader class, passing it to new instance of transformer class:
    def frioLoader = new FrioLoader(settings)
    def frioFormer = new FrioFormer(settings, frioLoader)

    // transform and load the result files in the directory
    def viaBulk = (settings.bulkLoad) ? 'Bulk ' : ''
    if (options.v) {
      if (settings.bulkLoad) {
        def bC = settings.bulkConcurrency
        log.info("(Friolo.main): Bulk Processing [main + ${bC} concurrent] result files from ${directory}...")
      }
      else
        log.info("(Friolo.main): Processing result files from ${directory}...")
    }
    def procCount = friolo.processDirs(frioFormer, directory)
    frioLoader.exit()                       // cleanup elastic node
    if (options.v)
      log.info("(Friolo.main): Processed ${procCount} results.")
  }


  /** Public constructor taking a map of ingest options. */
  public Friolo (options) {
    log.trace("(Friolo.init): options=${options}")
    VERBOSE = (options.v)
  }


  /** This class implements java.io.FilenameFilter with this method. */
  boolean accept (java.io.File dir, java.lang.String filename) {
    // return FILE_TYPES.any { filename.endsWith(it) } // more selective file types
    return filename.endsWith('.json')
  }

  /** Return the document type for the given filename or null, if unable to extract it.
   *  The filename must be of the form: id.uaz.type.json OR id.type.json
   */
  def extractDocType (String fileName) {
    def parts = fileName.split('\\.')
    if (parts.size() >= 3) {
      def ftype = (parts.size() > 3) ? parts[2] : parts[1]
      return (ftype in PART_TYPES) ? ftype : null
    }
    else
      return null                           // signal failure
  }

  /** Return true if the given file is a directory, readable and, optionally, writeable. */
  def goodDirectory (File dir, writeable=false) {
    return (dir && dir.isDirectory() && dir.canRead() && (!writeable || dir.canWrite()))
  }

  /** If first argument is a path string to a readable directory return it else return null. */
  File goodDirPath (dirPath, writeable=false) {
    if (dirPath.isEmpty())                  // sanity check
      return null
    def dir = new File(dirPath)
    return (goodDirectory(dir) ? dir : null)
  }

  /** If given filename string references a readable file return the file else return null. */
  File goodFile (File directory, String filename) {
    def fyl = new File(directory, filename)
    return (fyl && fyl.isFile() && fyl.canRead()) ? fyl : null
  }

  /** Return a map of document ID to a map of document type to filename for the
      files in the given directory. [docId => [ docType => filename, ...]] */
  def mapDocsToFiles (directory) {
    def fileList = directory.list(this) as List
    def docMap = fileList.groupBy({ s -> s.substring(0,s.indexOf('.')) })
    return docMap.collectEntries { docId, docFiles ->
      def tfMap = docFiles.collectEntries { filename ->
        def docType = extractDocType(filename)
        return (docType ? [(docType):filename] : [:])
      }
      return (tfMap ? [docId, tfMap] : [:])
    }
  }


  /** Process the files in all subdirectories of the given top-level directory. */
  def processDirs (frioFormer, topDirectory) {
    int cnt = processFiles(frioFormer, topDirectory)
    topDirectory.eachDirRecurse { dir ->
      if (goodDirectory(dir)) {
        cnt += processFiles(frioFormer, dir)
      }
    }
    return cnt
  }

  /** Read, aggregate and transform the results in the named REACH result files. */
  def processFiles (frioFormer, directory) {
    log.trace("(Friolo.processFiles): xformer=${frioFormer}, dir=${directory}")
    int cnt = 0
    def docs2Files = mapDocsToFiles(directory)
    docs2Files.each { docId, tfMap ->
      def validTfMap = validateFiles(directory, docId, tfMap)
      if (validTfMap) {
        cnt += frioFormer.convert(directory, docId, validTfMap)
      }
    }
    return cnt
  }

  /** Return a new type-to-file map from the given one after validating the files against the
   *  given input directory. Null is returned on failure if any named file is not found, not
   *  a file, or not readable, or if the map does not contain the expected number of files.
   */
  def validateFiles (directory, docId, tfMap) {
    log.trace("(Friolo.validateFiles): inDir=${directory}, docId=${docId}, tfMap=${tfMap}")
    def validTfMap = tfMap.collectEntries { docType, filename ->
      if (goodFile(directory, filename))    // if file valid
        return [(docType):filename]         // collect the entry
      else {                                // else file is not valid
        if (VERBOSE) log.error("${filename} is not found, not a file, or not readable.")
        return [:]                          // so skip this entry
      }
    }

    def expected = PART_TYPES.size()        // expect a certain number of files for each doc
    if (validTfMap.size() != expected) {
      if (VERBOSE)
        log.error("${docId} does not have the expected number (${expected}) of JSON part files.")
      return null                           // failed validation: ignore this doc
    }
    return validTfMap                       // return the validated type-to-file map
  }

}
