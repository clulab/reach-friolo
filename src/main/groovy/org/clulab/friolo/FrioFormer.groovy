package org.clulab.friolo

import org.apache.logging.log4j.*
import groovy.json.*

/**
 * Class to transform and load REACH results files, in Fries Output JSON format, into a
 * format more suitable for searching entity and event interconnections via ElasticSearch.
 *   Written by: Tom Hicks. 9/10/2015.
 *   Last Modified: Fix: missing sentences in some event types.
 */
class FrioFormer {

  static final Logger log = LogManager.getLogger(FrioFormer.class.getName());

  static final List INTERESTING_TYPES =
    [ 'activation', 'complex-assembly', 'protein-modification', 'regulation', 'translocation' ]

  Map settings                              // class global settings

  // the provided instance of a class to load events into ElasticSearch
  FrioLoader LOADER

  /** Public constructor taking a map of ingest options and an ElasticSearch loader class. */
  public FrioFormer (settings, frioLoader) {
    log.trace("(FrioFormer.init): settings=${settings}, frioLoader=${frioLoader}")
    this.settings = settings                // save incoming settings in global variable
    LOADER = frioLoader
  }


  /** Transform a single doc set from the given directory to a new JSON format
      and load it into an ElasticSearch engine. */
  def convert (directory, docId, tfMap) {
    log.trace("(FrioFormer.convert): dir=${directory}, docId=${docId}, tfMap=${tfMap}")

    // read JSON files into data structures:
    def jsonSlurper = new JsonSlurper(type: JsonParserType.INDEX_OVERLAY)
    def friesMap = tfMap.collectEntries { docType, filename ->
      def fRdr = new FileReader(new File(directory, filename))
      def json = jsonSlurper.parse(fRdr)
      if (json)                             // if JSON was parsed from file
        [(docType): json]
      else [:]                              // else JSON was missing in file
    }

    // extract relevant information from the data structures:
    friesMap['sentences'] = extractSentenceTexts(friesMap)  // must be extracted before others
    friesMap['entities']  = extractEntityMentions(friesMap) // must be extracted before events
    friesMap['events']    = extractEventMentions(friesMap)

    // convert one JSON format to another and load the events:
    def cnt = 0
    convertJson(docId, friesMap).each { event ->
      def jsonDocStr = JsonOutput.toJson(event)
      if (jsonDocStr) {
        // if debugging then echo the extraction to be indexed:
        if (settings.debug) System.err.println(JsonOutput.prettyPrint(jsonDocStr))
        def status = LOADER.addToIndex(jsonDocStr)
        if (status)
          cnt += 1
      }
    }
    return cnt                              // return number of events added
  }


  /** Create and return new JSON from the given type-to-json map for the specified document. */
  def convertJson (docId, friesMap) {
    log.trace("(FrioFormer.convertJson): docId=${docId}")
    def convertedEvents = []
    friesMap.events.each { id, event ->
      def evType = event.type ?: 'UNKNOWN'
      if (evType in INTERESTING_TYPES) {    // only process certain types of events
        def newEvents = convertEvent(docId, friesMap, event)  // can be 1->N
        newEvents.each { convertedEvents << it }
      }
    }
    return convertedEvents
  }


  /** Return a shallow, tuple-like map created by processing the given event
   *  using the given document ID, map of document sentence texts, and map of document
   *  child texts (from controlled event mentions).
   */
  def convertEvent (docId, friesMap, event) {
    log.trace("(FrioFormer.convertEvent): docId=${docId}, event=${event}")

    def newEvents = []                      // list of new events created here

    // properties for the predicate portion of the output format
    def evType = event.type
    def predMap = [ 'type': evType,
                    'sign': event.sign ]
    if (event['subtype']) predMap['sub_type'] = event.subtype
    if (event['regtype']) predMap['regulation_type'] = event.regtype
    if (event['is-direct']) predMap['is-direct'] = true
    if (event['is-hypothesis']) predMap['is-hypothesis'] = true
    if (event?.rule) predMap <<  ['rule': event.rule]

    // properties for the location portion of the output format
    def locMap = [:]
    if (event?.sentence) locMap <<  ['sentence': event.sentence]

    // flatten nested events in activation or regulation
    if ((evType == 'activation') || (evType == 'regulation')) {
      def patient = getControlled(docId, friesMap, event)

      // special check: only handle regulations when controlled is a modification
      def pSubtype = (patient) ? patient?.subtype : null
      if (evType == 'regulation' && (pSubtype != 'protein-modification'))
        return newEvents                    // exit out now with no results

      // kludge: flatten the nested event using the previously modified controlled theme
      if (patient && pSubtype) {            // retrieve the controlled event subtype and
        predMap['subtype'] = pSubtype       // transfer (assign) it to the parent predicate
        patient.remove('subtype')           // delete it from the patient where it was stashed
      }

      // split multiple controllers into multiple controlling events
      def agents = getControllers(friesMap, event)
      agents.each { agent ->
        def evMap = [ 'docId': docId,
                      'loc': locMap,
                      'p': predMap,
                      'a': agent ]
        if (patient) {
          evMap['t'] = patient
          // def sites = getSites(friesMap, event)
          // if (sites) evMap['sites'] = sites
        }
        newEvents << evMap
      }
    }

    // handle complex-assembly (aka binding)
    // NB: only handles bindings w/ exactly 2 participants
    // NB: creates two events from each binary binding (i.e., A -> B and B -> A)
    else if (evType == 'complex-assembly') {
      def themes = getThemes(friesMap, event)
      // def sites = getSites(friesMap, event)
      if (themes.size() == 2) {
        def aToB = [ 'docId': docId,
                     'loc': locMap,
                     'a': themes[0],
                     'p': predMap,
                     't': themes[1] ]
        // if (sites) aToB['sites'] = sites
        newEvents << aToB
        def bToA = [ 'docId': docId,
                     'loc': locMap,
                     'a': themes[1],
                     'p': predMap,
                     't': themes[0] ]
        // if (sites) bToA['sites'] = sites
        newEvents << bToA
      }
    }

    // handle translocation - but only if it is added to INTERESTING_TYPES
    else if (evType == 'translocation') {
      def patient = getThemes(friesMap, event)?.getAt(0) // should be just 1 theme arg
      def srcArg = getSources(friesMap, event)?.getAt(0) // should be just 1 source arg
      def destArg = getDestinations(friesMap, event)?.getAt(0) // should be just 1 dest arg
      // def sites = getSites(friesMap, event)
      if (patient && destArg) {
        def evMap = [ 'docId': docId,
                      'loc': locMap,
                      'p': predMap,
                      't': patient,
                      'to_loc': destArg ]
        if (srcArg) evMap['from_loc'] = srcArg
        // if (sites) evMap['sites'] = sites
        newEvents << evMap
      }
    }

    // handle protein-modification - but only if it is added to INTERESTING_TYPES
    else if (evType == 'protein-modification') {
      def themes = getThemes(friesMap, event)
      // def sites = getSites(friesMap, event)
      if (themes) {
        // a modification event will have exactly one theme
        def evMap = [ 'docId': docId,
                      'loc': locMap,
                      'p': predMap,
                      't': themes[0] ]
        // if (sites) evMap['sites'] = sites
        newEvents << evMap
      }
    }

    return newEvents
  }


  Map extractEntityMentions (friesMap) {
    log.trace("(FrioFormer.extractEntityMentions):")
    return friesMap.entities.frames.collectEntries { frame ->
      if (frame['frame-type'] == 'entity-mention') {
        def frameId = frame['frame-id']
        def frameMap = [ 'eText': frame['text'], 'eType': frame['type'] ]
        def frameXrefs = frame['xrefs']
        if (frameXrefs) {
          def namespaceInfo = extractNamespaceInfo(frame.xrefs)
          if (namespaceInfo)
            frameMap << namespaceInfo
        }
        // def frameMods = frame['modifications']
        // if (frameMods) {
        //   def modsInfo = extractModificationInfo(frameMods)
        //   if (modsInfo)
        //     frameMap << [ 'modifications': modsInfo ]
        // }
        [ (frameId): frameMap ]
      }
      else [:]                              // else frame was not an entity mention
    }
  }

  def extractModificationInfo (modList) {
    return modList.findResults { mod ->
      if (mod) {                            // ignore (bad) empty modifications (?)
        def modMap = [ 'modification_type': mod['type'] ]
        if (mod['evidence'])  modMap << [ 'evidence': mod['evidence'] ]
        if (mod['negated'])  modMap << [ 'negated': mod['negated'] ]
        if (mod['site'])  modMap << [ 'site': mod['site'] ]
        modMap
      }
    }
  }

  def extractNamespaceInfo (xrefList) {
    return xrefList.findResults { xref ->
      if (xref)                             // ignore (bad) empty xrefs (?)
        [ 'eNs': xref['namespace'], 'eId': xref['id'] ]
    }
  }


  Map extractEventMentions (friesMap) {
    log.trace("(FrioFormer.extractEventMentions):")
    return friesMap.events.frames.collectEntries { frame ->
      def frameId = frame['frame-id']
      def frameMap = [ 'id': frameId,
                       'type': frame['type'],
                       'sign': extractSign(frame) ]  // constructed field
      if (frame['subtype']) frameMap['subtype'] = frame.subtype
      if (frame['is-direct']) frameMap['is-direct'] = true
      if (frame['is-hypothesis']) frameMap['is-hypothesis'] = true
      if (frame['found-by']) frameMap['rule'] = frame['found-by']
      def sentence = lookupSentence(friesMap, frame?.sentence)
      if (sentence) frameMap['sentence'] = sentence
      frameMap['args'] = extractArgInfo(frame.arguments)
      [ (frameId): frameMap ]
    }
  }

  def extractArgInfo (argList) {
    return argList.collect { arg ->
      def role = arg['type'] ?: arg['argument-label'] // find role: new label or old label
      def aMap = [ 'role': role,
                   'text': arg['text'],
                   'argType': arg['argument-type'] ]
      if (arg.get('arg'))                   // arguments will have a link called 'arg'
        aMap << ['link': arg['arg']]
      if (arg.get('args'))                  // or a map of labeled links called 'args'
        aMap['links'] = arg.get('args')
      return aMap
    }
  }

  /** Test for the presence of the is-negated flag, indicating that something failed to happen. */
  def extractSign (frame) {
    log.trace("(extractSign): frame=${frame}")
    return (frame && frame['is-negated']) ? 'negative' : 'positive'
  }

  /** Return a map of frameId to sentence text for all sentences in the given doc map. */
  Map extractSentenceTexts (friesMap) {
    log.trace("(FrioFormer.extractSentenceTexts):")
    return friesMap.sentences.frames.collectEntries { frame ->
      if (frame['frame-type'] == 'sentence')
        [ (frame['frame-id']): frame['text'] ]
      else [:]
    }
  }


  /** Return a list of entity maps by dereferencing the entity cross-references in
      the links (args fields) map of the given argument-type=complex map. */
  def derefComplex (friesMap, arg) {
    if (!arg) return null                   // sanity check
    def complexLinks = getComplexLinks(friesMap, arg)
    return complexLinks.findResults { link ->
      lookupEntity(friesMap, link)
    }
  }

  /** Return a list of entity maps from the arguments in the given event arguments list. */
  def derefEntities (friesMap, argsList) {
    argsList.findResults { arg ->
      lookupEntity(friesMap, getEntityLink(arg))
    }
  }

  /** Return an entity map from the given entity argument map. */
  def derefEntity (friesMap, arg) {
    if (!arg) return null                   // sanity check
    lookupEntity(friesMap, getEntityLink(arg))
  }

  /** Return an event map by dereferencing the event cross-reference in
      the link (arg field) of the given argument-type=event map. */
  def derefEvent (friesMap, arg) {
    if (!arg) return null                   // sanity check
    lookupEvent(friesMap, getEventLink(arg))
  }


  /** Return a single map of salient properties from the named arguments of the given event. */
  def getArgByRole (event, role) {
    def argsWithRoles = getArgsByRole(event, role)
    return (argsWithRoles.size() > 0) ? argsWithRoles[0] : []
  }

  /** Return a list of maps of salient properties from the named arguments of the given event. */
  def getArgsByRole (event, role) {
    return event.args.findResults { if (it?.role == role) it }
  }


  /** Check that the given argument-type entity map refers to an entity and return
      the entity cross-reference link string it contains, or else null. */
  def getEntityLink (arg) {
    if (!arg) return null                   // sanity check
    return ((arg?.argType == 'entity') && arg?.link) ? arg.link : null
  }

  /** Check that the given argument-type entity map refers to an event and return
      the event cross-reference link string it contains, or else null. */
  def getEventLink (arg) {
    if (!arg) return null                   // sanity check
    return ((arg?.argType == 'event') && arg?.link) ? arg.link : null
  }

  /** Return a controlled entity map from the controlled argument of the given event. */
  def getControlled (docId, friesMap, event) {
    log.trace("(getControlled): docId=${docId}, event=${event}")
    def ctrld = getArgByRole(event, 'controlled') // should be just 1 controller arg
    if (!ctrld) return null                       // nothing controlled: exit out now
    if (ctrld.argType == 'event') {               // if it has a controlled (nested) event
      def ctrldEvent = derefEvent(friesMap, ctrld) // get the nested event
      if (!ctrldEvent) return null                // no nested event: exit out now
      def ctrldEntity = getThemes(friesMap, ctrldEvent)?.getAt(0) // should be just 1 theme arg
      if (!ctrldEntity) return null               // no theme in nested event: exit out now
      if (ctrldEvent?.subtype)                    // stash the controlled event subtype
        ctrldEntity['subtype'] = ctrldEvent.subtype // and return it in the entity
      return ctrldEntity                          // return the nested entity
    }
    else                                    // else it is a directly controlled entity
      return derefEntity(friesMap, ctrld)
  }

  /** Return a list of maps from the controller argument of
      the given parent event. Returns null if no controller argument found. */
  def getControllers (friesMap, event) {
    log.trace("(getControllers): event=${event}")
    def cntlr = getArgByRole(event, 'controller') // should be just 1 controller arg
    if (!cntlr) return null                 // no controller: exit out now

    if (cntlr.argType == 'complex') {
      def themeLinks = getLinksByPrefix(cntlr?.links, 'theme')
      return themeLinks.findResults { link -> lookupEntity(friesMap, link) }
    }
    else                                  // else it is a single controller event
      return derefEntities(friesMap, [cntlr])
  }

  /** Return a list of entity maps from the destination arguments of the given event. */
  def getDestinations (friesMap, event) {
    log.trace("(getDestinations): event=${event}")
    def destArgs = getArgsByRole(event, 'destination')
    return derefEntities(friesMap, destArgs)
  }

  /** Return a list of mention cross references from the arguments in the given
      cross reference map whose keys begins with the given prefix string. */
  def getLinksByPrefix (linksMap, prefix) {
    if (!linksMap) return null              // sanity check
    return linksMap.findResults { linkRole, link ->
      if (linkRole.startsWith(prefix))  return link
    }
  }

  /** Return a list of entity maps from the site arguments of the given event. */
  def getSites (friesMap, event) {
    log.trace("(getSites): event=${event}")
    def siteArgs = getArgsByRole(event, 'site')
    return derefEntities(friesMap, siteArgs)
  }

  /** Return a list of entity maps from the source arguments of the given event. */
  def getSources (friesMap, event) {
    log.trace("(getSources): event=${event}")
    def srcArgs = getArgsByRole(event, 'source')
    return derefEntities(friesMap, srcArgs)
  }

  /** Return a list of entity maps from the theme arguments of the given event. */
  def getThemes (friesMap, event) {
    log.trace("(getThemes): event=${event}")
    def themeArgs = getArgsByRole(event, 'theme')
    return derefEntities(friesMap, themeArgs)
  }


  /** Return the entity map referenced by the given entity cross-reference or null. */
  def lookupEntity (friesMap, link) {
    if (!link) return null                  // sanity check
    return friesMap['entities'].get(link)
  }

  /** Return the event map referenced by the given event cross-reference or null. */
  def lookupEvent (friesMap, link) {
    if (!link) return null                  // sanity check
    return friesMap['events'].get(link)
  }

  /** Return a map of salient properties from the given entity cross-reference. */
  def lookupSentence (friesMap, sentenceLink) {
    if (!sentenceLink) return null          // propogate null
    return friesMap['sentences'].get(sentenceLink)
  }

}
