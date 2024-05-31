package kafka.security.authorizer

import DifcAuthorizer.{ResourceOrdering, TagSeqs, VersionedEffectiveSetEntries, VersionedLabel, filterEffective, getLabelFromZk}
import kafka.zk.{KafkaZkClient, ZkLabelStore, ZkVersion}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.utils.SecurityUtils

import scala.util.{Failure, Success, Try}
import scala.collection.{immutable, mutable}
import org.apache.kafka.common.acl.{AclOperation, TagBinding}
import org.apache.kafka.common.acl.AclOperation.{DESCRIBE, READ, WRITE}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.security.authorizer.{EffectiveSetEntry, LabelEntry}

object DifcAuthorizer {
  case class VersionedLabel(tags: Set[LabelEntry], zkVersion: Int) {
    def exists: Boolean = zkVersion != ZkVersion.UnknownVersion
  }

  case class VersionedEffectiveSetEntries(tags: Set[EffectiveSetEntry], zkVersion: Int) {
    def exists: Boolean = zkVersion != ZkVersion.UnknownVersion
  }

  private class TagSeqs(seqs: Seq[EffectiveSetEntry]*) {
    def find(p: EffectiveSetEntry => Boolean): Option[EffectiveSetEntry] = {
      // Lazily iterate through the inner `Seq` elements and stop as soon as we find a match
      val it = seqs.iterator.flatMap(_.find(p))
      if (it.hasNext) Some(it.next())
      else None
    }

    def isEmpty: Boolean = !seqs.exists(_.nonEmpty)
  }

  class ResourceOrdering extends Ordering[ResourcePattern] {

    def compare(a: ResourcePattern, b: ResourcePattern): Int = {
      val rt = a.resourceType.compareTo(b.resourceType)
      if (rt != 0)
        rt
      else {
        val rnt = a.patternType.compareTo(b.patternType)
        if (rnt != 0)
          rnt
        else
          (a.name compare b.name) * -1
      }
    }
  }

  def loadAllLabels(zkClient: KafkaZkClient, labelConsumer: (ResourcePattern, VersionedLabel) => Unit, efConsumer: (ResourcePattern, VersionedEffectiveSetEntries) => Unit): Unit = {
    ZkLabelStore.stores.foreach { store =>
      val resourceTypes = zkClient.getResourceTypes(store.patternType)
      for (rType <- resourceTypes) {
        val resourceType = Try(SecurityUtils.resourceType(rType))
        resourceType match {
          case Success(resourceTypeObj) =>
            val resourceNames = zkClient.getResourceNames(store.patternType, resourceTypeObj)
            for (resourceName <- resourceNames) {
              val resource = new ResourcePattern(resourceTypeObj, resourceName, store.patternType)

              val label = getLabelFromZk(zkClient, resource)
              labelConsumer.apply(resource, label)

              val effectiveSetEntries = filterEffective(label)
              efConsumer.apply(resource, effectiveSetEntries)
            }
          case Failure(_) => print(s"Ignoring unknown ResourceType: $rType")
        }
      }
    }
  }

  private def getLabelFromZk(zkClient: KafkaZkClient, resource: ResourcePattern): VersionedLabel = {
    zkClient.getVersionedLabelForTopic(resource)
  }

  private def filterEffective(versionedLabel: VersionedLabel): VersionedEffectiveSetEntries = {
    def applyFilter(tags: Set[LabelEntry]): VersionedEffectiveSetEntries = {
      val entity_set: Set[String] = tags.map(tag => tag.entity.toString)

      val labelMap = new mutable.HashMap[String, mutable.Set[String]]

      tags.map(tag => {
        val owner = tag.owner.toString
        val entity = tag.entity.toString

        if(!labelMap.contains(owner)) labelMap.put(owner, new mutable.HashSet[String])
        labelMap(owner) ++ entity
      })

      entity_set.filter(entity => {
        val currSize = labelMap.keySet.size
        val fSize = labelMap.keySet.count(owner => {
          labelMap(owner).contains(entity)
        })
        currSize == fSize
      })

      val resTags = new mutable.HashSet[EffectiveSetEntry]
      tags.map(tag => {
        if(entity_set.contains(tag.entity.toString)) resTags.add(new EffectiveSetEntry(tag.entity, tag.operation))
      })

      VersionedEffectiveSetEntries(resTags.toSet, versionedLabel.zkVersion)
    }

    val tags: Set[LabelEntry] = versionedLabel.tags

    val filteredReadTags = applyFilter(tags.filter(tag => tag.operation == AclOperation.READ))
    val filteredWriteTags = applyFilter(tags.filter(tag => tag.operation == AclOperation.WRITE))

    VersionedEffectiveSetEntries(filteredReadTags.tags ++ filteredWriteTags.tags, filteredReadTags.zkVersion)
  }
}

class DifcAuthorizer {
  @volatile
  private var efCache = new scala.collection.immutable.TreeMap[ResourcePattern, VersionedEffectiveSetEntries]()(new ResourceOrdering)
  private var labelCache = new scala.collection.immutable.TreeMap[ResourcePattern, VersionedLabel]()(new ResourceOrdering)

  // no need for the resourceCache at all
//  @volatile
//  private var resourceCache = new scala.collection.immutable.HashMap[ResourceTypeKey,
//    scala.collection.immutable.HashSet[String]]()

  def updateCache(resource: ResourcePattern, newLabel: VersionedLabel): Unit = {
//    val currentTags = labelCache(resource).tags
//    val newTags: Set[LabelEntry] = newLabel.tags
//
//    val currentEfTags = efCache(resource).tags
//    val newEfTags: Set[EffectiveSetEntry] = newEfLabel.tags

//    val tagsToAdd = newTags.diff(currentTags)
//    val tagsToRemove = currentTags.diff(newTags)

//    tagsToAdd.foreach { tag =>
//      val resourceTypeKey = ResourceTypeKey(tag, resource.resourceType(), resource.patternType())
//      resourceCache.get(resourceTypeKey) match {
//        case Some(resources) => resourceCache += (resourceTypeKey -> (resources + resource.name()))
//        case None => resourceCache += (resourceTypeKey -> immutable.HashSet(resource.name()))
//      }
//    }

//    tagsToRemove.foreach { tag =>
//      val resourceTypeKey = ResourceTypeKey(tag, resource.resourceType(), resource.patternType())
//      resourceCache.get(resourceTypeKey) match {
//        case Some(resources) =>
//          val newResources = resources - resource.name()
//          if (newResources.isEmpty) {
//            resourceCache -= resourceTypeKey
//          } else {
//            resourceCache += (resourceTypeKey -> newResources)
//          }
//        case None =>
//      }
//    }

    if (newLabel.tags.nonEmpty) {
      labelCache = labelCache.updated(resource, newLabel)
    } else {
      labelCache -= resource
    }
  }

  def updateEfCache(resource: ResourcePattern, newEfLabel: VersionedEffectiveSetEntries): Unit = {
    if (newEfLabel.tags.nonEmpty) {
      efCache = efCache.updated(resource, newEfLabel)
    } else {
      efCache -= resource
    }
  }

  def labelsAllowAccess(resource: ResourcePattern, operation: AclOperation): Boolean = {
    // we allow an operation if no acls are found and user has configured to allow all users
    // when no acls are found or if no deny acls are found and at least one allow acls matches.
    val tags = matchingLabel(resource.resourceType, resource.name)
    !tags.isEmpty
  }

  def checkTagExists(label: TagSeqs, resource: ResourcePattern, entity: KafkaPrincipal, operation: AclOperation): Boolean = {
    // Check if there are any Allow ACLs which would allow this operation.
    // Allowing read, write, delete, or alter implies allowing describe.
    // See #{org.apache.kafka.common.acl.AclOperation} for more details about ACL inheritance.
    val allowOps = operation match {
      case DESCRIBE => Set[AclOperation](READ, WRITE)
      case _ => Set[AclOperation](operation)
    }
    allowOps.exists(operation => matchingTagExists(operation, resource, entity, label))
  }

  private def matchingTagExists(operation: AclOperation,
                                resource: ResourcePattern,
                                principal: KafkaPrincipal,
                                label: TagSeqs): Boolean = {
    label.find { tag =>
      tag.operation == operation && tag.entity == principal
    }.exists { tag =>
//      authorizerLogger.debug(s"operation = $operation on resource = $resource is ALLOWED based on tag = $tag")
      true
    }
  }

  private def matchingLabel(resourceType: ResourceType, resourceName: String): TagSeqs = {
    // this code is performance sensitive, make sure to run AclAuthorizerBenchmark after any changes

    // save aclCache reference to a local val to get a consistent view of the cache during acl updates.
    val efCacheSnapshot = efCache

    val versionedLabel = efCacheSnapshot.get(new ResourcePattern(resourceType, resourceName, PatternType.LITERAL))
      .map(_.tags.toBuffer)
      .getOrElse(mutable.Buffer.empty)

    new TagSeqs(versionedLabel.toSeq)
  }

  def addTags(zkClient: KafkaZkClient, tagBindings: immutable.List[TagBinding]): Boolean = {
    // get the tags to add
    // call updateTopicLabel with the tags and the resource
    // log some output that conveys the operation is done

    val resource = tagBindings.head.pattern()

    updateTopicLabel(zkClient, resource) { currentTags: Set[LabelEntry] =>
      val tagsToAdd = tagBindings.map {tb: TagBinding => new LabelEntry(tb.entity(), tb.owner(), tb.operation())}
      currentTags ++ tagsToAdd
    }
  }

  def deleteTags(zkClient: KafkaZkClient, tagBindings: immutable.List[TagBinding]): Boolean = {
    // get the tags to delete
    // call updateTopicLabel with the tags and the resource
    // log some output that conveys the operation is done

    val resource = tagBindings.head.pattern()

    updateTopicLabel(zkClient, resource) { currentAcls: Set[LabelEntry] =>
      val tagsToDelete = tagBindings.map {tb: TagBinding => new LabelEntry(tb.entity(), tb.owner(), tb.operation())}
      currentAcls -- tagsToDelete
    }
  }

  private def updateTopicLabel(zkClient: KafkaZkClient, resource: ResourcePattern)(getNewTags: Set[LabelEntry] => Set[LabelEntry]): Boolean = {
    val currentVersionedLabel =
      if (labelCache.contains(resource))
        getLabelFromCache(resource)
      else
        getLabelFromZk(zkClient, resource)

    val newTags = getNewTags(currentVersionedLabel.tags)

    val (_, updateVersion) =
      if (currentVersionedLabel.exists) zkClient.conditionalSetLabelForResource(resource, newTags, currentVersionedLabel.zkVersion)
      else zkClient.createLabelForResourceIfNotExists(resource, newTags)

    val newVersionedLabel = VersionedLabel(newTags, updateVersion)

    if (newVersionedLabel.tags != currentVersionedLabel.tags) {
//      info(s"Updated tags for $resource with new version ${newVersionedLabel.zkVersion}")
//      debug(s"Updated tags for $resource to $newVersionedLabel")
      updateCache(resource, newVersionedLabel)
      updateEfCache(resource, filterEffective(newVersionedLabel))
      updateLabelChangedFlag(zkClient, resource)
      true
    } else {
//      debug(s"Updated tags for $resource, no change was made")
      updateCache(resource, newVersionedLabel) // Even if no change, update the version
      updateEfCache(resource, filterEffective(newVersionedLabel))
      false
    }
  }

  private def getLabelFromCache(resource: ResourcePattern): VersionedLabel = {
    labelCache.getOrElse(resource, throw new IllegalArgumentException(s"Label does not exist in the cache for resource $resource"))
  }

  private def updateLabelChangedFlag(zkClient: KafkaZkClient, resource: ResourcePattern): Unit = {
    zkClient.createLabelChangeNotification(resource)
  }

  private[authorizer] def processLabelChangeNotification(zkClient: KafkaZkClient, resource: ResourcePattern): Unit = {
    val versionedLabel = getLabelFromZk(zkClient, resource)
//    info(s"Processing label change notification for $resource, versionedAcls : ${versionedLabel.tags}, zkVersion : ${versionedLabel.zkVersion}")
    updateCache(resource, versionedLabel)
    updateEfCache(resource, filterEffective(versionedLabel))
  }

//  private object LabelChangedNotificationHandler extends LabelChangeNotificationHandler {
//    def processNotification(zkClient: KafkaZkClient, resource: ResourcePattern): Unit = {
//      processLabelChangeNotification(zkClient, resource)
//    }
//  }

//  private case class ResourceTypeKey(tag: LabelEntry,
//                                     resourceType: ResourceType,
//                                     patternType: PatternType)
}
