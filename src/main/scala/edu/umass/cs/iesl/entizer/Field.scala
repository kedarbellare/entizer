package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import org.riedelcastro.nurupo.HasLogger
import collection.mutable.ArrayBuffer

/**
 * @author kedar
 */

trait Field extends HasLogger {
  /**
   * Name of the field
   */
  def name: String

  /**
   * Whether this field is a key
   */
  def isKey: Boolean

  /**
   * Possible values for aligning with an observed field. -1 means NULL or Other field.
   */
  def getPossibleValues(mentionId: ObjectId, begin: Int, end: Int): Seq[FieldValue]

  def getValuePhrase(valueId: Option[ObjectId]): Seq[String]

  def getValueMention(valueId: Option[ObjectId]): Option[ObjectId]

  def getValueMentionSegment(valueId: Option[ObjectId]): Option[MentionSegment]

  def getMentionValues(mentionId: Option[ObjectId]): Seq[FieldValue]

  /**
   * Maximum segment length allowed for the field.
   */
  var maxSegmentLength: Int = Short.MaxValue.toInt

  def useFullSegment: Boolean

  // TODO: move to processor (indicates whether to use all record segments or only those matching field name)
  def useAllRecordSegments: Boolean

  // TODO: move to processor (indicates whether to use records only or texts)
  def useOracle: Boolean

  override def toString = "%s[maxLen=%d]".format(name, maxSegmentLength)
}

case class FieldValue(field: Field, valueId: Option[ObjectId]) {
  override def toString = field.name + "[id=" + valueId + "][value='" + field.getValuePhrase(valueId).mkString(" ") + "']"
}

case class FieldValuesTextSegment(values: Seq[FieldValue], segment: TextSegment)

class FieldValuesTextSegmentation extends ArrayBuffer[FieldValuesTextSegment]