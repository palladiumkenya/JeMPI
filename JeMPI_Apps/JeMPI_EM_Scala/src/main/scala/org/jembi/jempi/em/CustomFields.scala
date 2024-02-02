package org.jembi.jempi.em

import scala.collection.immutable.ArraySeq

object CustomFields {

  val FIELDS: ArraySeq[Field] = ArraySeq(
    Field("givenName", 0),
    Field("familyName", 1),
    Field("gender", 2),
    Field("dob", 3),
    Field("nupi", 4),
    Field("cccNumber", 5),
    Field("docket", 6)
  )

}

