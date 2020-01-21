/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import org.specs2.mutable.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.client.ClientError.{ResolutionError, ValidationError}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.utils.Clock._

import com.snowplowanalytics.snowplow.badrows._

import io.circe.Json

class IgluUtilsSpec extends Specification with ValidatedMatchers {

  val notJson = "foo"
  val notIglu = """{"foo":"bar"}"""
  val unexistingSchema =
    """{"schema":"iglu:com.snowplowanalytics.snowplow/foo/jsonschema/1-0-0", "data": {}}"""
  val unstructSchema =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "unstruct_event",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
  val inputContextsSchema =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "contexts",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
  val emailSentSchema =
    SchemaKey(
      "com.acme",
      "email_sent",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
  val emailSent1 = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello@world.com",
      "emailAddress2": "foo@bar.org"
    }
  }"""
  val emailSent2 = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello2@world.com",
      "emailAddress2": "foo2@bar.org"
    }
  }"""
  val invalidEmailSent = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello@world.com"
    }
  }"""

  "extractAndValidateUnstructEvent" should {
    "return None if unstruct_event field is empty" >> {
      IgluUtils
        .extractAndValidateUnstructEvent(new EnrichedEvent, SpecHelpers.client)
        .value must beValid(None)
    }

    "return a SchemaViolation.NotJson failure if unstruct_event does not contain a properly formatted JSON string" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(notJson)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.NotJson => ok
        case err => ko(s"failure [$err] is not NotJson")
      }
    }

    "return a SchemaViolation.NotIglu failure if unstruct_event contains a properly formatted JSON string that is not self-describing" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(notIglu)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.NotIglu => ok
        case err => ko(s"failure [$err] is not NotIglu")
      }
    }

    "return a SchemaViolation.CriterionMismatch if unstruct_event contains a self-describing JSON but not with the right schema" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(unexistingSchema)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.CriterionMismatch => ok
        case err => ko(s"failure [$err] is not CriterionMismatch")
      }
    }

    "return a SchemaViolation.NotJson failure if the JSON in .data is not a JSON" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(notJson))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.NotJson => ok
        case err => ko(s"failure [$err] is not NotJson")
      }
    }

    "return a SchemaViolation.IgluError wrapping a ValidationError if the JSON in .data is not self-describing" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(notIglu))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ValidationError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return a SchemaViolation.IgluError wrapping a ValidationError if the JSON in .data is not a valid SDJ" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(invalidEmailSent))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ValidationError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return a SchemaViolation.IgluError wrapping a ResolutionError if the schema of the SDJ in .data can't be resolved" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(unexistingSchema))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ResolutionError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ResolutionError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return the extracted unstructured event if .data is a valid SDJ" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(emailSent1))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client)
        .value must beValid.like {
        case Some(sdj) if sdj.schema == emailSentSchema => ok
        case Some(sdj) =>
          ko(
            s"unstructured event's schema [${sdj.schema}] does not match expected schema [${emailSentSchema}]"
          )
        case None => ko("no unstructured event was extracted")
      }
    }
  }

  "extractAndValidateInputContexts" should {
    "return Nil if contexts field is empty" >> {
      IgluUtils
        .extractAndValidateInputContexts(new EnrichedEvent, SpecHelpers.client)
        .value must beValid(Nil)
    }

    "return a SchemaViolation.NotJson failure if .contexts does not contain a properly formatted JSON string" >> {
      val input = new EnrichedEvent
      input.setContexts(notJson)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.NotJson => ok
        case err => ko(s"failure [$err] is not NotJson")
      }
    }

    "return a SchemaViolation.NotIglu failure if .contexts contains a properly formatted JSON string that is not self-describing" >> {
      val input = new EnrichedEvent
      input.setContexts(notIglu)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.NotIglu => ok
        case err => ko(s"failure [$err] is not NotIglu")
      }
    }

    "return a SchemaViolation.CriterionMismatch if .contexts contains a self-describing JSON but not with the right schema" >> {
      val input = new EnrichedEvent
      input.setContexts(unexistingSchema)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.CriterionMismatch => ok
        case err => ko(s"failure [$err] is not CriterionMismatch")
      }
    }

    "return a SchemaViolation.IgluError wrapping a ValidationError if .data does not contain an array of JSON objects" >> {
      val input = new EnrichedEvent
      val notArrayContexts =
        s"""{"schema": "${inputContextsSchema.toSchemaUri}", "data": ${emailSent1}}"""
      input.setContexts(notArrayContexts)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ValidationError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return a SchemaViolation.IgluError wrapping a ValidationError if .data contains one invalid context" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(invalidEmailSent)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ValidationError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return a SchemaViolation.IgluError wrapping a ResolutionError if .data contains one context whose schema can't be resolved" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(unexistingSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ResolutionError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ResolutionError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return a the first failure of 2 invalid contexts" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(invalidEmailSent, unexistingSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ValidationError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }

      input.setContexts(buildInputContexts(List(unexistingSchema, invalidEmailSent)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case FailureDetails.SchemaViolation.IgluError(_, ResolutionError(_)) => ok
        case ie: FailureDetails.SchemaViolation.IgluError =>
          ko(s"failure [$ie] is IgluError but not a ResolutionError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return a failure if one context is valid and the other invalid" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(emailSent1, unexistingSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beInvalid.like {
        case _: FailureDetails.SchemaViolation.IgluError => ok
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return the extracted SDJs for 2 valid input contexts" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(emailSent1, emailSent2)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client)
        .value must beValid.like {
        case sdjs: List[SelfDescribingData[Json]] if sdjs.forall(_.schema == emailSentSchema) => ok
        case res => ko(s"[$res] are not SDJs with expected schema [${emailSentSchema.toSchemaUri}]")
      }
    }
  }

  "validateEnrichmentsContexts" should {
    "return a failure for one invalid context" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts)
        .value must beInvalid.like {
        case FailureDetails.EnrichmentFailure(
            _,
            FailureDetails.EnrichmentFailureMessage.IgluError(_, ValidationError(_))
            ) =>
          ok
        case FailureDetails
              .EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.IgluError(_, err)) =>
          ko(s"IgluError [$err] is not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return the first failure for 2 invalid contexts" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get,
        SpecHelpers.jsonStringToSDJ(unexistingSchema).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts)
        .value must beInvalid.like {
        case FailureDetails.EnrichmentFailure(
            _,
            FailureDetails.EnrichmentFailureMessage.IgluError(_, ValidationError(_))
            ) =>
          ok
        case FailureDetails
              .EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.IgluError(_, err)) =>
          ko(s"IgluError [$err] is not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }

      val contexts2 = List(
        SpecHelpers.jsonStringToSDJ(unexistingSchema).right.get,
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts2)
        .value must beInvalid.like {
        case FailureDetails.EnrichmentFailure(
            _,
            FailureDetails.EnrichmentFailureMessage.IgluError(_, ResolutionError(_))
            ) =>
          ok
        case FailureDetails
              .EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.IgluError(_, err)) =>
          ko(s"IgluError [$err] is not a ResolutionError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "return a failure for 1 valid context and one invalid" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get,
        SpecHelpers.jsonStringToSDJ(emailSent1).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts)
        .value must beInvalid.like {
        case FailureDetails.EnrichmentFailure(
            _,
            FailureDetails.EnrichmentFailureMessage.IgluError(_, ValidationError(_))
            ) =>
          ok
        case FailureDetails
              .EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.IgluError(_, err)) =>
          ko(s"IgluError [$err] is not a ValidationError")
        case err => ko(s"failure [$err] is not IgluError")
      }
    }

    "not return any error for 2 valid contexts" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(emailSent1).right.get,
        SpecHelpers.jsonStringToSDJ(emailSent2).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts)
        .value must beValid
    }
  }

  def buildUnstruct(sdj: String) =
    s"""{"schema": "${unstructSchema.toSchemaUri}", "data": $sdj}"""

  def buildInputContexts(sdjs: List[String] = List.empty[String]) =
    s"""{"schema": "${inputContextsSchema.toSchemaUri}", "data": [${sdjs.mkString(",")}]}"""
}
