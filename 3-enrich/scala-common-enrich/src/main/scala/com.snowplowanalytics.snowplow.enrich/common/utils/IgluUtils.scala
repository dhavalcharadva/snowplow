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

import cats.Monad
import cats.data.{EitherT, NonEmptyList, Validated}
import cats.effect.Clock
import cats.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.client.{Client, ClientError}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.iglu.core.{ParseError, SchemaCriterion, SchemaKey, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

/** Helper to validate:
 *  - An unstructured event,
 *  - The input contexts of an event,
 *  - The contexts adveuxded by the enrichments.
 */
object IgluUtils {

  /** Extract unstructured event from event and validate against its schema
   *  @param event Snowplow enriched event to extract unstructured event from (in String)
   *  @param client Iglu client used for SDJ validation
   *  @param field Name of the field containing the unstructured event, to put in the bad row
   *               in case of failure
   *  @param criterion Expected schema for the JSON containing the unstructured event
   *  @return Valid unstructured event if the input event has one or [[None]]
   */
  def extractAndValidateUnstructEvent[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json],
    field: String = "ue_properties",
    criterion: SchemaCriterion = SchemaCriterion("com.snowplowanalytics.snowplow", "unstruct_event",
      "jsonschema", 1, 0)
  ): F[Validated[FailureDetails.SchemaViolation, Option[SelfDescribingData[Json]]]] =
    (Option(event.unstruct_event) match {
      case Some(rawUnstructEvent) =>
        for {
          // Validate input Json string and extract unstructured event
          unstruct <- extractInputData(rawUnstructEvent, field, criterion, client)
          // Parse Json unstructured event as SelfDescribingData[Json]
          unstructSDJ <- SelfDescribingData
            .parse(unstruct)
            .leftMap(e => FailureDetails.SchemaViolation.NotIglu(unstruct, e))
            .toEitherT[F]
          // Check SelfDescribingData[Json] of unstructured event
          _ <- check(client, unstructSDJ)
            .leftMap(e => FailureDetails.SchemaViolation.IgluError(e._1, e._2))
            .leftWiden[FailureDetails.SchemaViolation]
        } yield unstructSDJ.some
      case None =>
        EitherT.rightT[F, FailureDetails.SchemaViolation](none[SelfDescribingData[Json]])
    }).value
      .map(_.toValidated)

  /** Extract list of custom contexts from event and validate each against its schema
   *  @param event Snowplow enriched event to extract custom context from (in String)
   *  @param client Iglu client used for SDJ validation
   *  @param field Name of the field containing the contexts, to put in the bad row
   *               in case of failure
   *  @param criterion Expected schema for the JSON containing the contexts
   *  @return List of valid contexts if any or empty list
   */
  def extractAndValidateInputContexts[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json],
    field: String = "contexts",
    criterion: SchemaCriterion = SchemaCriterion("com.snowplowanalytics.snowplow", "contexts",
      "jsonschema", 1, 0)
  ): F[Validated[FailureDetails.SchemaViolation, List[SelfDescribingData[Json]]]] =
    (Option(event.contexts) match {
      case Some(rawContexts) =>
        for {
          // Validate input Json string and extract contexts
          contexts <- extractInputData(rawContexts, field, criterion, client)
            .map(_.asArray.get.toList) // .get OK because SDJ valid
          // Parse each Json context as a SelfDescribingData[Json]
          contextsSDJ <- parseListSDJ(contexts)
            .leftMap { errs =>
              val firstError = errs.head // TODO: update SchemaViolation to contain all errors
              FailureDetails.SchemaViolation.NotIglu(firstError._1, firstError._2)
            }
            .toEitherT[F]
          // Check each SelfDescribingData[Json] of each context
          _ <- checkList(client, contextsSDJ)
            .leftMap { errs =>
              val firstError = errs.head // TODO: update SchemaViolation to contain all errors
              FailureDetails.SchemaViolation.IgluError(firstError._1, firstError._2)
            }
            .leftWiden[FailureDetails.SchemaViolation]
        } yield contextsSDJ
      case None =>
        EitherT.rightT[F, FailureDetails.SchemaViolation](List.empty[SelfDescribingData[Json]])
    }).value
      .map(_.toValidated)

  /** Validate each context added by the enrichments against its schema
   *  @param client Iglu client used for SDJ validation
   *  @param sdjs List of enrichments contexts to be added to the enriched event
   *  @return Unit if all the contexts are valid
   */
  def validateEnrichmentsContexts[F[_]: Monad: RegistryLookup: Clock](
    client: Client[F, Json],
    sdjs: List[SelfDescribingData[Json]]
  ): F[Validated[FailureDetails.EnrichmentFailure, Unit]] =
    checkList(client, sdjs)
      .leftMap { errs =>
        val firstError = errs.head // TODO: update EnrichmentFailure to contain all errors
        val enrichmentInfo =
          FailureDetails.EnrichmentInformation(firstError._1, "enrichments-contexts-validation")
        FailureDetails.EnrichmentFailure(
          enrichmentInfo.some,
          FailureDetails.EnrichmentFailureMessage.IgluError(firstError._1, firstError._2)
        )
      }
      .value
      .map(_.toValidated)

  // Used to extract .data for input custom contexts and input unstructured event
  private def extractInputData[F[_]: Monad: RegistryLookup: Clock](
    rawJson: String,
    field: String, // to put in the bad row
    expectedCriterion: SchemaCriterion,
    client: Client[F, Json]
  ): EitherT[F, FailureDetails.SchemaViolation, Json] =
    for {
      // Parse Json string with the SDJ
      json <- JsonUtils
        .extractJson(rawJson)
        .leftMap(e => FailureDetails.SchemaViolation.NotJson(field, rawJson.some, e))
        .toEitherT[F]
      // Parse Json as SelfDescribingData[Json] (which contains the .data that we want)
      sdj <- SelfDescribingData
        .parse(json)
        .leftMap(e => FailureDetails.SchemaViolation.NotIglu(json, e))
        .toEitherT[F]
      // Check that the schema of SelfDescribingData[Json] is the expected one
      _ <- if (validateCriterion(sdj, expectedCriterion))
        EitherT.rightT[F, FailureDetails.SchemaViolation](sdj)
      else
        EitherT
          .leftT[F, SelfDescribingData[Json]](
            FailureDetails.SchemaViolation.CriterionMismatch(sdj.schema, expectedCriterion)
          )
      // Check that the SDJ holding the .data is valid
      _ <- check(client, sdj)
        .leftMap(e => FailureDetails.SchemaViolation.IgluError(e._1, e._2))
      // Extract .data of SelfDescribingData[Json]
      data <- EitherT.rightT[F, FailureDetails.SchemaViolation](sdj.data)
    } yield data

  private def validateCriterion(
    sdj: SelfDescribingData[Json],
    criterion: SchemaCriterion
  ): Boolean =
    criterion.matches(sdj.schema)

  private def check[F[_]: Monad: RegistryLookup: Clock](
    client: Client[F, Json],
    sdj: SelfDescribingData[Json]
  ): EitherT[F, (SchemaKey, ClientError), Unit] =
    client
      .check(sdj)
      .leftMap(clientErr => (sdj.schema, clientErr))

  private def checkList[F[_]: Monad: RegistryLookup: Clock](
    client: Client[F, Json],
    sdjs: List[SelfDescribingData[Json]]
  ): EitherT[F, NonEmptyList[(SchemaKey, ClientError)], Unit] = EitherT {
    sdjs
      .map(check(client, _).toValidatedNel)
      .sequence
      .map(_.sequence_.toEither)
  }

  private def parseListSDJ(
    jsons: List[Json]
  ): Either[NonEmptyList[(Json, ParseError)], List[SelfDescribingData[Json]]] =
    jsons
      .map { json =>
        SelfDescribingData
          .parse(json)
          .leftMap(err => (json, err))
          .toValidatedNel
      }
      .sequence
      .toEither
}
