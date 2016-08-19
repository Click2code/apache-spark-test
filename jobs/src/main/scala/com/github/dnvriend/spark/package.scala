/*
 * Copyright 2016 Dennis Vriend
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dnvriend

import java.util.Date

import twitter4j.Status

package object spark {
  final case class Transaction(
    customer_id: Int,
    product_id: Int,
    quantity: Int,
    total_price: Double,
    purchase_time: java.sql.Timestamp
  )

  final case class PurchaseItem(
    PurchaseID: Int,
    Supplier: String,
    PurchaseType: String,
    PurchaseAmt: Double,
    PurchaseDate: java.sql.Date
  )

  final case class Tree(
    fid: String,
    gbiId: String,
    naam: String,
    latijn: String, // latin name
    soort: Option[String],
    jaar: Option[Int], // year
    ondergrond: String, // soil
    geom: String
  )

  final case class Order(
    order_id: Int,
    customer_id: Int,
    order_date: java.sql.Date
  )

  final case class Customer(
    customer_id: Int,
    customer_name: String,
    contact_name: String,
    country: String
  )

  final case class ElectionCandidate(
    txn_nm: String,
    nom_ty: String,
    state_ab: String,
    div_nm: String,
    ticket: String,
    ballot_position: String,
    surname: String,
    ballot_given_nm: String,
    party_ballot_nm: String,
    occupation: String,
    address_1: String,
    address_2: String,
    postcode: String,
    suburb: String,
    address_state_ab: String,
    contact_work_ph: String,
    contact_home_ph: String,
    postal_address_1: String,
    postal_address_2: String,
    postal_suburb: String,
    postal_postcode: String,
    contact_fax: String,
    postal_state_ab: String,
    contact_mobile_no: String,
    contact_email: String
  )

  final case class Tweet(
    createdAt: Date,
    userName: String,
    userScreenName: String,
    userDescription: String,
    text: String,
    source: String,
    inReplyToScreenName: String,
    isFavorited: Boolean,
    favoriteCount: Int,
    isRetweeted: Boolean,
    isRetweet: Boolean,
    retweetCount: Int,
    languageCode: String,
    withheldInCountries: Array[String]
  )
  object Tweet {
    def apply(status: Status): Tweet =
      Tweet(
        status.getCreatedAt,
        status.getUser.getName,
        status.getUser.getScreenName,
        status.getUser.getDescription,
        status.getText,
        status.getSource,
        status.getInReplyToScreenName,
        status.isFavorited,
        status.getFavoriteCount,
        status.isRetweeted,
        status.isRetweet,
        status.getRetweetCount,
        status.getLang,
        status.getWithheldInCountries
      )
  }
}
