package spark.utils

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.spark.sql.hive._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql._
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.spark.sql.hive._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

import twitter4j._

// 
//Vishal id: 166432707
//PS Id: 10671602
//spark-submit --master local --jars spark-streaming-twitter_2.10-1.1.0.jar,twitter4j-core-3.0.3.jar,twitter4j-stream-3.0.3.jar --class spark.utils.TwitterApp TwitterApp-0.1.jar hadoop spark bigdata hive mapreduce cloudera impala hbase
//sudo vi /etc/spark/conf/log4j.properties

object SparkStreamingTwitterAnalytics {

  //Class that needs to be registered must be outside of the main class

  case class Person(key: Int, value: String)

  def main(args: Array[String]) {

    //val filters = args
    val filters = Array("ps3", "ps4", "playstation", "sony", "vita", "psvita")
    //val filers = "ThisIsSparkStreamingFilter_100K_per_Second"

    val delimeter = "|"

    System.setProperty("twitter4j.oauth.consumerKey", "aI4UfqHyhTgQfDghD2VKjdFtT")
    System.setProperty("twitter4j.oauth.consumerSecret", "kjMkmFmjEE8qf8QyacvcdCt6XUGuNvlfHPQpDnTkhGUSLZ3zjM")
    System.setProperty("twitter4j.oauth.accessToken", "166432707-WhZ30qgh7buX0ugw7sr6T61NtFu6k3D0ufcsf6w3")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "w9VIIBZrZs9AF04FnqYq9eOeNEnZQAMBDLwJGzPT8a17S")
    System.setProperty("twitter4j.http.useSSL", "true")

    val conf = new SparkConf().setAppName("TwitterApp").setMaster("local[4]").set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val tweetStream = TwitterUtils.createStream(ssc, None, filters)

    val tweetRecords = tweetStream.map(status =>
      {

        def getValStr(x: Any): String = { if (x != null && !x.toString.isEmpty()) "|" + x.toString else "|" }
      
        var tweetRecord =
          getValStr(status.getUser().getId()) +
            getValStr(status.getUser().getScreenName()) +
            getValStr(status.getUser().getFriendsCount()) +
            getValStr(status.getUser().getFavouritesCount()) +
            getValStr(status.getUser().getFollowersCount()) +
            getValStr(status.getUser().getFriendsCount()) +
            getValStr(status.getUser().getLang()) +
            getValStr(status.getUser().getLocation()) +
            getValStr(status.getUser().getName()) +
            getValStr(status.getUser().getId()) +
            getValStr(status.getId()) +
            getValStr(status.getCreatedAt()) +
            getValStr(status.getGeoLocation()) +
            getValStr(status.getInReplyToUserId()) +
            getValStr(status.getPlace()) +
            getValStr(status.getRetweetCount()) +
            getValStr(status.getRetweetedStatus()) +
            getValStr(status.getSource()) +
            getValStr(status.getInReplyToScreenName()) +
            getValStr(status.getText())

            /*
             *  var tweetRecord =
          getValStr(status.getUser().getId().toString) +
            getValStr(status.getUser().getScreenName().toString) +
            getValStr(status.getUser().getFriendsCount().toString) +
            getValStr(status.getUser().getFavouritesCount().toString) +
            getValStr(status.getUser().getFollowersCount().toString) +
            getValStr(status.getUser().getFriendsCount().toString) +
            getValStr(status.getUser().getLang().toString) +
            getValStr(status.getUser().getLocation().toString) +
            getValStr(status.getUser().getName().toString) +
            getValStr(status.getUser().getId().toString) +
            getValStr(status.getId().toString) +
            getValStr(status.getCreatedAt().toString) +
            getValStr(status.getGeoLocation().toString) +
            getValStr(status.getInReplyToUserId().toString) +
            getValStr(status.getPlace().toString) +
            getValStr(status.getRetweetCount().toString) +
            getValStr(status.getRetweetedStatus().toString) +
            getValStr(status.getSource().toString) +
            getValStr(status.getInReplyToScreenName().toString) +
            getValStr(status.getText().toString)
             */
        tweetRecord

      })

    tweetRecords.print

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
