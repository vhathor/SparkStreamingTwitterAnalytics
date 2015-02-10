package spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql._
import org.apache.spark.sql
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.spark.sql.hive._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.twitter._

import twitter4j.Status
import twitter4j.Status
import twitter4j.FilterQuery

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

    /*
     * 
     * // Not available in Spark 1.2 streaming library yet
    val fq: FilterQuery =  new FilterQuery()
    val followPs= Array{10671602L};    
    fq.follow(followPs);
    tweetStream.filter(fq)
    
    org.apache.spark.streaming.twitter.TwitterUtils.
    */

    val tweetRecords = tweetStream.map(status =>
      {

        var tweetRecord = status.getUser().getId().toString

        //User Info
        tweetRecord = "|" + tweetRecord + status.getUser().getScreenName()
        tweetRecord = "|" + tweetRecord + status.getUser().getFriendsCount()
        tweetRecord = "|" + tweetRecord + status.getUser().getFavouritesCount()
        tweetRecord = "|" + tweetRecord + status.getUser().getFollowersCount()
        tweetRecord = "|" + tweetRecord + status.getUser().getFriendsCount()
        tweetRecord = "|" + tweetRecord + status.getUser().getLang()
        tweetRecord = "|" + tweetRecord + status.getUser().getLocation()
        tweetRecord = "|" + tweetRecord + status.getUser().getName()
        tweetRecord = "|" + tweetRecord + status.getUser().getId()
        tweetRecord = "|" + tweetRecord + status.getId()
        tweetRecord = "|" + tweetRecord + status.getCreatedAt()
        tweetRecord = "|" + tweetRecord + status.getGeoLocation()
        tweetRecord = "|" + tweetRecord + status.getInReplyToUserId()
        tweetRecord = "|" + tweetRecord + status.getPlace()
        tweetRecord = "|" + tweetRecord + status.getRetweetCount()
        tweetRecord = "|" + tweetRecord + status.getRetweetedStatus()
        tweetRecord = "|" + tweetRecord + status.getSource()
        tweetRecord = "|" + tweetRecord + status.getInReplyToScreenName()
        tweetRecord = "|" + tweetRecord + status.getText()

        tweetRecord
      })

    tweetRecords.print

    val saveLocaitonPrefix = "hdfs://quickstart.cloudera/user/cloudera/tweets/tweets"
    val saveLocationSuffix = "data"

    tweetRecords.saveAsTextFiles(saveLocaitonPrefix, saveLocationSuffix)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
