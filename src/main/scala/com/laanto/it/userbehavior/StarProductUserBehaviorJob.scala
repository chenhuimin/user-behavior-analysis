package com.laanto.it.userbehavior

import java.sql.Timestamp
import java.util.{Calendar, Date}

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}


/**
  * @author ${user.name}
  */
object StarProductUserBehaviorJob {

  case class StarProductBehavior(appName: String, eventType: String, createTime: Timestamp, mobile: String, policyPlanId: String, policyPlanName: String, companyName: String, client: String, version: String, readFrom: String)

  def saveBackToMongo(statisPairRDD: RDD[(Null, BasicBSONObject)], query: DBObject, collection: MongoCollection, mongoConfig: Configuration): Unit = {
    collection.remove(query)
    statisPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoConfig)
  }

  //读取mongo的数据
  def readFromMongoDB(sc: SparkContext, mongoConfig: Configuration): RDD[StarProductBehavior] = {
    val mongoPairRDD = sc.newAPIHadoopRDD(mongoConfig, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    //把读取的原始Mongo BSONObject数据转换 case class object 对象
    mongoPairRDD.values.map(obj => {
      val appName: String = if (obj.get("appName") != null) obj.get("appName").toString else null
      val eventType: String = if (obj.get("eventType") != null) obj.get("eventType").toString else null
      val createTime: Timestamp = if (obj.get("createTime") != null) new Timestamp(obj.get("createTime").asInstanceOf[Date].getTime()); else null
      val mobile: String = if (obj.get("mobile") != null) obj.get("mobile").toString else null
      val policyPlanId: String = if (obj.get("policyPlanId") != null) obj.get("policyPlanId").toString else null
      val policyPlanName: String = if (obj.get("policyPlanName") != null) obj.get("policyPlanName").toString else null
      val companyName: String = if (obj.get("companyName") != null) obj.get("companyName").toString else null
      val client: String = if (obj.get("client") != null) obj.get("client").toString else null
      val version: String = if (obj.get("version") != null) obj.get("version").toString else null
      val readFrom: String = if (obj.get("readFrom") != null) obj.get("readFrom").toString else null
      StarProductBehavior(appName, eventType, createTime, mobile, policyPlanId, policyPlanName, companyName, client, version, readFrom)
    })
  }

  def getMongoClient(mongodbHostPort: String): MongoClient = {
    MongoClient(MongoClientURI("mongodb://" + mongodbHostPort))
  }

  def getMongoCollection(mongoClient: MongoClient, mongodbDatabase: String, mongodbCollection: String): MongoCollection = {
    val db = mongoClient(mongodbDatabase)
    db(mongodbCollection)
  }

  //设置mongo数据连接地址
  def setMongoUri(key: String, mongodbHostPort: String, mongodbDatabase: String, mongodbCollection: String): Configuration = {
    val mongoConfig = new Configuration()
    mongoConfig.set(key, "mongodb://" + mongodbHostPort + "/" + mongodbDatabase + "." + mongodbCollection)
    mongoConfig
  }

  //获取结束查询时间
  def getSearchDate(addDays: Int): Date = {
    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.DAY_OF_MONTH, addDays)
    cal.getTime()
  }

  def main(args: Array[String]) {
    //读取配置文件
    val jobConfig = ConfigFactory.load("application.conf")
    val proMongodbHostPort = jobConfig.getString("mongodb-settings.proHost") + ":" + jobConfig.getInt("mongodb-settings.proPort")
    val preProMongodbHostPort = jobConfig.getString("mongodb-settings.preProHost") + ":" + jobConfig.getInt("mongodb-settings.preProPort")
    val mongodbInputDatabase = jobConfig.getString("mongodb-settings.inputDatabase")
    val mongodbInputCollection = jobConfig.getString("mongodb-settings.inputCollection")
    val mongodbOutputDatabase = jobConfig.getString("mongodb-settings.outputDatabase")
    val mongodbOutputCollection = jobConfig.getString("mongodb-settings.outputCollection")
    //创建sqlContext
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    import sqlc.implicits._

    //从mongoDB读取数据
    val mongoInputUriConfig = setMongoUri("mongo.input.uri", proMongodbHostPort, mongodbInputDatabase, mongodbInputCollection)
    //设置查询条件
    val appName = "star-product"
    val mongoInputQuery: String = """{"appName":"""" + s"${appName}" +""""}"""
    mongoInputUriConfig.set("mongo.input.query", mongoInputQuery)
    val starProductBehaviorDF = readFromMongoDB(sc, mongoInputUriConfig).toDF
    starProductBehaviorDF.registerTempTable("starProductBehavior")
    sqlc.cacheTable("starProductBehavior")

    //设置shuffle数
    sqlc.sql("SET spark.sql.shuffle.partitions=20")

    //    val proMongoClient = getMongoClient(proMongodbHostPort)
    //    val preProMongoClient = getMongoClient(preProMongodbHostPort)
    //    val proOutputCollection = getMongoCollection(proMongoClient, mongodbOutputDatabase, mongodbOutputCollection)
    //    val preProOutputCollection = getMongoCollection(preProMongoClient, mongodbOutputDatabase, mongodbOutputCollection)
    //    val proMongoOutputUriConfig = setMongoUri("mongo.output.uri", proMongodbHostPort, mongodbOutputDatabase, mongodbOutputCollection)
    //    val preProMongoOutputUriConfig = setMongoUri("mongo.output.uri", preProMongodbHostPort, mongodbOutputDatabase, mongodbOutputCollection)
    //
    //    sqlc.clearCache()
    //    if (proMongoClient != null) {
    //      proMongoClient.close()
    //    }
    //    if (preProMongoClient != null) {
    //      preProMongoClient.close()
    //    }
    //    System.exit(0)
  }


}
