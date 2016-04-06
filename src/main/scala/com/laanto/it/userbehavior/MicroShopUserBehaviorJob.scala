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
object MicroShopUserBehaviorJob {

  case class UserBehavior(appName: String, eventType: String, createTime: Timestamp, userId: String, userName: String, newsId: String, newsTitle: String, url: String, client: String, version: String, readFrom: String)

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
    val userBehaviorDF = readFromMongoDB(sc, mongoInputUriConfig).toDF
    userBehaviorDF.registerTempTable("userBehaviors")
    sqlc.cacheTable("userBehaviors")

    //设置shuffle数
    sqlc.sql("SET spark.sql.shuffle.partitions=20")

    val proMongoClient = getMongoClient(proMongodbHostPort)
    val preProMongoClient = getMongoClient(preProMongodbHostPort)
    val proOutputCollection = getMongoCollection(proMongoClient, mongodbOutputDatabase, mongodbOutputCollection)
    val preProOutputCollection = getMongoCollection(preProMongoClient, mongodbOutputDatabase, mongodbOutputCollection)
    val proMongoOutputUriConfig = setMongoUri("mongo.output.uri", proMongodbHostPort, mongodbOutputDatabase, mongodbOutputCollection)
    val preProMongoOutputUriConfig = setMongoUri("mongo.output.uri", preProMongodbHostPort, mongodbOutputDatabase, mongodbOutputCollection)

    //sql1：统计微店热度
    val statisShopSql =
      """select 'micro-shop' appName,
        |to_date(createTime) statisDate,
        |count(distinct userId) uv,
        |count(*) pv,
        |'1001' shopId,
        |'statisShop' statisType
        |from userBehaviors
        |where eventType='1'
        |and newsId is not null
        |and userId is not null and userId <> '-1'
        |and createTime >= current_date()
        |and createTime <= current_timestamp()
        |group by appName, to_date(createTime)""".stripMargin.replaceAll("\n", " ")
    // 把查询的结果转换成mongo的bsonObject
    val statisShopPairRDD = sqlc.sql(statisShopSql).rdd.map({
      case Row(appName: String, statisDate: java.sql.Date, uv: Long, pv: Long, shopId: String, statisType: String) => new BasicBSONObject().append("appName", appName).append("statisDate", statisDate).append("uv", uv).append("pv", pv).append("shopId", shopId).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopQuery = MongoDBObject(("appName", "micro-shop"), ("shopId", "1001"), ("statisType", "statisShop"), ("statisDate", getSearchDate(0)))
    //把查询结果存回数据库
    saveBackToMongo(statisShopPairRDD, statisShopQuery, proOutputCollection, proMongoOutputUriConfig)
    saveBackToMongo(statisShopPairRDD, statisShopQuery, preProOutputCollection, preProMongoOutputUriConfig)

    //sql2：统计微店产品热度
    val statisShopProductSql =
      """select 'micro-shop' appName,
        |newsId,
        |count(distinct userId) uv,
        |count(*) pv,
        |'1001' shopId,
        |'statisShopProduct' statisType
        |from userBehaviors
        |where eventType='1'
        |and newsId is not null
        |and userId is not null and userId <> '-1'
        |group by appName, newsId
        |order by uv desc, pv desc""".stripMargin.replaceAll("\n", " ")
    val statisShopProductPairRDD = sqlc.sql(statisShopProductSql).rdd.map({
      case Row(appName: String, newsId: String, uv: Long, pv: Long, shopId: String, statisType: String) => new BasicBSONObject().append("appName", appName).append("newsId", newsId).append("uv", uv).append("pv", pv).append("shopId", shopId).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopProductQuery = MongoDBObject(("appName", "micro-shop"), ("shopId", "1001"), ("statisType", "statisShopProduct"))
    saveBackToMongo(statisShopProductPairRDD, statisShopProductQuery, proOutputCollection, proMongoOutputUriConfig)
    saveBackToMongo(statisShopProductPairRDD, statisShopProductQuery, preProOutputCollection, preProMongoOutputUriConfig)

    //sql3：统计微店最近访客
    val statisShopVisitorSql =
      """select 'micro-shop' appName,
        |userId,
        |max(createTime) visitTime,
        |'1001' shopId,
        |'statisShopVisitor' statisType
        |from userBehaviors
        |where eventType='1'
        |and newsId is not null
        |and userId is not null and userId <> '-1'
        |group by appName, userId
        |order by max(createTime) desc
        |limit 100""".stripMargin.replaceAll("\n", " ")
    val statisShopVisitorPairRDD = sqlc.sql(statisShopVisitorSql).rdd.map({
      case Row(appName: String, userId: String, visitTime: Timestamp, shopId: String, statisType: String) => new BasicBSONObject().append("appName", appName).append("userId", userId).append("visitTime", visitTime).append("shopId", shopId).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopVisitorQuery = MongoDBObject(("appName", "micro-shop"), ("shopId", "1001"), ("statisType", "statisShopVisitor"))
    saveBackToMongo(statisShopVisitorPairRDD, statisShopVisitorQuery, proOutputCollection, proMongoOutputUriConfig)
    saveBackToMongo(statisShopVisitorPairRDD, statisShopVisitorQuery, preProOutputCollection, preProMongoOutputUriConfig)

    sqlc.clearCache()
    if (proMongoClient != null) {
      proMongoClient.close()
    }
    if (preProMongoClient != null) {
      preProMongoClient.close()
    }
    System.exit(0)
  }

   def saveBackToMongo(statisShopPairRDD: RDD[(Null, BasicBSONObject)], statisShopQuery: DBObject, collection: MongoCollection, mongoConfig: Configuration): Unit = {
    collection.remove(statisShopQuery)
    statisShopPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoConfig)
  }

  //读取mongo的数据
   def readFromMongoDB(sc: SparkContext, mongoConfig: Configuration): RDD[UserBehavior] = {
    val mongoPairRDD = sc.newAPIHadoopRDD(mongoConfig, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    //把读取的原始Mongo BSONObject数据转换 case class object 对象
    mongoPairRDD.values.map(obj => {
      val appName: String = if (obj.get("appName") != null) obj.get("appName").toString else null
      val eventType: String = if (obj.get("eventType") != null) obj.get("eventType").toString else null
      val createTime: Timestamp = if (obj.get("createTime") != null) new Timestamp(obj.get("createTime").asInstanceOf[Date].getTime()); else null
      val userId: String = if (obj.get("userId") != null) obj.get("userId").toString else null
      val userName: String = if (obj.get("userName") != null) obj.get("userName").toString else null
      val newsId: String = if (obj.get("newsId") != null) obj.get("newsId").toString else null
      val newsTitle: String = if (obj.get("newsTitle") != null) obj.get("newsTitle").toString else null
      val url: String = if (obj.get("url") != null) obj.get("url").toString else null
      val client: String = if (obj.get("client") != null) obj.get("client").toString else null
      val version: String = if (obj.get("version") != null) obj.get("version").toString else null
      val readFrom: String = if (obj.get("readFrom") != null) obj.get("readFrom").toString else null
      UserBehavior(appName, eventType, createTime, userId, userName, newsId, newsTitle, url, client, version, readFrom)
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

}
