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

  case class MicroShopUserBehavior(appName: String, eventType: String, createTime: Timestamp, userId: String, userName: String, shopUuid: String, productId: String, productType: String, productName: String, readFrom: String, shareTo: String, client: String)

  object StatisType extends Enumeration {
    val STATIS_SHOP = Value("statisShop")
    val STATIS_SHOP_PRODUCT = Value("statisShopProduct")
    val STATIS_SHOP_VISITOR = Value("statisShopVisitor")
  }

  def saveBackToMongo(statisShopPairRDD: RDD[(Null, BasicBSONObject)], statisShopQuery: DBObject, collection: MongoCollection, mongoConfig: Configuration): Unit = {
    collection.remove(statisShopQuery)
    statisShopPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoConfig)
  }

  //读取mongo的数据
  def readFromMongoDB(sc: SparkContext, mongoConfig: Configuration): RDD[MicroShopUserBehavior] = {
    val mongoPairRDD = sc.newAPIHadoopRDD(mongoConfig, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    //把读取的原始Mongo BSONObject数据转换 case class object 对象
    mongoPairRDD.values.map(obj => {
      val appName: String = if (obj.get("appName") != null) obj.get("appName").toString else null
      val eventType: String = if (obj.get("eventType") != null) obj.get("eventType").toString else null
      val createTime: Timestamp = if (obj.get("createTime") != null) new Timestamp(obj.get("createTime").asInstanceOf[Date].getTime()); else null
      val userId: String = if (obj.get("userId") != null) obj.get("userId").toString else null
      val userName: String = if (obj.get("userName") != null) obj.get("userName").toString else null
      val shopUuid: String = if (obj.get("shopUuid") != null) obj.get("shopUuid").toString else null
      val productId: String = if (obj.get("productId") != null) obj.get("productId").toString else null
      val productType: String = if (obj.get("productType") != null) obj.get("productType").toString else null
      val productName: String = if (obj.get("productName") != null) obj.get("productName").toString else null
      val readFrom: String = if (obj.get("readFrom") != null) obj.get("readFrom").toString else null
      val shareTo: String = if (obj.get("shareTo") != null) obj.get("shareTo").toString else null
      val client: String = if (obj.get("client") != null) obj.get("client").toString else null
      MicroShopUserBehavior(appName, eventType, createTime, userId, userName, shopUuid, productId, productType, productName, readFrom, shareTo, client)
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
    val mongodbHostPortStr = jobConfig.getString("mongodb-settings.host") + ":" + jobConfig.getInt("mongodb-settings.port")
    val mongodbInputDatabaseStr = jobConfig.getString("mongodb-settings.inputDatabase")
    val mongodbInputCollectionStr = jobConfig.getString("mongodb-settings.inputCollection")
    val mongodbOutputDatabaseStr = jobConfig.getString("mongodb-settings.outputDatabase")
    val mongodbOutputCollectionStr = jobConfig.getString("mongodb-settings.outputCollection")
    //创建sqlContext
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    import sqlc.implicits._

    //从mongoDB读取数据
    val mongoInputUriConfig = setMongoUri("mongo.input.uri", mongodbHostPortStr, mongodbInputDatabaseStr, mongodbInputCollectionStr)
    //设置查询条件
    val appName = "micro-shop"
    val mongoInputQuery: String = """{"appName":"""" + s"${appName}" +""""}"""
    mongoInputUriConfig.set("mongo.input.query", mongoInputQuery)
    val userBehaviorDF = readFromMongoDB(sc, mongoInputUriConfig).toDF
    userBehaviorDF.registerTempTable("microShopUserBehavior")
    sqlc.cacheTable("microShopUserBehavior")

    //设置shuffle数
    sqlc.sql("SET spark.sql.shuffle.partitions=20")

    val mongoClient = getMongoClient(mongodbHostPortStr)
    val mongoOutputCollection = getMongoCollection(mongoClient, mongodbOutputDatabaseStr, mongodbOutputCollectionStr)
    val mongoOutputUriConfig = setMongoUri("mongo.output.uri", mongodbHostPortStr, mongodbOutputDatabaseStr, mongodbOutputCollectionStr)

    //sql1：统计微店热度
    val statisShopSql =
      """select to_date(createTime) statisDate,
        |shopUuid,
        |count(distinct userId) uv,
        |count(*) pv,
        |'statisShop' statisType
        |from microShopUserBehavior
        |where eventType='0'
        |and userId is not null and userId <> '-1'
        |and createTime >= current_date()
        |and createTime <= current_timestamp()
        |group by to_date(createTime), shopUuid""".stripMargin.replaceAll("\n", " ")
    // 把查询的结果转换成mongo的bsonObject
    val statisShopPairRDD = sqlc.sql(statisShopSql).rdd.map({
      case Row(statisDate: java.sql.Date, shopUuid: String, uv: Long, pv: Long, statisType: String) => new BasicBSONObject().append("appName", appName).append("statisDate", statisDate).append("shopUuid", shopUuid).append("uv", uv).append("pv", pv).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopQuery = MongoDBObject(("appName", appName), ("statisType", StatisType.STATIS_SHOP.toString), ("statisDate", getSearchDate(0)))
    //把查询结果存回数据库
    saveBackToMongo(statisShopPairRDD, statisShopQuery, mongoOutputCollection, mongoOutputUriConfig)

    //sql2：统计微店产品热度
    val statisShopProductSql =
      """select shopUuid,
        |productType,
        |productId,
        |count(distinct userId) uv,
        |count(*) pv,
        |'statisShopProduct' statisType
        |from microShopUserBehavior
        |where eventType='2'
        |and productType is not null and productType <> ''
        |and productId is not null and productId <> ''
        |and userId is not null and userId <> '-1'
        |group by shopUuid, productType, productId
        |order by uv desc, pv desc""".stripMargin.replaceAll("\n", " ")
    val statisShopProductPairRDD = sqlc.sql(statisShopProductSql).rdd.map({
      case Row(shopUuid: String, productType: String, productId: String, uv: Long, pv: Long, statisType: String) => new BasicBSONObject().append("appName", appName).append("shopUuid", shopUuid).append("productType", productType).append("productId", productId).append("uv", uv).append("pv", pv).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopProductQuery = MongoDBObject(("appName", appName), ("statisType", StatisType.STATIS_SHOP_PRODUCT.toString))
    saveBackToMongo(statisShopProductPairRDD, statisShopProductQuery, mongoOutputCollection, mongoOutputUriConfig)

    //sql3：统计微店最近访客
    val statisShopVisitorSql =
      """select shopUuid,
        |userId,
        |max(createTime) visitTime,
        |'statisShopVisitor' statisType
        |from microShopUserBehavior
        |where eventType='0'
        |and userId is not null and userId <> '-1'
        |group by shopUuid, userId
        |order by max(createTime) desc""".stripMargin.replaceAll("\n", " ")
    val statisShopVisitorPairRDD = sqlc.sql(statisShopVisitorSql).rdd.map({
      case Row(shopUuid: String, userId: String, visitTime: Timestamp, statisType: String) => new BasicBSONObject().append("appName", appName).append("shopUuid", shopUuid).append("userId", userId).append("visitTime", visitTime).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopVisitorQuery = MongoDBObject(("appName", appName), ("statisType", StatisType.STATIS_SHOP_VISITOR.toString))
    saveBackToMongo(statisShopVisitorPairRDD, statisShopVisitorQuery, mongoOutputCollection, mongoOutputUriConfig)
    sqlc.clearCache()
    if (mongoClient != null) {
      mongoClient.close()
    }
    System.exit(0)
  }
}