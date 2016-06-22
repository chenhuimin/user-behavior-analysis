package com.laanto.it.userbehavior


import java.sql.Timestamp
import java.text.SimpleDateFormat
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

  case class MicroShopUserBehavior(appName: String, eventType: String, createTime: Timestamp, userId: String, userName: String, shopUuid: String,
                                   productId: String, productType: String, productName: String, companyId: String, url: String, referrer: String, readFrom: String, shareTo: String, client: String)

  object StatisType extends Enumeration {
    val STATIS_SHOP = Value("statisShop")
    val STATIS_SHOP_PRODUCT = Value("statisShopProduct")
    val STATIS_SHOP_VISITOR = Value("statisShopVisitor")
    val STATIS_TOP_PRODUCT = Value("statisTopProduct")
    val STATIS_SHOP_PRODUCT_VISITOR = Value("statisShopProductVisitor")
  }

  def removeAndSaveBackToMongo(statisShopPairRDD: RDD[(Null, BasicBSONObject)], statisShopQuery: DBObject, collection: MongoCollection, mongoConfig: Configuration): Unit = {
    collection.remove(statisShopQuery)
    statisShopPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoConfig)
  }

  def saveBackToMongo(statisShopPairRDD: RDD[(Null, BasicBSONObject)], mongoConfig: Configuration): Unit = {
    statisShopPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoConfig)
  }

  //读取mongo的数据
  def readFromMongoDB(sc: SparkContext, mongoConfig: Configuration): RDD[MicroShopUserBehavior] = {
    val mongoPairRDD = sc.newAPIHadoopRDD(mongoConfig, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    //把读取的原始Mongo BSONObject数据转换 case class object 对象
    mongoPairRDD.values.map(obj => {
      val appName: String = if (obj.get("appName") != null) obj.get("appName").toString else null
      val eventType: String = if (obj.get("eventType") != null) obj.get("eventType").toString else null
      val createTime: Timestamp = if (obj.get("createTime") != null) new Timestamp(obj.get("createTime").asInstanceOf[Date].getTime) else null
      val userId: String = if (obj.get("userId") != null) obj.get("userId").toString else null
      val userName: String = if (obj.get("userName") != null) obj.get("userName").toString else null
      val shopUuid: String = if (obj.get("shopUuid") != null) obj.get("shopUuid").toString else null
      val productId: String = if (obj.get("productId") != null) obj.get("productId").toString else null
      val productType: String = if (obj.get("productType") != null) obj.get("productType").toString else null
      val productName: String = if (obj.get("productName") != null) obj.get("productName").toString else null
      val companyId: String = if (obj.get("companyId") != null) obj.get("companyId").toString else null
      val url: String = if (obj.get("url") != null) obj.get("url").toString else null
      val referrer: String = if (obj.get("referrer") != null) obj.get("referrer").toString else null
      val readFrom: String = if (obj.get("readFrom") != null) obj.get("readFrom").toString else null
      val shareTo: String = if (obj.get("shareTo") != null) obj.get("shareTo").toString else null
      val client: String = if (obj.get("client") != null) obj.get("client").toString else null
      MicroShopUserBehavior(appName, eventType, createTime, userId, userName, shopUuid, productId, productType, productName, companyId, url, referrer, readFrom, shareTo, client)
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
    cal.getTime
  }

  def getBeginTime(executionInterval: Int): String = {
    var cal: Calendar = Calendar.getInstance()
    //放宽取值时间5分钟
    cal.add(Calendar.MINUTE, -(executionInterval + 2))
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime)
  }

  def main(args: Array[String]) {
    //读取配置文件
    val jobConfig = ConfigFactory.load("application.conf")
    val mongodbHostPortStr = jobConfig.getString("mongodb-settings.host") + ":" + jobConfig.getInt("mongodb-settings.port")
    val mongodbInputDatabaseStr = jobConfig.getString("mongodb-settings.inputDatabase")
    val mongodbInputCollectionStr = jobConfig.getString("mongodb-settings.inputCollection")
    val mongodbOutputDatabaseStr = jobConfig.getString("mongodb-settings.outputDatabase")
    val mongodbOutputCollectionStr = jobConfig.getString("mongodb-settings.outputCollection")
    val executionInterval = jobConfig.getInt("job-settings.executionInterval")
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

    //统计微店热度
    val statisShopSql =
      """select to_date(createTime) statisDate,
        |shopUuid,
        |count(distinct userId) uv,
        |count(*) pv,
        |'statisShop' statisType
        |from microShopUserBehavior
        |where eventType = '0'
        |and shopUuid is not null and shopUuid <> ''
        |and userId is not null and userId <> ''
        |and createTime >= current_date()
        |and createTime <= current_timestamp()
        |and client = '0'
        |group by to_date(createTime), shopUuid""".stripMargin.replaceAll("\n", " ")
    // 把查询的结果转换成mongo的bsonObject
    val statisShopPairRDD = sqlc.sql(statisShopSql).rdd.map({
      case Row(statisDate: java.sql.Date, shopUuid: String, uv: Long, pv: Long, statisType: String) => new BasicBSONObject().append("appName", appName).append("statisDate", statisDate).append("shopUuid", shopUuid).append("uv", uv).append("pv", pv).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopQuery = MongoDBObject(("appName", appName), ("statisType", StatisType.STATIS_SHOP.toString), ("statisDate", getSearchDate(0)))
    //把查询结果存回数据库
    removeAndSaveBackToMongo(statisShopPairRDD, statisShopQuery, mongoOutputCollection, mongoOutputUriConfig)

    //统计微店产品热度
    val statisShopProductSql =
      """select shopUuid,
        |productType,
        |productId,
        |count(distinct userId) uv,
        |count(*) pv,
        |'statisShopProduct' statisType
        |from microShopUserBehavior
        |where eventType = '2'
        |and shopUuid is not null and shopUuid <> ''
        |and productType is not null and productType <> ''
        |and productId is not null and productId <> ''
        |and userId is not null and userId <> ''
        |and client = '0'
        |group by shopUuid, productType, productId
        |order by uv desc, pv desc""".stripMargin.replaceAll("\n", " ")
    val statisShopProductPairRDD = sqlc.sql(statisShopProductSql).rdd.map({
      case Row(shopUuid: String, productType: String, productId: String, uv: Long, pv: Long, statisType: String) => new BasicBSONObject().append("appName", appName).append("shopUuid", shopUuid).append("productType", productType).append("productId", productId).append("uv", uv).append("pv", pv).append("statisType", statisType)
    }).map(bson => (null, bson))
    val statisShopProductQuery = MongoDBObject(("appName", appName), ("statisType", StatisType.STATIS_SHOP_PRODUCT.toString))

    removeAndSaveBackToMongo(statisShopProductPairRDD, statisShopProductQuery, mongoOutputCollection, mongoOutputUriConfig)

    //统计微店最近访客
    val shopVisitorBeginTime: String = getBeginTime(executionInterval: Int)
    val statisShopVisitorSql =
      s"""select shopUuid,
          |userId,
          |max(createTime) visitTime,
          |'statisShopVisitor' statisType
          |from microShopUserBehavior
          |where eventType = '0'
          |and shopUuid is not null and shopUuid <> ''
          |and userId is not null and userId <> '' and userId <> '-1'
          |and createTime between '${shopVisitorBeginTime}' and current_timestamp()
          |and client = '0'
          |group by shopUuid, userId
          |order by max(createTime) desc""".stripMargin.replaceAll("\n", " ")
    val statisShopVisitorPairRDD = sqlc.sql(statisShopVisitorSql).rdd.map({
      case Row(shopUuid: String, userId: String, visitTime: Timestamp, statisType: String) =>
        //remove the old record from mongodb
        val mongoClient = MongoClient(MongoClientURI("mongodb://" + mongodbHostPortStr))
        val db = mongoClient(mongodbOutputDatabaseStr)
        val mongoOutputCollection = db(mongodbOutputCollectionStr)
        mongoOutputCollection.remove(MongoDBObject(("appName", appName), ("shopUuid", shopUuid), ("userId", userId), ("statisType", statisType)))
        new BasicBSONObject().append("appName", appName).append("shopUuid", shopUuid).append("userId", userId).append("visitTime", visitTime).append("statisType", statisType)
    }).map(bson => (null, bson))
    saveBackToMongo(statisShopVisitorPairRDD, mongoOutputUriConfig)

    //统计7日客户关注产品热度
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val topProductBeginTime = sdf.format(getSearchDate(-7))
    val topProductEndTime = sdf.format(getSearchDate(0))
    val topProductStatisDate = getSearchDate(-1)
    val statisTopProduct7DaysSql =
      s"""select companyId,
          |productId,
          |productName,
          |count(distinct userId) uv,
          |count(*) pv,
          |'statisTopProduct' statisType
          |from microShopUserBehavior
          |where eventType = '2'
          |and companyId is not null and companyId <> ''
          |and productId is not null and productId <> ''
          |and productName is not null and productName <> ''
          |and userId is not null and userId <> ''
          |and createTime >= '${topProductBeginTime}' and createTime < '${topProductEndTime}'
          |group by companyId, productId, productName
          |order by pv desc, uv desc""".stripMargin.replaceAll("\n", " ")
    val statisTopProduct7DaysPairRDD = sqlc.sql(statisTopProduct7DaysSql).rdd.map({
      case Row(companyId: String, productId: String, productName: String, uv: Long, pv: Long, statisType: String) =>
        new BasicBSONObject().append("appName", appName).append("companyId", companyId).append("productId", productId).append("productName", productName).append("uv", uv).append("pv", pv).append("statisType", statisType).append("statisDate", topProductStatisDate)
    }).map(bson => (null, bson))
    val statisTopProduct7DaysQuery = MongoDBObject(("appName", appName), ("statisType", StatisType.STATIS_TOP_PRODUCT.toString), ("statisDate", topProductStatisDate))
    val count: Long = mongoOutputCollection.count(statisTopProduct7DaysQuery)
    if (count == 0) {
      saveBackToMongo(statisTopProduct7DaysPairRDD, mongoOutputUriConfig)
    }

    //统计产品最近访客
    val productVisitorBeginTime: String = getBeginTime(executionInterval: Int)
    val statisShopProductVisitorSql =
      s"""select shopUuid,
          |productId,
          |userId,
          |max(createTime) visitTime,
          |'statisShopProductVisitor' statisType
          |from microShopUserBehavior
          |where eventType = '2'
          |and createTime between '${productVisitorBeginTime}' and current_timestamp()
          |and shopUuid is not null and shopUuid <> ''
          |and productId is not null and productId <> ''
          |and userId is not null and userId <> '' and userId <> '-1'
          |and client = '0'
          |group by shopUuid, productId, userId
          |order by max(createTime) desc""".stripMargin.replaceAll("\n", " ")
    val statisShopProductVisitorPairRDD = sqlc.sql(statisShopProductVisitorSql).rdd.map({
      case Row(shopUuid: String, productId: String, userId: String, visitTime: Timestamp, statisType: String) =>
        //remove the old record from mongodb
        val mongoClient = MongoClient(MongoClientURI("mongodb://" + mongodbHostPortStr))
        val db = mongoClient(mongodbOutputDatabaseStr)
        val mongoOutputCollection = db(mongodbOutputCollectionStr)
        mongoOutputCollection.remove(MongoDBObject(("appName", appName), ("shopUuid", shopUuid), ("productId", productId), ("userId", userId), ("statisType", statisType)))
        new BasicBSONObject().append("appName", appName).append("shopUuid", shopUuid).append("productId", productId).append("userId", userId).append("visitTime", visitTime).append("statisType", statisType)
    }).map(bson => (null, bson))
    saveBackToMongo(statisShopProductVisitorPairRDD, mongoOutputUriConfig)

    sqlc.clearCache()
    if (mongoClient != null) {
      mongoClient.close()
    }
    System.exit(0)
  }
}