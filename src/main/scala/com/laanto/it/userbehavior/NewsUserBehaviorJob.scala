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
object NewsUserBehaviorJob {

  case class NewsUserBehavior(appName: String, eventType: String, createTime: Timestamp, userId: String, userName: String, shopUuid: String,
                              newsId: String, newsTitle: String, url: String, readFrom: String, shareTo: String, client: String, version: String)

  object StatisType extends Enumeration {
    val STATIS_TOP_NEWS = Value("statisTopNews")
  }

  def removeAndSaveBackToMongo(statisNewsPairRDD: RDD[(Null, BasicBSONObject)], statisNewsQuery: DBObject, collection: MongoCollection, mongoConfig: Configuration): Unit = {
    collection.remove(statisNewsPairRDD)
    statisNewsPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoConfig)
  }

  def saveBackToMongo(statisNewsPairRDD: RDD[(Null, BasicBSONObject)], mongoConfig: Configuration): Unit = {
    statisNewsPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoConfig)
  }

  //读取mongo的数据
  def readFromMongoDB(sc: SparkContext, mongoConfig: Configuration): RDD[NewsUserBehavior] = {
    val mongoPairRDD = sc.newAPIHadoopRDD(mongoConfig, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    //把读取的原始Mongo BSONObject数据转换 case class object 对象
    mongoPairRDD.values.map(obj => {
      val appName: String = if (obj.get("appName") != null) obj.get("appName").toString else null
      val eventType: String = if (obj.get("eventType") != null) obj.get("eventType").toString else null
      val createTime: Timestamp = if (obj.get("createTime") != null) new Timestamp(obj.get("createTime").asInstanceOf[Date].getTime) else null
      val userId: String = if (obj.get("userId") != null) obj.get("userId").toString else null
      val userName: String = if (obj.get("userName") != null) obj.get("userName").toString else null
      val shopUuid: String = if (obj.get("shopUuid") != null) obj.get("shopUuid").toString else null
      val newsId: String = if (obj.get("newsId") != null) obj.get("newsId").toString else null
      val newsTitle: String = if (obj.get("newsTitle") != null) obj.get("newsTitle").toString else null
      val url: String = if (obj.get("url") != null) obj.get("url").toString else null
      val readFrom: String = if (obj.get("readFrom") != null) obj.get("readFrom").toString else null
      val shareTo: String = if (obj.get("shareTo") != null) obj.get("shareTo").toString else null
      val client: String = if (obj.get("client") != null) obj.get("client").toString else null
      val version: String = if (obj.get("version") != null) obj.get("version").toString else null
      NewsUserBehavior(appName, eventType, createTime, userId, userName, shopUuid, newsId, newsTitle, url, readFrom, shareTo, client, version)
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
    val appName = "weixin-consultation"
    val mongoInputQuery: String = """{"appName":"""" + s"${appName}" +""""}"""
    mongoInputUriConfig.set("mongo.input.query", mongoInputQuery)
    val userBehaviorDF = readFromMongoDB(sc, mongoInputUriConfig).toDF
    userBehaviorDF.registerTempTable("weixinConsultationBehavior")
    sqlc.cacheTable("weixinConsultationBehavior")

    //设置shuffle数
    sqlc.sql("SET spark.sql.shuffle.partitions=20")

    val mongoClient = getMongoClient(mongodbHostPortStr)
    val mongoOutputCollection = getMongoCollection(mongoClient, mongodbOutputDatabaseStr, mongodbOutputCollectionStr)
    val mongoOutputUriConfig = setMongoUri("mongo.output.uri", mongodbHostPortStr, mongodbOutputDatabaseStr, mongodbOutputCollectionStr)

    //统计7日客户关注资讯热度
    val topNewsBeginTime = getSearchDate(7)
    val topNewsEndTime = getSearchDate(1)
    val statisTopNews7DaysSql =
      s"""select newsId,
          |newsTitle,
          |count(distinct userId) uv,
          |count(*) pv,
          |'statisTopNews' statisType,
          |to_date('${topNewsEndTime}') statisDate
          |from weixinConsultationBehavior
          |where eventType='1'
          |and newsId is not null
          |and newsTitle is not null
          |and userId is not nul
          |and createTime between '${topNewsBeginTime}' and '${topNewsEndTime}'
          |group by newsId, newsTitle
          |order by pv desc, uv desc""".stripMargin.replaceAll("\n", " ")
    val statisTopNews7DaysPairRDD = sqlc.sql(statisTopNews7DaysSql).rdd.map({
      case Row(newsId: String, newsTitle: String, uv: Long, pv: Long, statisType: String, statisDate: Timestamp) =>
        new BasicBSONObject().append("appName", appName).append("newsId", newsId).append("newsTitle", newsTitle).append("uv", uv).append("pv", pv).append("statisType", statisType).append("statisDate", statisDate)
    }).map(bson => (null, bson))
    val statisTopNews7DaysQuery = MongoDBObject(("appName", appName), ("statisType", StatisType.STATIS_TOP_NEWS.toString), ("statisDate", topNewsEndTime))
    val count: Long = mongoOutputCollection.count(statisTopNews7DaysQuery)
    if (count == 0) {
      saveBackToMongo(statisTopNews7DaysPairRDD, mongoOutputUriConfig)
    }
    sqlc.clearCache()
    if (mongoClient != null) {
      mongoClient.close()
    }
    System.exit(0)
  }
}