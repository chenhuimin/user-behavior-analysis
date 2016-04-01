package com.laanto.it.userbehavior


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import com.stratio.datasource.util.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.{SparkConf, SparkContext}
import tachyon.{Constants, TachyonURI}
import tachyon.client.ClientContext
import tachyon.client.file.options.DeleteOptions
import tachyon.client.file.options.DeleteOptions.Builder
import tachyon.client.file.{TachyonFile, TachyonFileSystem}


/**
  * @author ${user.name}
  */
object MicroShopUserBehaviorJob {

  private val userBehaviorSchema = StructType(StructField("appName", StringType, true) :: StructField("eventType", StringType, true) :: StructField("createTime", TimestampType, true) :: StructField("userId", StringType, true) :: StructField("userName", StringType, true) :: StructField("newsId", StringType, true) :: StructField("newsTitle", StringType, true) :: StructField("url", StringType, true) :: StructField("client", StringType, true) :: StructField("version", StringType, true) :: StructField("readFrom", StringType, true) :: Nil)


  def main(args: Array[String]) {
    //读取配置文件
    val jobConfig = ConfigFactory.load("application.conf")
    val proMongodbHostPort = jobConfig.getString("mongodb-settings.proHost") + ":" + jobConfig.getInt("mongodb-settings.proPort")
    val preProMongodbHostPort = jobConfig.getString("mongodb-settings.preProHost") + ":" + jobConfig.getInt("mongodb-settings.preProPort")
    val mongodbInputDatabase = jobConfig.getString("mongodb-settings.inputDatabase")
    val mongodbInputCollection = jobConfig.getString("mongodb-settings.inputCollection")
    val mongodbOutputDatabase = jobConfig.getString("mongodb-settings.outputDatabase")
    val mongodbOutputCollection = jobConfig.getString("mongodb-settings.outputCollection")
    //    val tachyonHost = "tachyon://" + jobConfig.getString("tachyon-settings.host") + ":" + jobConfig.getInt("tachyon-settings.port")
    //    val tachyonFileBasePath = jobConfig.getString("tachyon-settings.basePath")
    //创建sqlContext
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    val currentDate = getSearchDate(0)
    //    val tachyonFile = tachyonFileBasePath + s"${formatDate(currentDate)}" + ".parquet"
    //    val tfSystem = getTFSystem(tachyonHost)
    //从mongoDB读取数据
    val proReadConfig = MongodbConfigBuilder(Map(Host -> List(proMongodbHostPort), Database -> mongodbInputDatabase, Collection -> mongodbInputCollection, SamplingRatio -> 0.1, CursorBatchSize -> 1000)).build()
    sqlc.fromMongoDB(proReadConfig, Some(userBehaviorSchema)).registerTempTable("userBehaviors")
    sqlc.cacheTable("userBehaviors")
    //sqlc.fromMongoDB(proReadConfig, Some(userBehaviorSchema)).write.parquet(tachyonHost + tachyonFile)
    // userBehaviorsDF.where(userBehaviorsDF("createTime") > new java.sql.Timestamp(getSearchDate(0).getTime)).registerTempTable("userBehaviors")
    //sqlc.read.parquet(tachyonHost + tachyonFile).registerTempTable("userBehaviors")

    //设置shuffle数
    sqlc.sql("SET spark.sql.shuffle.partitions=20")

    val proMongoClient = getMongoClient(proMongodbHostPort)
    val preProMongoClient = getMongoClient(preProMongodbHostPort)
    val proOutputCollection = getMongoCollection(proMongoClient, mongodbOutputDatabase, mongodbOutputCollection)
    val preProOutputCollection = getMongoCollection(preProMongoClient, mongodbOutputDatabase, mongodbOutputCollection)
    val proSaveConfig = MongodbConfigBuilder(Map(Host -> List(proMongodbHostPort), Database -> mongodbOutputDatabase, Collection -> mongodbOutputCollection, SamplingRatio -> 0.1, WriteConcern -> "normal", CursorBatchSize -> 1000)).build()
    val preProSaveConfig = MongodbConfigBuilder(Map(Host -> List(preProMongodbHostPort), Database -> mongodbOutputDatabase, Collection -> mongodbOutputCollection, SamplingRatio -> 0.1, WriteConcern -> "normal", CursorBatchSize -> 1000)).build()

    //sql1：统计微店热度
    val statisShopSql =
      """select appName,
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

    val statisShopQuery = MongoDBObject(("appName", "weixin-consultation"), ("shopId", "1001"), ("statisType", "statisShop"), ("statisDate", getSearchDate(0)))
    //save to pro
    executeSqlAndSave(sqlc, statisShopSql, proOutputCollection, statisShopQuery, proSaveConfig)
    //save to pre-pro
    executeSqlAndSave(sqlc, statisShopSql, preProOutputCollection, statisShopQuery, preProSaveConfig)

    //sql2：统计微店产品热度
    val statisShopProductSql =
      """select appName,
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

    val statisShopProductQuery = MongoDBObject(("appName", "weixin-consultation"), ("shopId", "1001"), ("statisType", "statisShopProduct"))
    //save to pro
    executeSqlAndSave(sqlc, statisShopProductSql, proOutputCollection, statisShopProductQuery, proSaveConfig)
    //save to pre-pro
    executeSqlAndSave(sqlc, statisShopProductSql, preProOutputCollection, statisShopProductQuery, preProSaveConfig)

    //sql3：统计微店最近访客
    val statisShopVisitorSql =
      """select
        |appName,
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

    val statisShopVisitorQuery = MongoDBObject(("appName", "weixin-consultation"), ("shopId", "1001"), ("statisType", "statisShopVisitor"))
    //save to pro
    executeSqlAndSave(sqlc, statisShopVisitorSql, proOutputCollection, statisShopVisitorQuery, proSaveConfig)
    //save to pre-pro
    executeSqlAndSave(sqlc, statisShopVisitorSql, preProOutputCollection, statisShopVisitorQuery, preProSaveConfig)
    sqlc.clearCache()

    if (proMongoClient != null) {
      proMongoClient.close()
    }
    if (preProMongoClient != null) {
      preProMongoClient.close()
    }
    //    if (ifExists(tfSystem, tachyonFile)) {
    //      deleteFile(tfSystem, tachyonFile)
    //    }
    //sc.stop()
    System.exit(0)
  }


  private def executeSqlAndSave(sqlc: SQLContext, sql: String, outputCollection: MongoCollection, query: DBObject, mongoConfig: Config): Unit = {
    outputCollection.remove(query)
    sqlc.sql(sql).saveToMongodb(mongoConfig)
  }

  private def getMongoClient(mongodbHostPort: String): MongoClient = {
    MongoClient(MongoClientURI("mongodb://" + mongodbHostPort))
  }

  private def getMongoCollection(mongoClient: MongoClient, mongodbDatabase: String, mongodbCollection: String): MongoCollection = {
    val db = mongoClient(mongodbDatabase)
    db(mongodbCollection)
  }

  private def formatDate(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(date)
  }

  //获取结束查询时间
  private def getSearchDate(addDays: Int): Date = {
    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.DAY_OF_MONTH, addDays)
    cal.getTime()
  }

  private def ifExists(tachyonFileSystem: TachyonFileSystem, file: String): Boolean = {
    val dataFileURI = new TachyonURI(file)
    val dataFile: Option[TachyonFile] = Option(tachyonFileSystem.openIfExists(dataFileURI))
    if (dataFile.isDefined) true else false
  }

  private def deleteFile(tachyonFileSystem: TachyonFileSystem, file: String): Unit = {
    val dataFile: Option[TachyonFile] = Option(tachyonFileSystem.openIfExists(new TachyonURI(file)))
    val deleteOptions: DeleteOptions = new Builder(ClientContext.getConf).setRecursive(true).build
    tachyonFileSystem.delete(dataFile.get, deleteOptions)
  }

  private def getTFSystem(tachyonMaster: String): TachyonFileSystem = {
    val mMasterLocation = new TachyonURI(tachyonMaster)
    val mTachyonConf = ClientContext.getConf()
    mTachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost)
    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort))
    mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, "false")
    ClientContext.reset(mTachyonConf)
    val tachyonFileSystem: TachyonFileSystem = TachyonFileSystem.TachyonFileSystemFactory.get()
    tachyonFileSystem
  }
}
