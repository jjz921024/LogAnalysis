import java.io.{File, FileInputStream}
import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.LogWriter


/**
  * Created by Jun on 2017/8/9.
  */
object LogAnalysisStreaming {
    def main(args: Array[String]) {
        //System.setProperty("hadoop.home.dir", "D:/hadoop-2.7.3/hadoop-2.7.3")
        PropertyConfigurator.configure("conf/log4j.properties")

        val conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("LogAnalysisStreaming")
        //val sc = new SparkContext(conf)

        /*val winLog = sc.hadoopFile("E:\\guess_passwd", classOf[TextInputFormat],
            classOf[LongWritable], classOf[Text]).map(
            pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))*/

        val ssc = new StreamingContext(conf, Seconds(1))
        //val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 5142)

        //kafka配置
        val props: Properties = new Properties
        //val in: InputStream = this.getClass.getClassLoader.getResourceAsStream("conf/kafka.properties")
        val in: FileInputStream = new FileInputStream(new File("conf/kafka.properties"))

        props.load(in)
        val kafkaParams = Map[String, String](
            "metadata.broker.list" -> props.getProperty("bootstrap.servers"),
            //"key.deserializer" -> classOf[StringDeserializer],
            //"value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> props.getProperty("group.id"),
            "auto.offset.reset" -> props.getProperty("auto.offset.reset"),
            "enable.auto.commit" -> "true"
        )
        val topics = props.getProperty("topics").split(" ").toSet
        //val kafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))  //2.0.2
        val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)  //1.6.0

        // Master节点维护的hashmap
        val chm = new ConcurrentHashMap[String, GuessPasswdBean]()

        // 正则
        val logRegex =
            """([\d\.]+)##.*?(\w{3} \w{3} \d{2} [\d:]{8} \d{4}).*?\s+(\d{3})\sSecurity.*?(?:.*?:\s){3}(\w+)(?:.*?:\s+)([\w-]+).*?(?:.*?:\s+)(\d+).*?(?:.*?:\s+){6}([\(\w,\)]+).*?(?:.*?:\s+){3}([\d\.]+)""".r
        val logSuccessRegex = """([\d\.]+)##.*?(\w{3} \w{3} \d{2} [\d:]{8} \d{4}).*?\s+(\d{3})\s+Security.*?(?:.*?:\s+){5}([\(\w,\)]+)(?:.*?:\s+){9}([\d+\.\d+\.\d+\.\d+-]+)""".r
        val eventIDRegex = """\d{4}\s(\d{3})\sSecurity""".r

        //sql
        val sql: String = "insert into sysrisktable (sourceIP, destinationIP, startTime, endTime, sysType, riskType, times, eventDesc) values (?,?,?,?,?,?,?,?)"

        // 登录失败的事件id
        val loginSet: Set[Int] = Set(529,530,531,532,533,534,535,536,537,539, 552)
        val LIMIT_TIMES = 2



        // 过滤属于登录信息的日志
        def filterLoginLog(log: String): Boolean = {
            eventIDRegex.findFirstIn(log) match {
                case Some(eventIDRegex(eventid)) =>
                    if (loginSet.contains(eventid.toInt)) true else false
                case _ =>
                    false
            }
        }
        def isSuccessLogin(log: String): Boolean = {  //String  ConsumerRecord[String, String]
            eventIDRegex.findFirstIn(log) match {
                case Some(eventIDRegex(eventid)) =>
                    if ("552".equals(eventid)) true else false
                case _ =>
                    false
            }
        }


        /**
          *封装登录失败的log bean 和登录成功的log bean
          */
        val logWriter = new LogWriter()
        def extractLogBean(log: String): LogBean = {
            logRegex.findFirstIn(log) match {
                case Some(logRegex(destinationIP, time, eventid, username, region, logintype, loginid, sourceip)) => {
                    new LogBean(destinationIP, time, eventid, username, region, logintype,loginid, sourceip)
                }
                case _ => {
                    println("regex error")
                    logWriter.writeLog(log)
                    null
                }
            }
        }
        def extractSuccessLogBean(log: String): LogBean = {
            logSuccessRegex.findFirstIn(log) match {
                case Some(logSuccessRegex(destinationIP, time, eventid, loginid, sourceip)) => {
                    new LogBean(destinationIP, time, eventid, loginid, sourceip)
                }
                case _ => {
                    println("regex error")
                    logWriter.writeLog(log)
                    null
                }
            }
        }


        // 每次处理完的日志 批量插入map
        def insertHashMap(log: Array[(String, LogBean)], map: ConcurrentHashMap[String, GuessPasswdBean]): Unit = {
            if (log != null && !log.isEmpty) {
                for (i <- log) {
                    if (!"ERROR".equals(i._1)) {

                        val isSuccess: Boolean = if (i._1.split("@").length == 2) true else false
                        val ipidKey = i._1.split("@")(0)

                        if (map.containsKey(ipidKey)) {
                            //todo 只用sourceIP#loginID作为key查询，可以避免更新到猜解成功的事件
                            val event: GuessPasswdBean = map.get(ipidKey)

                            if (isSuccess) {
                                // 有记录，成功
                                if (event.getTimes >= LIMIT_TIMES) {
                                    //替换原有的记录，更改key为加上 @SUCCESS后的key
                                    map.put(i._1, event)
                                    map.remove(ipidKey)
                                    println("发生猜解成功事件！！！！！")
                                } else {
                                    // 在限制次数内登录成功，不算暴力破解
                                    map.remove(ipidKey)
                                    println("在限制次数内登录成功，不算暴力破解")
                                }
                            } else {
                                // 有记录，失败
                                if (Calendar.getInstance().getTimeInMillis <= event.getOverTime.getTime) {
                                    // 当前时间小于字段中的超时时间，则该猜解事件未结束，需要更新
                                    event.updateEvent(i._2)
                                } else if (event.getTimes < LIMIT_TIMES) {
                                    // 事件超时并没有达到阈值，应在map中删除该事件
                                    println(event.getEventKey + "事件失效，移出Map....")
                                    map.remove(ipidKey)
                                }
                            }


                        } else {
                            //map中未包含该日志，直接插入，并设置overtime和times
                            //todo 并且第一条不是成功日志 避免正确登录页被记录的情况
                            if (!isSuccess) {
                                // 无记录，失败
                                val eventBean: GuessPasswdBean = new GuessPasswdBean(i._2)
                                map.put(i._1, eventBean)
                            }
                        }
                    }
                }
            }
        }

        /**
          * 分布式RDD算子
          * 生成K-V对，封装成数组传回Driver节点
          */
        var loginLog: Array[(String, LogBean)] = null
        kafkaStream.foreachRDD(rdd => {
            loginLog = rdd.filter(log => filterLoginLog(log._2)).map(log2 => {
                if (isSuccessLogin(log2._2)) {
                    val bean: LogBean = extractSuccessLogBean(log2.toString)
                    if (bean != null) {
                        (bean.getLogKey+"@SUCCESS", bean)
                    } else {
                        ("ERROR", null)
                    }
                } else {
                    val bean: LogBean = extractLogBean(log2.toString)
                    if (bean != null) {
                        (bean.getLogKey, bean)
                    } else {
                        ("ERROR", null)
                    }
                }
            }).collect()
        })

        /*kafkaStream.foreachRDD(rdd => {
            loginLog = rdd.filter(log => filterLoginLog(log.value()).map(log2 => {
                if (isSuccessLogin(log2)) {    //todo filter出来后还应该是二元组
                    val bean: LogBean = extractSuccessLogBean(log2.toString)
                    (bean.getLogKey+"@SUCCESS", bean)
                } else {
                    val bean: LogBean = extractLogBean(log2.toString)
                    (bean.getLogKey, bean)
                }
            }).collect()
        })*/

        /*socketStream.foreachRDD(rdd => {
            loginLog = rdd.filter(log =>
                filterLoginLog(log.toString)).map(log2 => {
                    //判断是否是登录成功事件  552
                    if (isSuccessLogin(log2)) {
                        val bean: LogBean = extractSuccessLogBean(log2.toString)
                        (bean.getLogKey+"@SUCCESS", bean)
                    } else {
                        val bean: LogBean = extractLogBean(log2.toString)
                        (bean.getLogKey, bean)
                    }
            }).collect()
        })*/

        ssc.start()

        val dbManager: DBManager = DBManager.getMDBManager()
        while (true) {
            // 主线程遍历hashmap
            insertHashMap(loginLog, chm)
            //loginLog = null


            //数据库 todo: 开线程写数据库
            val conn: Connection = dbManager.getConnection
            val prepareStatement: PreparedStatement = conn.prepareStatement(sql)
            conn.setAutoCommit(false)

            //遍历map 打印并写入数据库
            val entrySet: util.Set[Entry[String, GuessPasswdBean]] = chm.entrySet()
            val it: util.Iterator[Entry[String, GuessPasswdBean]] = entrySet.iterator()
            while (it.hasNext) {
                val next: Entry[String, GuessPasswdBean] = it.next()
                println("key: "+ next.getKey + " value: " + next.getValue.toString)
                val event = next.getValue
                //找出超时并达到阈值的事件记录，将其写入数据库并移除
                if (event.getTimes >= LIMIT_TIMES && Calendar.getInstance().getTimeInMillis > event.getOverTime.getTime) {
                    println(event.getEventKey + "达到阈值并超时，写入数据库并从Map中移除")

                    prepareStatement.setString(1, event.getSourceIP)
                    prepareStatement.setString(2, if (event.getDestinationIP != null) event.getDestinationIP else "Unknown")
                    prepareStatement.setTimestamp(3, new Timestamp(event.getStartTime.getTime))
                    prepareStatement.setTimestamp(4, new Timestamp(event.getEndTime.getTime))
                    prepareStatement.setInt(5, event.getSysType)
                    prepareStatement.setInt(6, event.getRiskType)
                    prepareStatement.setInt(7, event.getTimes)
                    prepareStatement.setString(8, event.getEventDesc)

                    prepareStatement.addBatch()
                    chm.remove(next.getKey) //成功的key移不出
                }
            }
            prepareStatement.executeBatch()
            conn.commit()
            prepareStatement.close()
            conn.close()
            println("=============================================Finish iteration=============================================")
            Thread.sleep(1000)
        }





        /**
          * batch
          */
        /*val loginFailLog: Array[(String, LogBean)] = winLog.filter(log => filteLoginFailLog(log))
                            .map(failLog => {
                                val bean: LogBean = extractLogBean(failLog)
                                (bean.getLogKey, bean)
                            }).collect()}*/

        // RDD算子操作
        /*val loginFailLog: Array[(String, LogBean)] = winLog
                .filter(log => filteLoginFailLog(log))
                .map(log => {
                    val bean: LogBean = extractLogBean(log)
                    (bean.getLogKey, bean)
                }).collect()

        insertHashMap(loginFailLog, chm)
        val entrySet: util.Set[Entry[String, GuessPasswdBean]] = chm.entrySet()
        val it: util.Iterator[Entry[String, GuessPasswdBean]] = entrySet.iterator()
        while (it.hasNext) {
            val next: Entry[String, GuessPasswdBean] = it.next()
            println("key: "+ next.getKey + " value: " + next.getValue.toString)
        }*/


        ssc.awaitTermination()
    }
}