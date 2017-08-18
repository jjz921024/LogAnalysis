import java.io.{File, FileInputStream}
import java.sql.{PreparedStatement, Timestamp}
import java.util
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.{Calendar, Properties}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Jun on 2017/8/9.
  */
object LogAnalysisStreaming {
    def main(args: Array[String]) {
        //System.setProperty("hadoop.home.dir", "D:/hadoop-2.7.3/hadoop-2.7.3")
        val conf = new SparkConf()
                //.setMaster("local[2]")
                .setAppName("LogAnalysisStreaming")
        val sc = new SparkContext(conf)

        /*val winLog = sc.hadoopFile("E:\\guess_passwd", classOf[TextInputFormat],
            classOf[LongWritable], classOf[Text]).map(
            pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))*/

        val ssc = new StreamingContext(sc, Seconds(20))
        //val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 5142)

        //kafka配置
        val props: Properties = new Properties
        //val in: InputStream = this.getClass.getClassLoader.getResourceAsStream("conf/kafka.properties")
        val in: FileInputStream = new FileInputStream(new File("conf/kafka.properties"))

        props.load(in)
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> props.getProperty("bootstrap.servers"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> props.getProperty("group.id"),
            "auto.offset.reset" -> props.getProperty("auto.offset.reset"),
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )
        val topics: Array[String] = props.getProperty("topics").split(" ")
        val kafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

        // Master节点维护的hashmap
        val chm = new ConcurrentHashMap[String, GuessPasswdBean]()

        // 正则
        val logRegex =
            """([\d+\.\d+\.\d+\.\d+]+).+?([a-zA-Z]{3} [a-zA-Z]{3} \d{2} \d{2}:\d{2}:\d{2} \d{4})\s+(\d{3})\sSecurity.+?[用户名?]+: (\w+) +域:\s+([\w-]+).+?登录类型: (\d).+?调用方登录 ID: ([\(\w,\)]+).+?源网络地址: ([\d+\.\d+\.\d+\.\d+-]+)""".r
        val logSuccessRegex =
            """([\d+\.\d+\.\d+\.\d+]+).+([a-zA-Z]{3} [a-zA-Z]{3} \d{2} \d{2}:\d{2}:\d{2} \d{4})\s+(\d{3})\sSecurity.+?登录 ID:  ([\(\w,\)]+).+?源网络地址: ([\d+\.\d+\.\d+\.\d+-]+)""".r
        val eventIDRegex = """\d{4}\s(\d{3})\sSecurity""".r

        //sql
        val sql: String = "insert into sysrisktable (sourceIP, destinationIP, startTime, endTime, sysType, riskType, times, eventDesc) values (?,?,?,?,?,?,?,?)"

        // 登录失败的事件id
        val loginSet: Set[Int] = Set(529,530,531,532,533,534,535,536,537,539, 528, 552)
        val LIMIT_TIMES = 5


        // 过滤属于登录信息的日志
        def filterLoginLog(log: String): Boolean = {
            eventIDRegex.findFirstIn(log) match {
                case Some(eventIDRegex(eventid)) =>
                    if (loginSet.contains(eventid.toInt)) true else false
                case _ =>
                    false
            }
        }
        def isSuccessLogin(log: ConsumerRecord[String, String]): Boolean = {  //String
            eventIDRegex.findFirstIn(log.value()) match {
                case Some(eventIDRegex(eventid)) =>
                    if (eventid.toInt == 552) true else false
                case _ =>
                    false
            }
        }


        /**
          *封装登录失败的log bean 和登录成功的log bean
          */
        def extractLogBean(log: String): LogBean = {
            logRegex.findFirstIn(log) match {
                case Some(logRegex(destinationIP, time, eventid, username, region, logintype, loginid, sourceip)) =>
                    new LogBean(destinationIP, time, eventid, username, region, logintype,loginid, sourceip)
                case _ => {
                    println("regex error")
                    new LogBean(null, null, null, null, null, null, null, null)
                }
            }
        }
        def extractSuccessLogBean(log: String): LogBean = {
            logSuccessRegex.findFirstIn(log) match {
                case Some(logSuccessRegex(destinationIP, time, eventid, loginid, sourceip)) =>
                    new LogBean(destinationIP, time, eventid, loginid, sourceip)
                case _ => {
                    println("regex error")
                    new LogBean(null, null, null, null, null)
                }
            }
        }


        // 每次处理完的日志 批量插入map
        def insertHashMap(log: Array[(String, LogBean)], map: ConcurrentHashMap[String, GuessPasswdBean]): Unit = {
            if (log != null && !log.isEmpty) {
                for (i <- log) {
                    // TODO: i._1有可能包含SUCCESS标志
                    val isSuccess: Boolean = if (i._1.split("@").length == 2) true else false
                    val ipidKey = i._1.split("@")(0)

                    if (map.containsKey(ipidKey)) {
                        //todo 只用sourceIP#loginID作为key查询，可以避免更新到猜解成功的事件
                        val event: GuessPasswdBean = map.get(ipidKey)

                        // 登录成功并且相同key达到猜解次数，认为猜解成功事件
                        if (isSuccess && event.getTimes >= LIMIT_TIMES) {
                            //替换原有的记录，更改key为加上 @SUCCESS后的key
                            map.put(i._1, event)
                            map.remove(ipidKey)
                            println("发生猜解成功事件！！！！！")
                        }

                        // 当前时间小于字段中的超时时间，则该猜解事件未结束，需要更新
                        if (Calendar.getInstance().getTimeInMillis <= event.getOverTime.getTime) {
                            //事件未超时，更新事件
                            event.updateEvent(i._2)
                        } else if (event.getTimes < LIMIT_TIMES) {
                            // 事件超时并没有达到阈值，应在map中删除该事件
                            println(event.getEventKey + "事件失效，移出Map....")
                            map.remove(ipidKey)
                        }


                    } else {
                        //map中未包含该日志，直接插入，并设置overtime和times
                        //todo 并且第一条不是成功日志 避免正确登录页被记录的情况
                        if (!isSuccess) {
                            // 不是@SUCCESS的情况
                            val eventBean: GuessPasswdBean = new GuessPasswdBean(i._2)
                            map.put(i._1, eventBean)
                        }
                    }
                }
                //lock.notifyAll()

            }
        }

        /**
          * 分布式RDD算子
          * 生成K-V对，封装成数组传回Driver节点
          */
        var loginLog: Array[(String, LogBean)] = null
        kafkaStream.foreachRDD(rdd => {
            loginLog = rdd.filter(log => filterLoginLog(log.value())).map(log2 => {   //todo
                if (isSuccessLogin(log2)) {
                    val bean: LogBean = extractSuccessLogBean(log2.toString)
                    (bean.getLogKey+"@SUCCESS", bean)
                } else {
                    val bean: LogBean = extractLogBean(log2.toString)
                    (bean.getLogKey, bean)
                }
            }).collect()
        })

        /*socketStream.foreachRDD(rdd => {
            loginLog = rdd.filter(log =>
                filterLoginLog(log.toString)).map(log2 => {
                    //判断是否是登录成功事件  552
                    if (isSuccessLogin_socket(log2)) {
                        val bean: LogBean = extractSuccessLogBean(log2.toString)
                        (bean.getLogKey+"@SUCCESS", bean)
                    } else {
                        val bean: LogBean = extractLogBean(log2.toString)
                        (bean.getLogKey, bean)
                    }
            }).collect()
        })*/

        ssc.start()

        while (true) {
            // 主线程遍历hashmap
            insertHashMap(loginLog, chm)
            loginLog = null

            /*count += 1
            if (count == 65535) {
                val dbThread: DBThread = new DBThread(chm)
                val thread: Thread = new Thread(dbThread)
                thread.start()
                count = 0
            }*/


            //数据库 todo: 开线程写数据库
            val conn = DBManager.getMDBManager().getConnection
            val prepareStatement: PreparedStatement = conn.prepareStatement(sql)  //todo 是否需要close
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
                    chm.remove(event.getEventKey)
                }
            }
            prepareStatement.executeBatch()
            conn.commit()
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