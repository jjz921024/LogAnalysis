package utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

/**
 * Created by Jun on 2017/8/17.
 */
public class KafkaLogProducer {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy", Locale.ENGLISH);
    private static int[] userid = {1,2,3,4,5,6,7,8};
    private static KafkaProducer<String, String> kafkaProducer;

    public static void main(String[] args) throws InterruptedException, IOException {
        Properties props = new Properties();
        InputStream in = KafkaLogProducer.class.getResourceAsStream("/kafkaProductor.properties");
        props.load(in);

        kafkaProducer = new KafkaProducer<>(props);

        int i = 0;
        while (true) {
            if (i%2 == 0) {
                String log = printFailLog(Calendar.getInstance().getTime(), userid[i % 5]); //2*5*1000  10秒每个id失败日志
                sendToKafka(log);
            }
            else {
                String log = printNormalLog(Calendar.getInstance().getTime());
                sendToKafka(log);
            }
            if (i%5 == 0) {
                String log = printSuccessLog(Calendar.getInstance().getTime(), userid[i % 2]);
                sendToKafka(log);
            }
            i++;
            Thread.sleep(1000);
        }
    }

    private static void sendToKafka(String log) {
        kafkaProducer.send(new ProducerRecord<>("log-win03", log), new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null)
                    e.printStackTrace();
                System.out.println("刚发送的消息的offset: " + recordMetadata.offset());
            }
        });
    }


    private static String printNormalLog(Date curDate) {
        String s = "192.168.3.123##<11>Jul 25 16:48:14 JUN-59DF1BE8B1D MSWinEventLog\t3\tSecurity\t117\t" +
                dateFormat.format(curDate) +
                "\t680\tSecurity\tSYSTEM\tUser\tFailure Audit\tJUN-59DF1BE8B1D\t" +
                "帐户登录 \t\t尝试登录的用户:  MICROSOFT_AUTHENTICATION_PACKAGE_V1_0   " +
                "登录帐户:   Administrator   源工作站:  JUN-59DF1BE8B1D   错误代码:  0xC000006A   \t4\n";
        //System.out.print(s);
        return s;
    }

    private static String printFailLog(Date curDate, int user) {
        String s = "192.168.3.123##<11>Jul 25 16:48:14 JUN-59DF1BE8B1D MSWinEventLog\t3\tSecurity\t118\t" +
                dateFormat.format(curDate) +
                "\t529\tSecurity\tSYSTEM\tUser\tFailure Audit\tJUN-59DF1BE8B1D\t登录/注销 \t\t" +
                "登录失败:    原因:  用户名未知或密码错误    用户名: Administrator    域:  JUN-59DF1BE8B1D    " +
                "登录类型: 7    登录进程: User32      身份验证数据包: Negotiate    工作站名称: JUN-59DF1BE8B1D    " +
                "调用方用户名: JUN-59DF1BE8B1D$    调用方域: WORKGROUP    " +
                "调用方登录 ID: (0x0,0x3E"+user+")    " +
                "调用方进程 ID:  368    传递服务:  -    源网络地址: 127.0.0.1    源端口: 0   \t5\n";
        //System.out.print(s);
        return s;
    }

    private static String printSuccessLog(Date curDate, int user) {
        String s = "192.168.3.123##<14>Jul 25 16:49:46 JUN-59DF1BE8B1D MSWinEventLog\t1\tSecurity\t161\t" +
                dateFormat.format(curDate) +
                "\t552\tSecurity\tSYSTEM\tUser\tSuccess Audit\tJUN-59DF1BE8B1D\t登录/注销 \t\t" +
                "使用明确凭据的登录尝试:   登录的用户:    用户名: JUN-59DF1BE8B1D$    域:  WORKGROUP    登录 ID:  (0x0,0x3E"+user+")    " +
                "登录 GUID: -   凭据被使用的用户:    目标用户名: Administrator    目标域: JUN-59DF1BE8B1D    目标登录 GUID: -     " +
                "目标服务器名称: localhost   目标服务器信息: localhost   调用方进程 ID: 368   源网络地址: 127.0.0.1   源端口: 0   \t48\n";
        //System.out.print(s);
        return s;
    }
}

