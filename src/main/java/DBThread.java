import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Jun on 2017/8/15.
 */
public class DBThread implements Runnable{

    private static String sql = "insert into sysrisktable (sourceIP, destinationIP, startTime, endTime, sysType, riskType, times, eventDesc) values (?,?,?,?,?,?,?,?)";
    private ConcurrentHashMap chm;
    private static DBManager dbManager = new DBManager();
    private static final int LIMIT_TIMES = 5;

    public DBThread(ConcurrentHashMap map) {
        this.chm = map;
    }

    @Override
    public void run() {

        try {
            Connection conn = dbManager.getConnection();
            PreparedStatement prepareStatement = conn.prepareStatement(sql); //todo 是否需要close
            conn.setAutoCommit(false);

            //遍历map 打印并写入数据库
            Set<Map.Entry<String,GuessPasswdBean>> entrySet = chm.entrySet();
            for (Map.Entry<String, GuessPasswdBean> next : entrySet) {
                System.out.println("key: "+ next.getKey() + " value: " + next.getValue().toString());
                GuessPasswdBean event = next.getValue();
                if (event.getTimes() >= LIMIT_TIMES && Calendar.getInstance().getTimeInMillis() > event.getOverTime().getTime()) {
                    System.out.println(event.getEventKey() + "达到阈值并超时，写入数据库并从Map中移除");
                    prepareStatement.setString(1, event.getSourceIP());
                    prepareStatement.setString(2, "Unknown"); //todo 数据库中该字段不能为空
                    prepareStatement.setTimestamp(3, new Timestamp(event.getStartTime().getTime()));
                    prepareStatement.setTimestamp(4, new Timestamp(event.getEndTime().getTime()));
                    prepareStatement.setInt(5, event.getSysType());
                    prepareStatement.setInt(6, event.getRiskType());
                    prepareStatement.setInt(7, event.getTimes());
                    prepareStatement.setString(8, event.getEventDesc());

                    prepareStatement.addBatch();
                    chm.remove(event.getEventKey());
                }
            }
            prepareStatement.executeBatch();
            conn.commit();
            conn.close();
            System.out.println("=============================================Finish iteration=============================================");
            Thread.sleep(5000);




        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
