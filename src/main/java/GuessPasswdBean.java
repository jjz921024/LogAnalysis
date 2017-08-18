import java.util.Date;

/**
 * Created by Jun on 2017/8/11.
 *
 * 每一起密码猜解事件对应一个该类对象
 */
public class GuessPasswdBean {
    private String sourceIP;
    private String destinationIP;
    private Date startTime;
    private Date endTime;
    private int sysType;  // TODO: 2017/8/14 先默认为0
    private int riskType;
    private int times;
    private String eventDesc;

    private Date overTime;
    private String loginID;
    private static int OVERTIME = 60*1000; //5分种  5*60*1000


    public GuessPasswdBean(LogBean logBean) {
        this.sourceIP = logBean.getSourceIP();
        this.loginID = logBean.getLoginID();

        // 根据失败日志 转换成自定义riskType
        int tmp = logBean.getLoginType();
        if (tmp == 2)
            this.riskType = 3; // 2 为telnet登录 转为自定义3
        else if (tmp == 10)
            this.riskType = 15; // 10为remote登录，转为自定义15
        else
            this.riskType = 17; // 其余当作未知登录方式，转为自定义17

        this.destinationIP = logBean.getDestinationIP();
        this.startTime = logBean.getGeneratedTime(); //该key第一次产生登录失败的时间
        this.overTime = new Date(this.startTime.getTime() + OVERTIME);
        this.eventDesc = logBean.toString();  //每次将最新的log信息设置为事件描述
        this.times++;

    }

    public void updateEvent(LogBean logBean) {
        this.overTime = new Date(logBean.getGeneratedTime().getTime() + OVERTIME);
        this.endTime = logBean.getGeneratedTime(); //每次更新事件时都设endtime为当前时间
        //this.eventDesc = logBean.toString();  //每次将最新的log信息设置为事件描述
        this.times++;

        if (logBean.getEventID() == 552) {
            // 更新时，根据成功日志的eventid 转换成自定义的riskType
            // TODO: 2017/8/15  注意逻辑，根据旧bean的riskType更新riskType
            int tmp = this.getRiskType();
            if (tmp == 3)
                this.riskType = 5; // telnet登录成功，转为自定义3
            else if (tmp == 15)
                this.riskType = 16; // remote登录成功，转为自定义15
            else
                this.riskType = 18; // 其余当作未知方式登录成功，转为自定义17

            this.destinationIP = logBean.getDestinationIP();
        }
    }

    public Date getOverTime() {
        return overTime;
    }

    public int getTimes() {
        return times;
    }

    public String getEventKey() {
        return this.sourceIP + "#" + this.loginID;
    }

    public String getSourceIP() {
        return sourceIP;
    }

    public String getDestinationIP() {
        return destinationIP;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public int getSysType() {
        return sysType;
    }

    public int getRiskType() {
        return riskType;
    }

    public String getEventDesc() {
        return eventDesc;
    }

    public String getLoginID() {
        return loginID;
    }


    @Override
    public String toString() {
        return "GuessPasswdBean{" +
                //"sourceIP='" + sourceIP + '\'' +
                //", loginID='" + loginID + '\'' +
                //", destinationIP='" + destinationIP + '\'' +
                ", riskType=" + riskType +
                ", times=" + times +
                ", startTime=" + startTime +
                ", overTime=" + overTime +
                ", endTime=" + endTime +
                //", sysType=" + sysType +

                //", eventDesc='" + eventDesc + '\'' +
                '}';
    }
}
