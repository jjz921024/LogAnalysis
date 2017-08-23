import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Logger;

/**
 * Created by Jun on 2017/8/10.
 */
public class LogBean implements Serializable{

    private int eventID;
    private Date generatedTime;
    private String username;
    private String region;
    private String loginID;
    private String sourceIP;
    private int loginType;
    private String destinationIP;
    private static SimpleDateFormat dateFormat;

    static {
        dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy", Locale.ENGLISH);
    }


    public LogBean(String destinationIP, String generatedTime, String eventID, String username, String region, String loginType, String loginID, String sourceIP) {
        if (eventID != null) {
            this.eventID = new Integer(eventID);
        }
        this.username = username;
        this.region = region;
        this.loginID = loginID;
        this.sourceIP = sourceIP;
        if (loginType != null) {
            this.loginType = new Integer(loginType);  //登录类型只有 失败日志或成功的528日志有
        }
        this.destinationIP = destinationIP;
        try {
            if (generatedTime != null)
                this.generatedTime = dateFormat.parse(generatedTime);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public LogBean(String destinationIP, String generatedTime, String eventid, String loginid, String sourceIP) {
        if (eventid != null) {
            this.eventID = new Integer(eventid);
        }
        this.sourceIP = sourceIP;
        this.destinationIP = destinationIP;
        this.loginID = loginid;
        try {
            if (generatedTime != null)
                this.generatedTime = dateFormat.parse(generatedTime);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    @Override
    public String toString() {
        return "LogBean{" +
                "eventID=" + eventID +
                ", generatedTime=" + generatedTime +
                ", username='" + username + '\'' +
                ", region='" + region + '\'' +
                ", loginID='" + loginID + '\'' +
                ", sourceIP='" + sourceIP + '\'' +
                ", loginType=" + loginType +
                ", destinationIP='" + destinationIP + '\'' +
                '}';
    }

    public String getLogKey() {
        return this.sourceIP + "#" + this.loginID;
    }

    public Date getGeneratedTime() {
        return generatedTime;
    }

    public int getEventID() {
        return eventID;
    }

    public String getUsername() {
        return username;
    }

    public String getRegion() {
        return region;
    }

    public String getLoginID() {
        return loginID;
    }

    public String getSourceIP() {
        return sourceIP;
    }

    public int getLoginType() {
        return loginType;
    }

    public String getDestinationIP() {
        return destinationIP;
    }


}
