package utils;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Created by Jun on 2017/8/22.
 */
public class LogWriter implements Serializable{
    private String REGEX_ERROR_FILE = "data/regex_error.log";

    public void writeLog(String log) {
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(new File(REGEX_ERROR_FILE), true);

            fileOutputStream.write(log.getBytes(Charset.forName("utf-8")));
            fileOutputStream.write("\n".getBytes());
            fileOutputStream.flush();



        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
