package com.own;

import com.own.mariadb.MariadbBinglogOption;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * @author Cheng Kung
 */
public class Main {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        MariadbBinglogOption mariadbBinglogOption = new MariadbBinglogOption("D://logposition");
        Thread thread = new Thread(mariadbBinglogOption);
        thread.start();

        String driver ="com.mysql.jdbc.Driver";
        //从配置参数中获取数据库url
        String url = "jdbc:mysql://192.168.56.254:3307";
        //从配置参数中获取用户名
        String user = "root";
        //从配置参数中获取密码
        String pass = "root";
        //注册驱动
        Class.forName(driver);
        //获取数据库连接
        Connection conn = DriverManager.getConnection(url,user,pass);
        Statement statement = conn.createStatement();
        while (true){
            System.out.println(MariadbBinglogOption.SQL_QUEUE.size());
            Map<String, String> take = MariadbBinglogOption.SQL_QUEUE.take();
            String binlogFilename = take.get("binlogFilename");
            Long binlogPosition = Long.valueOf(take.get("binlogPosition"));
            System.out.println(take.get("sql"));
            try{
                boolean sql = statement.execute(take.get("sql"));
                mariadbBinglogOption.updateLogPosition(binlogFilename,binlogPosition);
            }catch (SQLException e){
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
//            System.out.println(mysqlUpdateExecutor.update(take.get("sql")));
//            stmt.execute(take.get("sql"));
        }
    }
}
