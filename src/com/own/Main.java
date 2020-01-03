package com.own;


import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.ClientAuthenticationPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.RegisterSlaveCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.HandshakeInitializationPacket;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannelPool;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.own.mariadb.BinlogEvent;
import com.own.mariadb.MariadbBinglogOption;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.Map;

/**
 * @author Cheng Kung
 */
public class Main {

    public static void main(String[] args) throws Exception {
    }

    private static void printSQL() throws InterruptedException {
        MariadbBinglogOption mariadbBinglogOption = new MariadbBinglogOption("D://logposition");
        Thread thread = new Thread(mariadbBinglogOption);
        thread.start();

        while (true) {
            System.out.println(MariadbBinglogOption.SQL_QUEUE.size());
            Map<String, String> take = MariadbBinglogOption.SQL_QUEUE.take();
            String binlogFilename = take.get("binlogFilename");
            Long binlogPosition = Long.valueOf(take.get("binlogPosition"));
            System.out.println(take.get("sql"));
//            System.out.println(mysqlUpdateExecutor.update(take.get("sql")));
//            stmt.execute(take.get("sql"));
        }
    }







    private static void start() throws ClassNotFoundException, SQLException, InterruptedException {
        MariadbBinglogOption mariadbBinglogOption = new MariadbBinglogOption("D://logposition");
        Thread thread = new Thread(mariadbBinglogOption);
        thread.start();

        String driver = "com.mysql.jdbc.Driver";
        //从配置参数中获取数据库url
        String url = "jdbc:mysql://192.168.56.254:3307";
        //从配置参数中获取用户名
        String user = "root";
        //从配置参数中获取密码
        String pass = "root";
        //注册驱动
        Class.forName(driver);
        //获取数据库连接
        Connection conn = DriverManager.getConnection(url, user, pass);
        Statement statement = conn.createStatement();
        while (true) {
            System.out.println(MariadbBinglogOption.SQL_QUEUE.size());
            Map<String, String> take = MariadbBinglogOption.SQL_QUEUE.take();
            String binlogFilename = take.get("binlogFilename");
            Long binlogPosition = Long.valueOf(take.get("binlogPosition"));
            System.out.println(take.get("sql"));
            try {
                boolean sql = statement.execute(take.get("sql"));
                mariadbBinglogOption.updateLogPosition(binlogFilename, binlogPosition);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
//            System.out.println(mysqlUpdateExecutor.update(take.get("sql")));
//            stmt.execute(take.get("sql"));
        }
    }
}
