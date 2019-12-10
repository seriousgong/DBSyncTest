package com.own.mariadb;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlQueryExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.RegisterSlaveCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher.MASTER_HEARTBEAT_PERIOD_SECONDS;

/**
 * MariadbBinglogOption class
 * @author Cheng Kung
 * @date 2019/11/29 15:00
 */
public class MariadbBinglogOption implements  Runnable {
    private Logger logger = LoggerFactory.getLogger(MariadbBinglogOption.class);
    private static String binlogFileName ;
    private static Long binlogPosition ;
    /**缓存收集到的sql,而不是一股脑直接消费*/
    public final static ArrayBlockingQueue<Map<String,String>> SQL_QUEUE  =  new ArrayBlockingQueue<>(100);
    public String filePath ;

    private Charset charset = Charset.forName("utf-8");
    private static MysqlQueryExecutor executor;

    static {
        try {
            MysqlConnector mysqlConnector = new MysqlConnector(new InetSocketAddress("192.168.56.253", 3307), "root", "root");
            mysqlConnector.connect();
            executor = new MysqlQueryExecutor(mysqlConnector);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static String loadLogPositionFile(String filePath) throws IOException {
        FileReader fileReader = new FileReader(filePath);
        char[] chars = new char[50];
        fileReader.read(chars);
        fileReader.close();
        return String.valueOf(chars).trim();
    }

    private void sendRegisterSlave(MysqlConnector connector, int slaveId) throws IOException {
        RegisterSlaveCommandPacket cmd = new RegisterSlaveCommandPacket();
        cmd.reportHost = connector.getAddress().getAddress().getHostAddress();
        cmd.reportPasswd = connector.getPassword();
        cmd.reportUser = connector.getUsername();
        cmd.serverId = slaveId;
        byte[] cmdBody = cmd.toBytes();

        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(cmdBody.length);
        header.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), header.toBytes(), cmdBody);

        header = PacketManager.readHeader(connector.getChannel(), 4);
        byte[] body = PacketManager.readBytes(connector.getChannel(), header.getPacketBodyLength());
        assert body != null;
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Register slave:" + err.toString());
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
            }
        }
    }

    private void update(String cmd, MysqlConnector connector) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    private void updateSettings(MysqlConnector connector) throws IOException {
        try {
            update("set wait_timeout=9999999", connector);
        } catch (Exception e) {
            logger.warn("update wait_timeout failed", e);
        }
        try {
            update("set net_write_timeout=1800", connector);
        } catch (Exception e) {
            logger.warn("update net_write_timeout failed", e);
        }

        try {
            update("set net_read_timeout=1800", connector);
        } catch (Exception e) {
            logger.warn("update net_read_timeout failed", e);
        }

        try {
            // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
            update("set names 'binary'", connector);
        } catch (Exception e) {
            logger.warn("update names failed", e);
        }

        try {
            // mysql5.6针对checksum支持需要设置session变量
            // 如果不设置会出现错误： Slave can not handle replication events with the
            // checksum that master is configured to log
            // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
            update("set @master_binlog_checksum= @@global.binlog_checksum", connector);
        } catch (Exception e) {
            logger.warn("update master_binlog_checksum failed", e);
        }

        try {
            // 参考:https://github.com/alibaba/canal/issues/284
            // mysql5.6需要设置slave_uuid避免被server kill链接
            update("set @slave_uuid=uuid()", connector);
        } catch (Exception e) {
            if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
                logger.warn("update slave_uuid failed", e);
            }
        }

        try {
            // mariadb针对特殊的类型，需要设置session变量
            update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'", connector);
        } catch (Exception e) {
            logger.warn("update mariadb_slave_capability failed", e);
        }

        try {
            long period = TimeUnit.SECONDS.toNanos(MASTER_HEARTBEAT_PERIOD_SECONDS);
            update("SET @master_heartbeat_period=" + period, connector);
        } catch (Exception e) {
            logger.warn("update master_heartbeat_period failed", e);
        }
    }
    public void loadLogPosition(){
        System.out.println("-----------"+binlogFileName+":"+binlogPosition+"-------------");
    }
    private void connect(String filePath) throws IOException, SQLException {

        /*获取上次同步binlog文件位置*/
        String binlogFileNamePosition = loadLogPositionFile(filePath);
        String[] split = binlogFileNamePosition.split(":");
        binlogFileName=split[0];
        binlogPosition= Long.valueOf(split[1]);
        /*创建mysql客户端*/
        MysqlConnector mysqlConnector = new MysqlConnector(new InetSocketAddress("192.168.56.253", 3307), "root", "root");
        mysqlConnector.connect();
        /*修改session配置*/
        updateSettings(mysqlConnector);
        /*设置微服务模拟从id*/
        sendRegisterSlave(mysqlConnector, 4);
        /*创建数据包*/
        BinlogDumpCommandPacket binlogDumpCommandPacket = new BinlogDumpCommandPacket();
        binlogDumpCommandPacket.binlogFileName = binlogFileName ;
        binlogDumpCommandPacket.binlogPosition = binlogPosition;
        binlogDumpCommandPacket.slaveServerId = 4;
        byte[] binlogPositionPackageToByte = binlogDumpCommandPacket.toBytes();
        /*header包,socker传递规则每次传包必须带header*/
        HeaderPacket headerPacket = new HeaderPacket();
        headerPacket.setPacketBodyLength(binlogPositionPackageToByte.length);
        headerPacket.setPacketSequenceNumber((byte) 0x00);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(headerPacket.toBytes());
        byteArrayOutputStream.write(binlogPositionPackageToByte);
        mysqlConnector.getChannel().write(byteArrayOutputStream.toByteArray());

        DirectLogFetcher directLogFetcher = new DirectLogFetcher();
        directLogFetcher.start(mysqlConnector.getChannel());
        MysqlConnection mysqlConnection = new MysqlConnection();
        mysqlConnection.setConnector(mysqlConnector);
        LogDecoder logDecoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext logContext = new LogContext();
        logContext.setFormatDescription(new FormatDescriptionLogEvent(4, LogEvent.BINLOG_CHECKSUM_ALG_CRC32));
        /*接收管道传回包数据*/
        while (directLogFetcher.fetch()) {
//            System.out.println(directLogFetcher);
            /*事件对象*/
            LogEvent event = logDecoder.decode(directLogFetcher, logContext);
            LogHeader header = event.getHeader();
            int eventType = event.getHeader().getType();
            switch (eventType) {
                case LogEvent.ROTATE_EVENT:
                    // binlogFileName = ((RotateLogEvent)
                    // event).getFilename();
                    System.out.println(((RotateLogEvent) event).getFilename());
                    break;
                case LogEvent.WRITE_ROWS_EVENT_V1:
                case LogEvent.WRITE_ROWS_EVENT:
                    parseRowsEvent((WriteRowsLogEvent) event, mysqlConnector);
                    break;
                case LogEvent.UPDATE_ROWS_EVENT_V1:
                case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
                case LogEvent.UPDATE_ROWS_EVENT:
                    parseRowsEvent((UpdateRowsLogEvent) event, mysqlConnector);
                    break;
                case LogEvent.DELETE_ROWS_EVENT_V1:
                case LogEvent.DELETE_ROWS_EVENT:
                    parseRowsEvent((DeleteRowsLogEvent) event, mysqlConnector);
                    break;
//                case LogEvent.QUERY_EVENT:
//                    parseQueryEvent((QueryLogEvent) event);
//                    break;
//                case LogEvent.ROWS_QUERY_LOG_EVENT:
//                    parseRowsQueryEvent((RowsQueryLogEvent) event);
//                    break;
                case LogEvent.ANNOTATE_ROWS_EVENT:
                    break;
                case LogEvent.XID_EVENT:
                    break;
                default:
                    break;
            }
        }
        mysqlConnector.disconnect();
    }

    private void parseRowsEvent(RowsLogEvent event, MysqlConnector mysqlConnector) throws IOException, SQLException {
//        String sql="";
        System.out.println("" +
                "*******************************************************************************************************************************"
        );
        TableMapLogEvent table = event.getTable();
        LogHeader header = event.getHeader();
        if(header.getLogFileName().matches("binlog\\.00")){
            header.setLogFileName(binlogFileName);
        }
        TableMapLogEvent.ColumnInfo[] columnInfo = table.getColumnInfo();
        System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s]",
                header.getLogFileName(),
                header.getLogPos(),
                event.getTable().getDbName(),
                event.getTable().getTableName()));
        RowsLogBuffer buffer = event.getRowsBuf(charset.name());

        BitSet columns = event.getColumns();
        BitSet changeColumns = event.getChangeColumns();
        List<String> before;
        List<String> after = null;

        while (buffer.nextOneRow(columns)) {
            // 处理row记录
            int type = event.getHeader().getType();
            if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
                // insert的记录放在before字段中
                before = parseOneRow(event, buffer, columns, true, mysqlConnector);
                makeSqlLanguage(event, before, after, "insert");
            } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
                // delete的记录放在before字段中
                before = parseOneRow(event, buffer, columns, false, mysqlConnector);
                makeSqlLanguage(event, before, after, "delete");
            } else {
                // update需要处理before/after
                before = parseOneRow(event, buffer, columns, false, mysqlConnector);
                if (!buffer.nextOneRow(changeColumns, true)) {
                    break;
                }
                after = parseOneRow(event, buffer, changeColumns, true, mysqlConnector);
                makeSqlLanguage(event, before, after, "update");
            }
//            System.out.println("" +
//                    "********************************************************************************************************"
//            );
        }
        if (changeColumns == null || changeColumns.size() == 0) {
//            System.out.println(changeColumns+"is null");
        }
    }

    public   void updateLogPosition(String binLogFileName ,long binLogPosition) throws IOException {
        FileWriter fileWriter = new FileWriter(this.filePath);
        fileWriter.write(binLogFileName+":"+binLogPosition);
        fileWriter.flush();
        fileWriter.close();
    }

    private List<String> parseOneRow(RowsLogEvent event, RowsLogBuffer buffer, BitSet cols, boolean isAfter, MysqlConnector mysqlConnector)
            throws IOException, SQLException {

        TableMapLogEvent map = event.getTable();
        if (map == null) {
            throw new RuntimeException("not found TableMap with tid=" + event.getTableId());
        }
        TableMapLogEvent.ColumnInfo[] columnInfo = map.getColumnInfo();
        final int columnCnt = map.getColumnCnt();
        List<String> columnsValue = new ArrayList<>(columnCnt);
//        System.out.println("列数量:" + columnCnt);
        for (int i = 0; i < columnCnt; i++) {
            if (!cols.get(i)) {

                continue;
            }
            TableMapLogEvent.ColumnInfo info = columnInfo[i];
            buffer.nextValue(null, i, info.type, info.meta);
            if (buffer.isNull()) {
                columnsValue.add(null);
                //
            } else {
                final Serializable value = buffer.getValue();
                if (value instanceof byte[]) {
                    String s = new String((byte[]) value);
                    columnsValue.add(s);
//                    ResultSetPacket resultSetPacket = resultSetPackets.get(i);
//                    System.out.println(
//                            "'" + resultSetPacket + "'::" +
//                            );
                } else {
                    columnsValue.add(value + "");
//                    System.out.println(
//                            "'" + resultSetPackets + "'::" +
//                                   value);
                }
            }
        }
        return columnsValue;
    }

    private void makeSqlLanguage(RowsLogEvent event, List<String> beforeValues, List<String> afterValues, String DMLType) throws IOException {
        String sql = DMLType;
        String dbName = event.getTable().getDbName();
        String tableName = event.getTable().getTableName();
        List<ResultSetPacket> resultSet = executor.queryMulti("DESCRIBE " + dbName + "." + tableName + ";");
        List<String> columnsName = resultSet.get(0).getFieldValues();
/*        System.out.println("每个表字段有占6个值"+columnsName.size() % 6 + "::::::"+columnsName.size());
        System.out.println(columnsName);*/
        switch (DMLType) {
            case "delete":
                sql += (" from " + dbName + "." + tableName + " where ");
                for (int i = 0; i < beforeValues.size(); i++) {
                    if (beforeValues.get(i) == null || "null".equals(beforeValues.get(i))) {
                        sql += (columnsName.get(i * 6) + "=" + beforeValues.get(i) + " and ");
                    } else {
                        sql += (columnsName.get(i * 6) + "='" + beforeValues.get(i) + "' and ");
                    }
                }
                sql = sql.substring(0, sql.length() - 4) + ";";
                break;
            case "insert":
                sql += (" into " + dbName + "." + tableName + " values ( ");
                for (String beforeValue : beforeValues) {
                    if (beforeValue == null || "null".equals(beforeValue)) {
                        sql += ("" + beforeValue + ",");
                    } else {
                        sql += ("'" + beforeValue + "',");
                    }
                }
                sql = sql.substring(0, sql.length() - 1) + ");";
                break;
            case "update":
                sql += (" " + dbName + "." + tableName + " set ");
                String whereConndition = " where ";
                for (int i = 0; i < beforeValues.size(); i++) {
                    String beforevalue = beforeValues.get(i);
                    String afterValue = afterValues.get(i);
                    if (beforevalue == null && afterValue == null) {
                        continue;
                    } else if (beforevalue == null) {
                            sql += (columnsName.get(i * 6) + "='"+ afterValue+"',");
                    } else if (afterValue == null){
                            sql += (columnsName.get(i * 6) + "=" + afterValue + ",");
                    }
                    else if (!beforevalue.equals(afterValue)) {
                            sql += (columnsName.get(i * 6) + "='" + afterValue + "',");
                    } else {
                            whereConndition += (columnsName.get(i * 6) + "='" + beforevalue + "' and ");

                    }
                }
                sql = sql.substring(0, sql.length() - 1);
                sql += whereConndition;
                sql = sql.substring(0, sql.length() - 4) + ";";
                break;
                default:
        }
        /*System.out.println(sql);*/
        HashMap<String,String> argMap = new HashMap<>(3);
        argMap.put("sql",sql);
        argMap.put("binlogFilename",event.getHeader().getLogFileName());
        argMap.put("binlogPosition",event.getHeader().getLogPos()+"");
        while (SQL_QUEUE.size()==100){
        }
        try {
            SQL_QUEUE.put(argMap);
            System.out.println(SQL_QUEUE.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            connect(filePath);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public MariadbBinglogOption(String filePath) {
        this.filePath = filePath;
    }

    public MariadbBinglogOption() {
    }
}
