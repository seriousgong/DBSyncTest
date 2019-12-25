package com.own;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.ClientAuthenticationPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.RegisterSlaveCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.HandshakeInitializationPacket;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannelPool;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.own.mariadb.MariadbBinglogOption;
import com.own.mariadb.nnn;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.sql.*;
import java.util.Arrays;
import java.util.Map;

/**
 * @author Cheng Kung
 */
public class Main {

    public static void main(String[] args) throws Exception {
        mysqlConnectionImpl();
    }

    private static void printSQL() throws InterruptedException {
        MariadbBinglogOption mariadbBinglogOption = new MariadbBinglogOption("D://logposition");
        Thread thread = new Thread(mariadbBinglogOption);
        thread.start();

        while (true){
            System.out.println(MariadbBinglogOption.SQL_QUEUE.size());
            Map<String, String> take = MariadbBinglogOption.SQL_QUEUE.take();
            String binlogFilename = take.get("binlogFilename");
            Long binlogPosition = Long.valueOf(take.get("binlogPosition"));
            System.out.println(take.get("sql"));
//            System.out.println(mysqlUpdateExecutor.update(take.get("sql")));
//            stmt.execute(take.get("sql"));
        }
    }

    private static void mysqlConnectionImpl() throws Exception {
        //        start();
        SocketChannel channel = SocketChannelPool.open( new InetSocketAddress("192.168.56.251",3307));
        /*
        *
        *   类型	                    名称	                                    描述
            整数<3>	              payload_lengt                有效负载的长度。超出组成包头的前4个字节的包中的字节数。
            整数<1>	              sequence_id                  序列号
            字符串<var>	     payload[len = payload_length ]    数据包的有效载荷
        * */
        byte[] headerPackageByte = channel.read(4, 9999);
        System.out.println("headerPackageByteSize:"+headerPackageByte.length);
        for (byte b : headerPackageByte) {
            System.out.print(b+" ");
        }
        System.out.println();
        HeaderPacket headerPacket = new HeaderPacket();
        headerPacket.fromBytes(headerPackageByte);
        byte[] serverReponseHandShakePackage = channel.read(headerPacket.getPacketBodyLength(), 9999);
        System.out.println("bodyLength:"+serverReponseHandShakePackage.length);
        for (int i = 0; i < serverReponseHandShakePackage.length; i++) {
//            System.out.println(i+":'"+read[i]+"'");
        }
        /*protocol version*/
        /*String  protocolVersion =*/
        int num = 0;
        System.out.println("protocolVersion:'" + Byte.toString(serverReponseHandShakePackage[num])+"'");
        num++;
        /*server version*/
        int index = indexOf(serverReponseHandShakePackage,0x00,0);
        byte[] bytes = join(serverReponseHandShakePackage,num,index-1);
        System.out.println("serverVersion:'"+new String(bytes)+"'");
        num=index;
        /*connection id  4个字节 需要计算得出*/
        long forthByte = (long)(serverReponseHandShakePackage[++num] & 0xFF);
        long thirdByte = (long)((serverReponseHandShakePackage[++num] & 0xFF) << 8);
        long secondByte = (long)((serverReponseHandShakePackage[++num] & 0xFF) << 16);
        long firstByte = (long)((serverReponseHandShakePackage[++num] & 0xFF) << 24);
        long connectionId = firstByte | secondByte|thirdByte|forthByte;
        System.out.println("connectionId:'" + connectionId + "'");
        /*auth-plugin-data-part1   8字节*/
        byte[] sha1EncryptPart1 = join(serverReponseHandShakePackage, ++num, num += 7);
        System.out.print("{ ");
        for (byte b : sha1EncryptPart1) {
            System.out.print(b+" ");
        }
        System.out.print("}");
        /* filler_1 (1) -- 0x00(固定) */
        byte filler = serverReponseHandShakePackage[++num];
        System.out.println((filler == 0x00)+" "+filler);
        /* capability flags int */
        secondByte = (serverReponseHandShakePackage[++num] & 0xFF);
        firstByte  = (serverReponseHandShakePackage[++num] & 0xFF) << 8;
        int  capabilityFlags_lower =(int) secondByte | (int)firstByte;
        System.out.println("capabilityFlags:'"+capabilityFlags_lower+"'");
        /*如果还*/
        if (serverReponseHandShakePackage.length>num){
            /* charset 33  0x21  utf8_general_ci*/
            byte charSet = serverReponseHandShakePackage[++num];
            System.out.println("charset:'"+Byte.toString(charSet)+"'");
            /*status flags   SERVER_STATUS_AUTOCOMMIT	0x0002	auto-commit is enabled*/
             secondByte = (long)(serverReponseHandShakePackage[++num]);
             firstByte  = (long) (serverReponseHandShakePackage[++num]) << 8;
             long statusFlags = secondByte|firstByte;

             /*capability flags  */
            secondByte = (long)(serverReponseHandShakePackage[++num]);
            firstByte  = (long) (serverReponseHandShakePackage[++num]) << 8;
            int capabilityFlags_upper = (int)secondByte |(int)firstByte;
            System.out.println("statusFlags:'" + statusFlags + "'");
            /*capabilities =  */
            int capabilityFlags = (capabilityFlags_upper << 16) |capabilityFlags_lower;
            System.out.println("capabilities:'"+capabilityFlags+"'");
            System.out.println(capabilityFlags & 0x00008000 );
            System.out.println(capabilityFlags & 0x00080000 );
            /*length of auth-plugin-data or 00 */
            byte b = serverReponseHandShakePackage[++num];
            System.out.println("length of auth-plugin-data:'"+b+"'");
            /*reserved*/

            num+=10;
            /*auth-plugin-data part 2*/
            byte[] sha1EncryptPart2 = join(serverReponseHandShakePackage, ++num,  num+=((int)b-sha1EncryptPart1.length-2));

            System.out.print("{ ");
            for (byte c : sha1EncryptPart2) {
                System.out.print(c+" ");
            }
            System.out.println("}");
//            byte [] authPluginData = joinByteArray(join,join1);
//            System.out.println("auth-plugin-data:'"+new String(authPluginData)+"'");
            num++;
            /*此时值为 0x00 */
            System.out.println(serverReponseHandShakePackage[num]);

            index = indexOf(serverReponseHandShakePackage, 0x00, ++num);
            System.out.println(num);
            System.out.println(index);
            byte[] join2 = join(serverReponseHandShakePackage, num, index-1);
            System.out.println(Arrays.toString(join2));



            /*构建客户端认证包 不知*/
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            int i = Capability.CLIENT_LONG_PASSWORD | Capability.CLIENT_LONG_FLAG
                    | Capability.CLIENT_PROTOCOL_41 | Capability.CLIENT_INTERACTIVE
                    | Capability.CLIENT_TRANSACTIONS | Capability.CLIENT_SECURE_CONNECTION
                    | Capability.CLIENT_MULTI_STATEMENTS | Capability.CLIENT_PLUGIN_AUTH;
            System.out.println(i);
            /*byte 数组每个元素存储8位 int 32 位占4个成员 */
            byteArrayOutputStream.write((byte)(i & 0xFF));
            byteArrayOutputStream.write((byte) (i >>> 8 ));
            byteArrayOutputStream.write((byte)(i >>> 16));
            byteArrayOutputStream.write((byte)(i >>> 24));
            /*max-packet size   16M 固定  int 4个字节*/
            int maxPackageSize = 1 << 24;
            byteArrayOutputStream.write((byte)(maxPackageSize & 0xFF));
            byteArrayOutputStream.write((byte)(maxPackageSize >>> 8));
            byteArrayOutputStream.write((byte)(maxPackageSize >>> 16));
            byteArrayOutputStream.write((byte)(maxPackageSize >>> 24));
            /*charset   字符集 */
            byteArrayOutputStream.write(charSet);
            /* 此处保留23个字节*/
            for (int j = 0; j < 23 ; j++) {
                byteArrayOutputStream.write(0x00);
            }
            /* 认证用户名 */
            byteArrayOutputStream.write("root".getBytes());
            /*String字符不知位数，所以添加截止符*/
            byteArrayOutputStream.write(0x00);
            /*密码 要求加密
                          SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
                          使用 java自带加密类
            * */
            byte[] passwordBytes = "root".getBytes();
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] firstEncrypt = sha1.digest(passwordBytes);
            sha1.reset();
            byte[] secondEncrypt = sha1.digest(firstEncrypt);
            byte[] sha1Encrypt = joinByteArray(sha1EncryptPart1, sha1EncryptPart2);
            HandshakeInitializationPacket handshakeInitializationPacket  = new HandshakeInitializationPacket();
            handshakeInitializationPacket.fromBytes(serverReponseHandShakePackage);
            System.out.println(Arrays.toString(sha1Encrypt));
            System.out.println(Arrays.toString(joinByteArray(handshakeInitializationPacket.seed, handshakeInitializationPacket.restOfScrambleBuff)));

            sha1.reset();
            sha1.update(sha1Encrypt);
            byte[] thirdEncrypt = sha1.digest(secondEncrypt);
            for (int i1 = 0; i1 < thirdEncrypt.length; i1++) {
                thirdEncrypt[i1] =(byte) (thirdEncrypt[i1] ^ firstEncrypt[i1]);
            }
            System.out.println(thirdEncrypt.length);
            byteArrayOutputStream.write((byte)thirdEncrypt.length);
            byteArrayOutputStream.write(thirdEncrypt);

            /*进入数据库名,我们不仅如此数据库，所以不写入*/
//            byteArrayOutputStream.write(0x00);

            /*auth plugin name 认证插件名 我们使用的是安全认证，所以使用 mysql_native_password*/
            byteArrayOutputStream.write("mysql_native_password".getBytes());
            byteArrayOutputStream.write(0x00);
            /*封装好认证包*/
            byte[] clientAuthResponsePackageBytes = byteArrayOutputStream.toByteArray();

            /*每次发送需要由headerPakage缀上数据包发送*/
            HeaderPacket authHeaderPacket = new HeaderPacket();

            authHeaderPacket.setPacketBodyLength(clientAuthResponsePackageBytes.length);
            /*序列号随每次发包递增，代码开头发起时为0*/
            authHeaderPacket.setPacketSequenceNumber((byte)(headerPacket.getPacketSequenceNumber()+1));

            /*header包 auth包 二合一 */
            byte[] authHeaderPackageBytes = authHeaderPacket.toBytes();
            byte[] fullClientAuthPackageBytes = joinByteArray(authHeaderPackageBytes, clientAuthResponsePackageBytes);
            ClientAuthenticationPacket clientAuthenticationPacket = new ClientAuthenticationPacket();
            clientAuthenticationPacket.setUsername("root");
            clientAuthenticationPacket.setPassword("root");
            clientAuthenticationPacket.setCharsetNumber(charSet);
            clientAuthenticationPacket.setDatabaseName(null);
            clientAuthenticationPacket.setScrumbleBuff(sha1Encrypt);
            clientAuthenticationPacket.setServerCapabilities(capabilityFlags_lower);
            byte[] bytes1 = clientAuthenticationPacket.toBytes();
            System.out.println(Arrays.toString(authHeaderPackageBytes));
            System.out.println(bytes1.length);
            System.out.println(fullClientAuthPackageBytes.length);
            System.out.println(Arrays.toString(bytes1));
            System.out.println(Arrays.toString(fullClientAuthPackageBytes));
            /*发包*/
            channel.write(fullClientAuthPackageBytes);
            /*此时包已发完*/
            HeaderPacket authMessageHeaderPackage = new HeaderPacket();
            byte[] read = channel.read(4, 9999);
            authMessageHeaderPackage.fromBytes(read);
            byte[] body = channel.read(authMessageHeaderPackage.getPacketBodyLength(), 9999);
            System.out.println(body[0]);
            System.out.println(body[1]|body[2] << 8);
            byte[] join = join(body, 3, body.length - 1);
            System.out.println(new String(join));
//            sendRegisterSlave(channel);

            sendBinlogPackage(channel);
//            sendTableDumpPackage(channel);
//            byte[] read1 = channel.read(4, 9999);
//            System.out.println(read1[0]);
//            HeaderPacket headerPacket1 = new HeaderPacket();
//            headerPacket1.fromBytes(read1);
//            System.out.println(Arrays.toString(read1));
//           channel.read(headerPacket1.getPacketBodyLength(), 9999);

            formatBinlog(channel);
//            byte[] join3 = join(read2, 3, read2.length - 1);
//            System.out.println(new String(join3));
//            System.out.println(Arrays.toString(read2));
//            byte[] read3 = channel.read(98, 9999);
//
//            System.out.println(new String(read3));
        }


//        HandshakeInitializationPacket handshakeInitializationPacket = new HandshakeInitializationPacket();
//        handshakeInitializationPacket.fromBytes(serverReponseHandShakePackage);
//        System.out.println(handshakeInitializationPacket);
//        Thread.sleep(1000000);
        channel.close();
    }

    private static void formatBinlog(SocketChannel channel) throws IOException {


        byte[] rotateHeader = channel.read(4, 9999);
        System.out.println(Arrays.toString(rotateHeader));
        byte[] read1 = channel.read(rotateHeader[0], 9999);
        System.out.println(Arrays.toString(read1));
/*      以上为rotate事件*/
        byte[] format = channel.read(4, 9999);
        int i = (format[0] & 0xff) | (format[1] & 0xff << 8) | (format[2] & 0xff << 16);
        System.out.println(i);
        byte[] read = channel.read(i, 9999);
        System.out.println(Arrays.toString(read));
        System.out.println(new String(join(read, 20, 70)));
//        System.out.println(new String(join(read, 71, 74)));
        /*以上为format事件*/
        byte[] read3 = channel.read(4, 9999);
        System.out.println(Arrays.toString(read3));
        int j = (read3[0] & 0xff) | (read3[1] & 0xff << 8) | (read3[2] & 0xff << 16);
        byte[] read2 = channel.read(j, 9999);
        System.out.println(Arrays.toString(read2));
//        System.out.println(read[0]);
        /*table map 事件*/
        byte[] read4 = channel.read(4, 9999);
        int k = (read4[0] & 0xff) | (read4[1] & 0xff << 8) | (read4[2] & 0xff << 16);
        System.out.println(Arrays.toString(read4));
        byte[] read5 = channel.read(k, 9999);
        System.out.println(Arrays.toString(read5));
        System.out.println("tableId:'" + (read5[20] & 0xff)+"'");
        System.out.println("flag:'" + (read5[26] & 0xff) + "'");

    }
    private void sendRegisterSlave(SocketChannel channel) throws IOException {
        RegisterSlaveCommandPacket cmd = new RegisterSlaveCommandPacket();
        cmd.reportHost ="192.168.56.251";
        cmd.reportPasswd = "root";
        cmd.reportUser = "root";
        cmd.serverId = 4;
        byte[] cmdBody = cmd.toBytes();

        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(cmdBody.length);
        header.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(channel, header.toBytes(), cmdBody);

        header = PacketManager.readHeader(channel, 4);
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength());
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
    private static void sendTableDumpPackage(SocketChannel channel) throws IOException {
        /*创建 binglogdump 请求包*/

        HeaderPacket headerPacket = new HeaderPacket();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        /*默认值  [13] COM_BINLOG_DUMP 占一个字节*/
        byteArrayOutputStream.write((byte)0x13);
        /*  数据库长度*/
        byte[] bytes1 = "test".getBytes();
        byteArrayOutputStream.write(bytes1.length);
        /* 写入数据库名字信息*/
        byteArrayOutputStream.write(bytes1);
        /* 表名*/
        byte[] bytes2 = "asdq".getBytes();
        byteArrayOutputStream.write(bytes2.length);
        byteArrayOutputStream.write(bytes2);
        /*binlog filename */
        byte[] bytes = byteArrayOutputStream.toByteArray();
        headerPacket.setPacketBodyLength(bytes.length);
        headerPacket.setPacketSequenceNumber((byte)0x00);

        byteArrayOutputStream.reset();
        byteArrayOutputStream.write(headerPacket.toBytes());
        byteArrayOutputStream.write(bytes);

        channel.write(byteArrayOutputStream.toByteArray());
    }
    private static void sendBinlogPackage(SocketChannel channel) throws IOException {
        /*创建 binglogdump 请求包*/

        HeaderPacket headerPacket = new HeaderPacket();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        /*默认值  [12] COM_BINLOG_DUMP 占一个字节*/
        byteArrayOutputStream.write((byte)0x12);
        /*  binlog-pos   4个字节 ，我们就从4开始*/
        int binlogPosition = 414;
        byte fouthByte = (byte)(binlogPosition & 0xFF);
        byte thirdByte = (byte)(binlogPosition >>> 8);
        byte secondByte = (byte)(binlogPosition >>> 16);
        byte firstByte = (byte) (binlogPosition >>> 24);
        byteArrayOutputStream.write(fouthByte);
        byteArrayOutputStream.write(thirdByte);
        byteArrayOutputStream.write(secondByte);
        byteArrayOutputStream.write(firstByte);

        /* flags mysql官网让我们选择  0x01  BINLOG_DUMP_NON_BLOCK   0x02 BINLOG_SEND_ANNOTATE_ROWS_EVENT */
        byteArrayOutputStream.write(0x02);
        byteArrayOutputStream.write(0x00);
        /* server-id */
        int serverId = 4;
         fouthByte = (byte)(serverId & 0xFF);
         thirdByte = (byte)(serverId >>> 8);
         secondByte = (byte)(serverId >>> 16);
         firstByte = (byte) (serverId >>> 24);

        byteArrayOutputStream.write(fouthByte);
        byteArrayOutputStream.write(thirdByte);
        byteArrayOutputStream.write(secondByte);
        byteArrayOutputStream.write(firstByte);
        /*binlog filename */
        String  binlogFilename = "binlog.000009";
        byteArrayOutputStream.write(binlogFilename.getBytes());
        byte[] bytes = byteArrayOutputStream.toByteArray();
        System.out.println("asdasdas:::::::::::"+Arrays.toString(bytes));
        headerPacket.setPacketBodyLength(bytes.length);
        headerPacket.setPacketSequenceNumber((byte)0x00);

        byteArrayOutputStream.reset();
        byteArrayOutputStream.write(headerPacket.toBytes());
        byteArrayOutputStream.write(bytes);

        channel.write(byteArrayOutputStream.toByteArray());
    }

    private static byte[] joinByteArray(byte[] join, byte[] join1) {
        int length = join.length+join1.length;
        byte[] newByteArray = new byte[length];
        System.arraycopy(join,0,newByteArray,0,join.length);
        System.arraycopy(join1,0,newByteArray,join.length,join1.length);
        return newByteArray;

    }

    private static byte[] join(byte[] serverReponseHandShakePackage, int offset, int end) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        for (int i = offset; i <= end; i++) {
            byteArrayOutputStream.write(serverReponseHandShakePackage[i]);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private static int indexOf(byte[] serverReponseHandShakePackage, int value,int offset) {
        for (int i = offset; i < serverReponseHandShakePackage.length; i++) {

            if (serverReponseHandShakePackage[i]==value){
                return i;
            }
        }
        return -1;
    }


    private static void start() throws ClassNotFoundException, SQLException, InterruptedException {
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
