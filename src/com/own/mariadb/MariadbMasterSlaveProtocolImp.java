package com.own.mariadb;

import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.RegisterSlaveCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannelPool;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;

/**
 *
 * @author GongCheng
 * @date 2020/1/3 15:53
 *
 */
public class MariadbMasterSlaveProtocolImp {

    public static void main(String[] args) throws Exception {
        /* 建立socker通信*/
        SocketChannel channel = SocketChannelPool.open(new InetSocketAddress("192.168.56.130", 14544));
        /* 建立mysql connect 连接*/
        connect(channel);
        sendBinlogPackage(channel);
        formatBinlog(channel);
        channel.close();
    }
    private static void sendBinlogPackage(SocketChannel channel) throws IOException {
        /*创建 binglogdump 请求包*/

        HeaderPacket headerPacket = new HeaderPacket();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        /*默认值  [12] COM_BINLOG_DUMP 占一个字节*/
        byteArrayOutputStream.write((byte) 0x12);
        /*  binlog-pos   4个字节 ，我们就从4开始*/
        int binlogPosition = 4; //1009 414 414
        byte fouthByte = (byte) (binlogPosition & 0xFF);
        byte thirdByte = (byte) (binlogPosition >>> 8);
        byte secondByte = (byte) (binlogPosition >>> 16);
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
        fouthByte = (byte) (serverId & 0xFF);
        thirdByte = (byte) (serverId >>> 8);
        secondByte = (byte) (serverId >>> 16);
        firstByte = (byte) (serverId >>> 24);

        byteArrayOutputStream.write(fouthByte);
        byteArrayOutputStream.write(thirdByte);
        byteArrayOutputStream.write(secondByte);
        byteArrayOutputStream.write(firstByte);
        /*binlog filename */
        String binlogFilename = "binlog.000015"; //binlog.000013 binlog.000015 binlog.000017
        byteArrayOutputStream.write(binlogFilename.getBytes());
        byte[] bytes = byteArrayOutputStream.toByteArray();
        System.out.println("asdasdas:::::::::::" + Arrays.toString(bytes));
        headerPacket.setPacketBodyLength(bytes.length);
        headerPacket.setPacketSequenceNumber((byte) 0x00);
        byteArrayOutputStream.reset();
        byteArrayOutputStream.write(headerPacket.toBytes());
        byteArrayOutputStream.write(bytes);
        channel.write(byteArrayOutputStream.toByteArray());
    }

    private static byte[] joinByteArray(byte[] join, byte[] join1) {
        int length = join.length + join1.length;
        byte[] newByteArray = new byte[length];
        System.arraycopy(join, 0, newByteArray, 0, join.length);
        System.arraycopy(join1, 0, newByteArray, join.length, join1.length);
        return newByteArray;

    }

    private static byte[] join(byte[] serverReponseHandShakePackage, int offset, int end) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        for (int i = offset; i <= end; i++) {
            byteArrayOutputStream.write(serverReponseHandShakePackage[i]);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private static int indexOf(byte[] serverReponseHandShakePackage, int value, int offset) {
        for (int i = offset; i < serverReponseHandShakePackage.length; i++) {

            if (serverReponseHandShakePackage[i] == value) {
                return i;
            }
        }
        return -1;
    }

    private static int get32Int(byte fouth, byte third, byte second, byte first) {
        int forthByte = (int) fouth & 0xFF;
        int thirdByte = ((int) third & 0xFF) << 8;
        int secondByte = ((int) second & 0xFF) << 16;
        int firstByte = ((int) first & 0xFF) << 24;
        return firstByte | secondByte | thirdByte | forthByte;
    }

    private static int get16Int(byte second, byte first) {

        int secondByte = ((int) second & 0xFF);
        int firstByte = ((int) first & 0xFF) << 8;
        return firstByte | secondByte;
    }

    private static void connect(SocketChannel channel) throws IOException, NoSuchAlgorithmException {
        /* 获取server端 header package*/
        byte[] headerPackageByte = channel.read(4, 9999);
        System.out.println(Arrays.toString(headerPackageByte));
        /*有效数据包长度*/
        int packageBodyLength = (headerPackageByte[0]) & 0xff | ((int) headerPackageByte[1] & 0xff << 8) | ((int) headerPackageByte[2] & 0xff << 16);
        /* package序列号 从0开始以1为递增量递增*/
        int sequenceNumber = headerPackageByte[3];
        System.out.println("packageBodyLength:'" + packageBodyLength + "'sequenceNumber:'" + sequenceNumber + "'");
        /* 获取server端 handshake package 部分*/
        byte[] serverReponseHandShakePackage = channel.read(packageBodyLength, 9999);
        /*String  protocolVersion  占用1字节*/
        int num = 0;
        System.out.println("protocolVersion:'" + Byte.toString(serverReponseHandShakePackage[num]) + "'");
        num++;
        /*server version 字符串，到0x00截止 */
        int serverVersionEndIndex = indexOf(serverReponseHandShakePackage, 0x00, 0);
        byte[] bytes = join(serverReponseHandShakePackage, num, serverVersionEndIndex - 1);
        System.out.println("serverVersion:'" + new String(bytes) + "'");
        num = serverVersionEndIndex;
        /*connection id  4个字节 需要计算得出*/
        int connectionId = get32Int(serverReponseHandShakePackage[++num], serverReponseHandShakePackage[++num], serverReponseHandShakePackage[++num], serverReponseHandShakePackage[++num]);
        System.out.println("connectionId:'" + connectionId + "'");
        /*auth-plugin-data-part1   8字节*/
        byte[] sha1EncryptPart1 = join(serverReponseHandShakePackage, ++num, num += 7);
        /* filler_1 (1) -- 0x00(固定) */
        byte filler = serverReponseHandShakePackage[++num];
        /* capability flags int */
        int capabilityFlagsLower = get16Int(serverReponseHandShakePackage[++num], serverReponseHandShakePackage[++num]);
        System.out.println("capabilityFlags:'" + capabilityFlagsLower + "'");
        /*如果还*/
        if (serverReponseHandShakePackage.length > num) {
            /* charset 33  0x21  utf8_general_ci*/
            byte charSet = serverReponseHandShakePackage[++num];
            System.out.println("charset:'" + Byte.toString(charSet) + "'");
            /*status flags   SERVER_STATUS_AUTOCOMMIT	0x0002	auto-commit is enabled*/
            int statusFlags = get16Int(serverReponseHandShakePackage[++num], serverReponseHandShakePackage[++num]);
            System.out.println("statusFlags:'" + statusFlags + "'");
            /*capability flags  */
            int capabilityFlagsUpper = get16Int(serverReponseHandShakePackage[++num], serverReponseHandShakePackage[++num]);
            /*capabilities =  */
            int capabilityFlags = (capabilityFlagsUpper << 16) | capabilityFlagsLower;
            System.out.println("capabilities:'" + capabilityFlags + "'");
            /*length of auth-plugin-data or 00 占1字节 */
            byte b = serverReponseHandShakePackage[++num];
            System.out.println("length of auth-plugin-data:'" + b + "'");
            /*reserved 预留10字节*/
            num += 10;
            /*auth-plugin-data part 2*/
            byte[] sha1EncryptPart2 = join(serverReponseHandShakePackage, ++num, num += ((int) b - sha1EncryptPart1.length - 2));
            num++;
            /*此时值为 0x00 */
            /*auth-plugin-name*/
            int authPluginNameEndIndex = indexOf(serverReponseHandShakePackage, 0x00, ++num);
            byte[] join2 = join(serverReponseHandShakePackage, num, authPluginNameEndIndex - 1);
            String authPluginName = new String(join2);
            System.out.println("authPluginName:'" + authPluginName + "'");
            /*构建客户端认证包 */
            clientAuthResponse(channel, sequenceNumber, sha1EncryptPart1, charSet, sha1EncryptPart2, authPluginName);
        }
    }

    private static void clientAuthResponse(SocketChannel channel, int sequenceNumber, byte[] sha1EncryptPart1, byte charSet, byte[] sha1EncryptPart2, String authPluginName) throws IOException, NoSuchAlgorithmException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        /* clientCapabilities  猜测是客户端要求server端使用这些要求去解析客户端认证包 */
        int clientCapabilities = Capability.CLIENT_LONG_PASSWORD | Capability.CLIENT_LONG_FLAG
                | Capability.CLIENT_PROTOCOL_41 | Capability.CLIENT_INTERACTIVE
                | Capability.CLIENT_TRANSACTIONS | Capability.CLIENT_SECURE_CONNECTION
                | Capability.CLIENT_MULTI_STATEMENTS | Capability.CLIENT_PLUGIN_AUTH;
        System.out.println("clientCapabilities:'" + clientCapabilities);
        /*byte 数组每个元素存储8位 int 32 位占4个成员 */
        write32Int(byteArrayOutputStream, clientCapabilities);
        /*max-packet size   16M 固定  int 4个字节*/
        int maxPackageSize = 1 << 24;
        write32Int(byteArrayOutputStream, maxPackageSize);
        /*charset   字符集 */
        byteArrayOutputStream.write(charSet);
        /* 此处保留23个字节*/
        for (int j = 0; j < 23; j++) {
            byteArrayOutputStream.write(0x00);
        }
        /* 认证用户名 */
        byteArrayOutputStream.write("root".getBytes());
        byteArrayOutputStream.write(0x00);
        /* auth_plugin 选择mysql_native_plugin */
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
        sha1.reset();
        sha1.update(sha1Encrypt);
        byte[] thirdEncrypt = sha1.digest(secondEncrypt);
        for (int i1 = 0; i1 < thirdEncrypt.length; i1++) {
            thirdEncrypt[i1] = (byte) (thirdEncrypt[i1] ^ firstEncrypt[i1]);
        }
        byteArrayOutputStream.write((byte) thirdEncrypt.length);
        byteArrayOutputStream.write(thirdEncrypt);

        /*  capabilities & CLIENT_CONNECT_WITH_DB 进入数据库名,在客户端需求中没有| CLIENT_CONNECT_WITH_DB 所以不写入*/

        /*auth plugin name 认证插件名 我们使用 mysql_native_password*/
        byteArrayOutputStream.write(authPluginName.getBytes());
        byteArrayOutputStream.write(0x00);
        /*封装好认证包*/
        byte[] clientAuthResponsePackageBytes = byteArrayOutputStream.toByteArray();

        /*发送headerPakage*/

        /*header包 auth包 二合一 */
        byte[] authHeaderPackageBytes =
                {
                        (byte) clientAuthResponsePackageBytes.length,
                        (byte) (clientAuthResponsePackageBytes.length >> 8),
                        (byte) (clientAuthResponsePackageBytes.length >> 16),
                        (byte) ++sequenceNumber
                };
        byte[] fullClientAuthPackageBytes = joinByteArray(authHeaderPackageBytes, clientAuthResponsePackageBytes);
        System.out.println("clientResponsePackage:'" + Arrays.toString(fullClientAuthPackageBytes) + "'");
        /*发包*/
        channel.write(fullClientAuthPackageBytes);
        /*此时包已发完*/
        byte[] serverAuthResponse = channel.read(4, 9999);
        int serverAuthResponsePackageBodyLength =
                (int) serverAuthResponse[0] & 0xff | ((int) serverAuthResponse[1] & 0xff << 8) | ((int) serverAuthResponse[2] & 0xff << 16);
        byte[] body = channel.read(serverAuthResponsePackageBodyLength, 9999);
        System.out.println("serverAuthResponsePackageBodyLength:'" + serverAuthResponsePackageBodyLength + "'");
        if (body[0] == 0) {
            System.out.println("认证成功");
        } else if ((int) body[0] == 254) {
            System.out.println("认证包数据缺失");
        } else {
            System.out.println("认证失败");

        }
    }

    private static void write32Int(ByteArrayOutputStream byteArrayOutputStream, int clientCapabilities) {
        byteArrayOutputStream.write((byte) (clientCapabilities & 0xFF));
        byteArrayOutputStream.write((byte) (clientCapabilities >>> 8));
        byteArrayOutputStream.write((byte) (clientCapabilities >>> 16));
        byteArrayOutputStream.write((byte) (clientCapabilities >>> 24));
    }

    private static void formatBinlog(SocketChannel channel) throws IOException {
        int count = 0;
        while (true) {
            count++;
            byte[] headerPackageBytes = channel.read(4, 9999);
            int bodyLength = get24Int(headerPackageBytes[0], headerPackageBytes[1], headerPackageBytes[2]);
            System.out.println(Arrays.toString(headerPackageBytes));
            System.out.println("bodyLength:'" + bodyLength + "'");
            if (bodyLength>0) {
                int squenceNumber = headerPackageBytes[3];
                byte[] eventPackageHeaderBytes = channel.read(20, 9999);
                System.out.println(Arrays.toString(eventPackageHeaderBytes));
                /*取 binlog事件头 */
                int index = 0;
                int timestamp = get32Int(eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index]);
                String date = timeToString(timestamp);
                System.out.println("本次事件发生时间:'" + date + "'");
                /* 事件类型*/
                int binglogEvenType = eventPackageHeaderBytes[++index];
                System.out.println("事件类型:'" + binglogEvenType + "'" + index);
                /*server id*/
                int serverId = get32Int(eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index]);
                System.out.println("serverId:'" + serverId + "'" + index);
                /*事件大小*/
                int eventSize = get32Int(eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index]);
                System.out.println("eventSize:'" + eventSize + "'" + index);
                /* binlog位置*/
                int binlogPosition = get32Int(eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index]);
                System.out.println("binlogPosition:'" + binlogPosition + "'" + index);
                /* binlog 事件标志*/
                int binlogEventFlag = get16Int(eventPackageHeaderBytes[++index], eventPackageHeaderBytes[++index]);
                System.out.println("binlogEventFlag:'" + binlogEventFlag + "'" + index);
                byte[] eventBody  = channel.read(eventSize-19,9999);

                switch (binglogEvenType) {
                    case BinlogEvent.ROTATE_EVENT:
                        System.out.println("本次事件为ROTATE事件");
                        System.out.println("当前binlog文件:'"+new String (eventBody)+"'");
                        break;
                    case BinlogEvent.FORMAT_DESCRIPTION_EVENT:
                        System.out.println("本次事件为FORMAT_DESCRIPTION事件");
                        break;
                    case BinlogEvent.TABLE_MAP_EVENT:
                        System.out.println("本次事件为TABLE_MAP事件");
                        break;
                    case BinlogEvent.WRITE_ROWS_EVENT_V0:
                    case BinlogEvent.WRITE_ROWS_EVENT_V1:
                    case BinlogEvent.WRITE_ROWS_EVENT_V2:
                        System.out.println("本次事件为WRITE_ROWS事件");
                        break;
                    case BinlogEvent.DELETE_ROWS_EVENT_V0:
                    case BinlogEvent.DELETE_ROWS_EVENT_V1:
                    case BinlogEvent.DELETE_ROWS_EVENT_V2:
                        System.out.println("本次事件为DELETE_ROWS事件");
                        break;
                    case BinlogEvent.UPDATE_ROWS_EVENT_V0:
                    case BinlogEvent.UPDATE_ROWS_EVENT_V1:
                    case BinlogEvent.UPDATE_ROWS_EVENT_V2:
                        System.out.println("本次事件为UPDATE_ROWS事件");
                        break;
                    case BinlogEvent.XID_EVENT:
                        System.out.println("commit事件");
                    default:
                        break;
                }
            }
//            formatRotateEvent(channel);
//            formatDescriptionEvent(channel);
//            formatTableMapEvent(channel);
//            formatRowEvent(channel);
        }


    }

    private static String timeToString(int timestamp) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        date.setTime((long)timestamp*1000);
        return simpleDateFormat.format(date);
    }

    private static int get24Int(byte third, byte second, byte first) {
        return (third & 0xff) | ((second & 0xff) << 8) | ((first & 0xff) << 16);
    }
    private static void formatDescriptionEvent(SocketChannel channel) throws IOException {
        byte[] format = channel.read(4, 9999);
        int i = (format[0] & 0xff) | (format[1] & 0xff << 8) | (format[2] & 0xff << 16);
        System.out.println(i);
        byte[] read = channel.read(i, 9999);
        System.out.println(Arrays.toString(read));
        System.out.println(new String(join(read, 20, 70)));
//        System.out.println(new String(join(read, 71, 74)));
    }

    private static void formatRotateEvent(SocketChannel channel) throws IOException {
        byte[] rotateHeader = channel.read(4, 9999);
        System.out.println(Arrays.toString(rotateHeader));
        int i1 = (rotateHeader[0] & 0xff) | (rotateHeader[1] & 0xff << 8) | (rotateHeader[2] & 0xff << 16);
        System.out.println("i1:'" + i1 + "'");
        byte[] read1 = channel.read(i1, 9999);
        System.out.println(Arrays.toString(read1));
        System.out.println("294:'" + new String(read1));
    }

    private static void formatRowEvent(SocketChannel channel) throws IOException {
        byte[] read4 = channel.read(4, 9999);
        int k = (read4[0] & 0xff) | (read4[1] & 0xff << 8) | (read4[2] & 0xff << 16);
        System.out.println(Arrays.toString(read4));
        byte[] read5 = channel.read(k, 9999);
        System.out.println(Arrays.toString(read5));
        System.out.println("tableId:'" + (read5[20] & 0xff) + "'");
        System.out.println("flag:'" + ((read5[26] & 0xff) | (read5[27] & 0xff << 8)) + "'");
        System.out.println("effort columns:'" + (read5[28] & 0xff) + "'");
        BitSet bitMap = new BitSet(5);
        int index = 29;
        int flag;
        for (int i2 = 0; i2 < (read5[28] & 0xff); i2 += 8) {
            if ((flag = read5[index++]) != 0) {
                if ((flag & 0x01) != 0) bitMap.set(i2);
                if ((flag & 0x02) != 0) bitMap.set(i2 + 1);
                if ((flag & 0x04) != 0) bitMap.set(i2 + 2);
                if ((flag & 0x08) != 0) bitMap.set(i2 + 3);
                if ((flag & 0x10) != 0) bitMap.set(i2 + 4);
                if ((flag & 0x20) != 0) bitMap.set(i2 + 5);
                if ((flag & 0x40) != 0) bitMap.set(i2 + 6);
                if ((flag & 0x80) != 0) bitMap.set(i2 + 7);
            }
        }
        System.out.println(bitMap.toString());
        String s = "400";
        System.out.println(((byte) (-1) & 0xff));
        System.out.println(-32 & 0xff);
        System.out.println(Arrays.toString(s.getBytes()));
    }

    private static void formatTableMapEvent(SocketChannel channel) throws IOException {
        int index = 19;
        byte[] read3 = channel.read(4, 9999);
        System.out.println(Arrays.toString(read3));
        int j = (read3[0] & 0xff) | (read3[1] & 0xff << 8) | (read3[2] & 0xff << 16);
        byte[] read2 = channel.read(j, 9999);
        System.out.println(Arrays.toString(read2));
        /* table id 6 bytes */
        index += 6;
        /* flag 2 bytes*/
        index += 2;
        byte databaseNameLength = read2[++index];
        System.out.println("database name length :'" + databaseNameLength + "'");
        System.out.println("database name :'" + new String(join(read2, ++index, index + databaseNameLength)) + "'");
        index += databaseNameLength;
        byte tableNameLength = read2[++index];
        System.out.println("table name length:'" + tableNameLength);
        System.out.println("table name :'" + new String(join(read2, ++index, index + tableNameLength)) + "'");
        /*列数量*/
        index += (tableNameLength + 1);
        byte columnCount = read2[index];
        System.out.println("column count :'" + columnCount + "'");
        /*字段类型*/
        for (int i = 0; i < 5; i++) {
            ++index;
            System.out.print("字符类型:'" + read2[index] + "',");
        }
        System.out.println();
        /*字段总长*/
        System.out.println("metainfo length :'" + (read2[++index] & 0xff) + "'");
        /* 字段长度 */
        for (int i = 0; i < 6; i++) {
            ++index;
            System.out.print("字段长度:'" + (read2[index] & 0xff) + "',");
        }
        System.out.println();


//        System.out.println(read[0]);
        /*table map 事件*/
    }

    private void sendRegisterSlave(SocketChannel channel) throws IOException {
        RegisterSlaveCommandPacket cmd = new RegisterSlaveCommandPacket();
        cmd.reportHost = "192.168.56.251";
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
        byteArrayOutputStream.write((byte) 0x13);
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
        headerPacket.setPacketSequenceNumber((byte) 0x00);

        byteArrayOutputStream.reset();
        byteArrayOutputStream.write(headerPacket.toBytes());
        byteArrayOutputStream.write(bytes);

        channel.write(byteArrayOutputStream.toByteArray());
    }


}
