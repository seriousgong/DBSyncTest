package com.own.mariadb;

import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannelPool;

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
 * @author GongCheng
 * @date 2020/1/3 15:53
 */
public class MariadbMasterSlaveProtocolImp {
    private static byte[] columnTypeArray;
    private static int[] columnMetaValueArray;

    public static void main(String[] args) throws Exception {
        /* 建立socker通信*/
        SocketChannel channel = SocketChannelPool.open(new InetSocketAddress("192.168.56.251", 14544));
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
        int binlogPosition = 4;
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
        String binlogFilename = "binlog.000031";
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
        int thirdByte = (third & 0xFF) << 8;
        int secondByte = (second & 0xFF) << 16;
        int firstByte = (first & 0xFF) << 24;
        return firstByte | secondByte | thirdByte | forthByte;
    }

    private static int get16Int(byte second, byte first) {

        int secondByte = ((int) second & 0xFF);
        int firstByte = ((int) first & 0xFF) << 8;
        return firstByte | secondByte;
    }

    private static void connect(SocketChannel channel) throws IOException, NoSuchAlgorithmException {
        /* 获取server端 header package*/
        byte[] headerPackageByte = channel.read(4, 99999);
        System.out.println(Arrays.toString(headerPackageByte));
        /*有效数据包长度*/
        int packageBodyLength = (headerPackageByte[0]) & 0xff | ((int) headerPackageByte[1] & 0xff << 8) | ((int) headerPackageByte[2] & 0xff << 16);
        /* package序列号 从0开始以1为递增量递增*/
        int sequenceNumber = headerPackageByte[3];
        System.out.println("packageBodyLength:'" + packageBodyLength + "'sequenceNumber:'" + sequenceNumber + "'");
        /* 获取server端 handshake package 部分*/
        byte[] serverReponseHandShakePackage = channel.read(packageBodyLength, 99999);
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
         ++num;
        /* capability flags int */
        int capabilityFlagsLower = get16Int(serverReponseHandShakePackage[++num], serverReponseHandShakePackage[++num]);
        System.out.println("capabilityFlags:'" + capabilityFlagsLower + "'");
        /*如果还*/
//        if (serverReponseHandShakePackage.length > num) {
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
//        }
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
        System.out.println("maxPackageSize:'"+maxPackageSize+"'");
        write32Int(byteArrayOutputStream, maxPackageSize);
        /*charset   字符集 */
        byteArrayOutputStream.write(charSet);
        /* 此处保留23个字节*/
        int reservedBytes = 23;
        for (int j = 0; j < reservedBytes; j++) {
            byteArrayOutputStream.write(0x00);
        }
        /* 认证用户名 */
        byteArrayOutputStream.write("root".getBytes());
        byteArrayOutputStream.write(0x00);
        /* auth_plugin 选择mysql_native_plugin  'SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1(SHA1( password )))' */
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
        byte[] serverAuthResponse = channel.read(4, 99999);
        int serverAuthResponsePackageBodyLength =
                (int) serverAuthResponse[0] & 0xff | ((int) serverAuthResponse[1] & 0xff << 8) | ((int) serverAuthResponse[2] & 0xff << 16);
        byte[] body = channel.read(serverAuthResponsePackageBodyLength, 99999);
        System.out.println(Arrays.toString(body));
        System.out.println("serverAuthResponsePackageBodyLength:'" + serverAuthResponsePackageBodyLength + "'");
        final int statusEOF=254;
        if (body[0] == 0) {
            System.out.println("认证成功");
        } else if ((int) body[0] == statusEOF) {
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
        while (true) {
            byte[] headerPackageBytes = channel.read(4, 99999);
            int bodyLength = get24Int(headerPackageBytes[0], headerPackageBytes[1], headerPackageBytes[2]);
            System.out.println(Arrays.toString(headerPackageBytes));
            System.out.println("bodyLength:'" + bodyLength + "'");
            if (bodyLength > 0) {
                int squenceNumber = headerPackageBytes[3];
                System.out.println("squenceNumber:'" + squenceNumber + "'");
                /*取 binlog事件头 */
                byte[] eventPackageHeaderBytes = channel.read(20, 99999);
                System.out.println(Arrays.toString(eventPackageHeaderBytes));
                parseBinlogEvent(channel, eventPackageHeaderBytes);
            }
        }


    }

    private static void parseBinlogEvent(SocketChannel channel, byte[] eventPackageHeaderBytes) throws IOException {
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
        byte[] eventBody = channel.read(eventSize - 19, 99999);
        System.out.println(Arrays.toString(eventBody));
        /**/

        switch (binglogEvenType) {
            case BinlogEvent.ROTATE_EVENT:
                System.out.println("本次事件为ROTATE事件");
                System.out.println("当前binlog文件:'" + new String(eventBody) + "'");
                break;
            case BinlogEvent.FORMAT_DESCRIPTION_EVENT:
                System.out.println("本次事件为FORMAT_DESCRIPTION事件");
                break;
            case BinlogEvent.TABLE_MAP_EVENT:
                System.out.println("本次事件为TABLE_MAP事件");
                parseTableMapEvent(eventBody);
                break;
            case BinlogEvent.WRITE_ROWS_EVENT_V0:
            case BinlogEvent.WRITE_ROWS_EVENT_V1:
            case BinlogEvent.WRITE_ROWS_EVENT_V2:
                System.out.println("本次事件为WRITE_ROWS事件");
                parseWriteRowsEvent(eventBody);
                break;
            case BinlogEvent.DELETE_ROWS_EVENT_V0:
            case BinlogEvent.DELETE_ROWS_EVENT_V1:
            case BinlogEvent.DELETE_ROWS_EVENT_V2:
                System.out.println("本次事件为DELETE_ROWS事件");
                parseWriteRowsEvent(eventBody);
                break;
            case BinlogEvent.UPDATE_ROWS_EVENT_V0:
            case BinlogEvent.UPDATE_ROWS_EVENT_V1:
            case BinlogEvent.UPDATE_ROWS_EVENT_V2:
                System.out.println("本次事件为UPDATE_ROWS事件");
                break;
            case BinlogEvent.XID_EVENT:
                System.out.println("commit事件");
//                System.exit(0);
            default:
                break;
        }
    }

    private static void parseTableMapEvent(byte[] eventBody) {
        /*已知该事件为写事件*/
        int index = 0;
        /* table map事件中,table id 占6字节*/
        index = parseBinlogEventHeader(eventBody, index);
        /* schema name length  占1个字节 */
        int dataBaseLength = (int) eventBody[++index];
        System.out.println("dataBaseLength:'" + dataBaseLength + "'");
        /*  schema name  */
        byte[] dataBaseNameBytes = join(eventBody, ++index, index += (dataBaseLength - 1));
        String dataBaseName = new String(dataBaseNameBytes);
        System.out.println("dataBaseName:'" + dataBaseName + "'");
        /* 0x00 截止 */
        index++;
        /* table name length */
        int tableNameLength = eventBody[++index];
        System.out.println("tableNameLength:'" + tableNameLength + "'");
        /*  table name */
        byte[] tableNameBytes = join(eventBody, ++index, index += (tableNameLength - 1));
        String tableName = new String(tableNameBytes);
        System.out.println("tableName:'" + tableName + "'");
        /* 0x00 截止*/
        index++;
        /* column count */
        int columnCount = eventBody[++index];
        System.out.println("columnCount:'" + columnCount + "'");
        /* column def 是一个数组 */
        byte[] columnDefBytes = join(eventBody, ++index, index += (columnCount - 1));

//        String columnDef = new String(columnDefBytes);
        /* column meta def length */
        int metaLength = eventBody[++index];
        /* column meta def  是一个数组 */
        byte[] columnMetaDefBytes = join(eventBody, ++index, index += (metaLength - 1));
        MariadbMasterSlaveProtocolImp.columnTypeArray = columnDefBytes;
        MariadbMasterSlaveProtocolImp.columnMetaValueArray = transformMetaValue(columnDefBytes, columnMetaDefBytes);

        System.out.println("columnTypeArray:'" + Arrays.toString(MariadbMasterSlaveProtocolImp.columnTypeArray) + "'");
        System.out.println("metaValueArrays:'" + Arrays.toString(MariadbMasterSlaveProtocolImp.columnMetaValueArray) + "'");
        /* null bitmask*/
        /* 每个bit set 标识 当前字段能否为null 1标识可以 0标识不能 */
        BitSet bitSet = new BitSet();
        index = fillBitMap(eventBody, index, columnCount, bitSet);
        System.out.println("nullable bitmap:'"+bitSet+"'");
        if(index<eventBody.length-1){
            System.out.println("解析出错");
        }else{
            System.out.println("解析完成");
        }
    }

    private static int[] transformMetaValue(byte[] columnDefBytes, byte[] columnMetaDefBytes) {
        int[] columnMetaValueArray = new int[columnDefBytes.length];
        int columnMetaDefBytesIndex = -1;
        for (int i = 0; i < columnDefBytes.length; i++) {
            int columnMetaValue;
            switch ((int) columnDefBytes[i]){
                case ColumnMetaDef.MYSQL_TYPE_VAR_STRING:
                case ColumnMetaDef.MYSQL_TYPE_VARCHAR:
                    columnMetaValue = (columnMetaDefBytes[++columnMetaDefBytesIndex] & 0xff) | ((columnMetaDefBytes[++columnMetaDefBytesIndex] & 0xff) << 8);
                    columnMetaValueArray[i] = columnMetaValue;
                    break;
                case ColumnMetaDef.MYSQL_TYPE_LONG:
                    columnMetaValue = Integer.MAX_VALUE;
                    columnMetaValueArray[i] = columnMetaValue;
                    break;
                case ColumnMetaDef.MYSQL_TYPE_LONGLONG:

                default:
                    break;
            }
//            if (columnDefBytes[i] == ColumnMetaDef.MYSQL_TYPE_VARCHAR) {
//            } else if (columnDefBytes[i] == ColumnMetaDef.MYSQL_TYPE_LONG) {
//                columnMetaValue = Integer.MAX_VALUE;
//                columnMetaValueArray[i] = columnMetaValue;
//            }
        }

        return columnMetaValueArray;
    }

    private static void parseWriteRowsEvent(byte[] eventBody) {
        /*已知该事件为写事件*/
        int index = 0;
        index = parseBinlogEventHeader(eventBody, index);
        /* column num  */
        int columnCount = (eventBody[++index] & 0xff);
        System.out.println("columnNum:'" + columnCount + "'");
        /* columns-present-bitmap1 */
        BitSet bitmap1 = new BitSet();
        index = fillBitMap(eventBody, index, columnCount, bitmap1);
        System.out.println("bitMap1Value:'" + bitmap1 + "'");
        while (index < eventBody.length - 1) {
            /* rows nullable  */
            BitSet nullMap = new BitSet();
            index = fillBitMap(eventBody, index, columnCount, nullMap);
            System.out.println("nullMap:'" + nullMap + "'");
            /* rows data 通过table map 事件缓存的 字段 类型 以及长度进行转换 */
            System.out.println("--------------------------------------------->");
            for (int i = 0; i < columnCount; i++) {
                if (!nullMap.get(i)) {
                    switch ( MariadbMasterSlaveProtocolImp.columnTypeArray[i]&0xff){
                        case ColumnMetaDef.MYSQL_TYPE_VAR_STRING:
                        case ColumnMetaDef.MYSQL_TYPE_VARCHAR:
                            int metaLength = MariadbMasterSlaveProtocolImp.columnMetaValueArray[i];
                            int columnValueLength;
                            if (metaLength < 0xfb) {
                                 columnValueLength = eventBody[++index] & 0xff;
                            }else {
                                byte meta1 = eventBody[++index];
                                byte meta2 = eventBody[++index];
                                columnValueLength = get16Int(meta1,meta2);
                            }
                            String columnValueString = new String(join(eventBody, ++index, index += (columnValueLength - 1)));
                            System.out.println("columnValue:'" + columnValueString + "'meta length:'" + MariadbMasterSlaveProtocolImp.columnMetaValueArray[i]);
                            break;
                        case ColumnMetaDef.MYSQL_TYPE_LONG:
                            int columnValueInt= get32Int(eventBody[++index], eventBody[++index], eventBody[++index], eventBody[++index]);
                            System.out.println("columnValue:'" + columnValueInt + "'meta length:'" + Integer.MAX_VALUE);
                            break;
                        case ColumnMetaDef.MYSQL_TYPE_LONGLONG:
                            long columnValueLong = get64Long(join(eventBody,++index,index+=(7)));
                            System.out.println("columnValue:'" + columnValueLong + "'meta length:'" + Long.MAX_VALUE);
                            default:
                    }
                } else {
                    System.out.println("columnValue:'null' meta length:'" + MariadbMasterSlaveProtocolImp.columnMetaValueArray[i]);
                }
            }
            System.out.println("<---------------------------------------------");
            if (index < eventBody.length - 1) {
                System.out.println("此次事件操作语句影响多行,尚未解析完成");
            } else {
                System.out.println("解析完成");
            }
        }
    }

    private static long get64Long(byte[] bytes) {
        long number = 0;
        for (int i = 0; i < bytes.length; i++) {
            number |= ((long) bytes[i] << (i * 8));
        }

        return number;

    }

    private static int parseBinlogEventHeader(byte[] eventBody, int index) {
        /* row 事件中,table id 占6字节*/
        byte[] tableIdBytes = join(eventBody, index, index += 5);
        long tableId = get48Long(tableIdBytes);
        System.out.println("tableId:'" + tableId + "'");
        /*flags*/
        int flags = get16Int(eventBody[++index], eventBody[++index]);
        System.out.println("flags:'" + flags + "'");
        return index;
    }

    private static int fillBitMap(byte[] eventBody, int index, int columnCount, BitSet bitmap1) {
        int bit8 = 8;
        for (int bitLoc = 0; bitLoc < columnCount; bitLoc += bit8) {
            int mapValue = eventBody[++index] & 0xff;
            System.out.println("mapValue:'" + mapValue + "'");
            if ((mapValue & (0x01)) != 0) {
                bitmap1.set(bitLoc);
            }
            if ((mapValue & 0x02) != 0) {
                bitmap1.set(bitLoc + 1);
            }
            if ((mapValue & 0x04) != 0) {
                bitmap1.set(bitLoc + 2);
            }
            if ((mapValue & 0x08) != 0) {
                bitmap1.set(bitLoc + 3);
            }
            if ((mapValue & 0x10) != 0) {
                bitmap1.set(bitLoc + 4);
            }
            if ((mapValue & 0x20) != 0) {
                bitmap1.set(bitLoc + 5);
            }
            if ((mapValue & 0x40) != 0) {
                bitmap1.set(bitLoc + 6);
            }
            if ((mapValue & 0x80) != 0) {
                bitmap1.set(bitLoc + 7);
            }
        }
        return index;
    }


    private static long get48Long(byte[] tableIdBytes) {
        long number = 0;
        for (int i = 0; i < tableIdBytes.length; i++) {
            number |= ((long) tableIdBytes[i] << (i * 8));
        }

        return number;
    }

    private static String timeToString(int timestamp) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        date.setTime((long) timestamp * 1000);
        return simpleDateFormat.format(date);
    }

    private static int get24Int(byte third, byte second, byte first) {
        return (third & 0xff) | ((second & 0xff) << 8) | ((first & 0xff) << 16);
    }


}
