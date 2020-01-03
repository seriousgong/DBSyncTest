package com.own.mariadb;

/**
 *
 * @author GongCheng
 * @date 2020/1/2 17:36
 *
 */
public class BinlogEvent {
    public final static int  UNKNOWN_EVENT = 0x00;

    public final static int  START_EVENT_V3 = 0x01;

    public final static int  QUERY_EVENT = 0x02;

    public final static int  STOP_EVENT = 0x03;

    public final static int  ROTATE_EVENT = 0x04;

    public final static int  INTVAR_EVENT = 0x05;

    public final static int  LOAD_EVENT = 0x06;

    public final static int  SLAVE_EVENT = 0x07;

    public final static int  CREATE_FILE_EVENT = 0x08;

    public final static int  APPEND_BLOCK_EVENT = 0x09;

    public final static int  EXEC_LOAD_EVENT = 0x0a;

    public final static int  DELETE_FILE_EVENT = 0x0b;

    public final static int  NEW_LOAD_EVENT = 0x0c;

    public final static int  RAND_EVENT = 0x0d;

    public final static int  USER_VAR_EVENT = 0x0e;

    public final static int  FORMAT_DESCRIPTION_EVENT = 0x0f;

    public final static int  XID_EVENT = 0x10;

    public final static int  BEGIN_LOAD_QUERY_EVENT = 0x11;

    public final static int  EXECUTE_LOAD_QUERY_EVENT = 0x12;

    public final static int  TABLE_MAP_EVENT = 0x13;

    public final static int  WRITE_ROWS_EVENT_V0 = 0x14;

    public final static int  UPDATE_ROWS_EVENT_V0 = 0x15;

    public final static int  DELETE_ROWS_EVENT_V0 = 0x16;

    public final static int  WRITE_ROWS_EVENT_V1 = 0x17;

    public final static int  UPDATE_ROWS_EVENT_V1 = 0x18;

    public final static int  DELETE_ROWS_EVENT_V1 = 0x19;

    public final static int  INCIDENT_EVENT = 0x1a;

    public final static int  HEARTBEAT_EVENT = 0x1b;

    public final static int  IGNORABLE_EVENT = 0x1c;

    public final static int  ROWS_QUERY_EVENT = 0x1d;

    public final static int  WRITE_ROWS_EVENT_V2 = 0x1e;

    public final static int  UPDATE_ROWS_EVENT_V2 = 0x1f;

    public final static int  DELETE_ROWS_EVENT_V2 = 0x20;

    public final static int  GTID_EVENT = 0x21;

    public final static int  ANONYMOUS_GTID_EVENT = 0x22;

    public final static int  PREVIOUS_GTIDS_EVENT = 0x23;
}
