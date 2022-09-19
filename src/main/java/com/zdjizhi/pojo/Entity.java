package com.zdjizhi.pojo;

public class Entity {

    private String Hashkey;
    private int ifError;

    private String gtp_apn;
    private String gtp_imei;
    private String gtp_imsi;
    private String gtp_phone_number;
    private long gtp_uplink_teid;
    private long gtp_downlink_teid	;
    private String gtp_msg_type;
    private long common_recv_time;
    private String gtp_uuid;


    public String getHashkey() {
        return Hashkey;
    }

    public void setHashkey(String hashkey) {
        Hashkey = hashkey;
    }

    public int getIfError() {
        return ifError;
    }

    public void setIfError(int ifError) {
        this.ifError = ifError;
    }

    public String getGtp_apn() {
        return gtp_apn;
    }

    public void setGtp_apn(String gtp_apn) {
        this.gtp_apn = gtp_apn;
    }

    public String getGtp_imei() {
        return gtp_imei;
    }

    public void setGtp_imei(String gtp_imei) {
        this.gtp_imei = gtp_imei;
    }

    public String getGtp_imsi() {
        return gtp_imsi;
    }

    public void setGtp_imsi(String gtp_imsi) {
        this.gtp_imsi = gtp_imsi;
    }

    public String getGtp_phone_number() {
        return gtp_phone_number;
    }

    public void setGtp_phone_number(String gtp_phone_number) {
        this.gtp_phone_number = gtp_phone_number;
    }

    public long getGtp_uplink_teid() {
        return gtp_uplink_teid;
    }

    public void setGtp_uplink_teid(long gtp_uplink_teid) {
        this.gtp_uplink_teid = gtp_uplink_teid;
    }

    public long getGtp_downlink_teid() {
        return gtp_downlink_teid;
    }

    public void setGtp_downlink_teid(long gtp_downlink_teid) {
        this.gtp_downlink_teid = gtp_downlink_teid;
    }

    public String getGtp_msg_type() {
        return gtp_msg_type;
    }

    public void setGtp_msg_type(String gtp_msg_type) {
        this.gtp_msg_type = gtp_msg_type;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public String getGtp_uuid() {
        return gtp_uuid;
    }

    public void setGtp_uuid(String gtp_uuid) {
        this.gtp_uuid = gtp_uuid;
    }
}
