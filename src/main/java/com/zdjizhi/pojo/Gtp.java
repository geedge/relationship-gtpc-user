package com.zdjizhi.pojo;

public class Gtp {

    private String gtp_apn;
    private String gtp_imei;
    private String gtp_imsi;
    private String gtp_phone_number;
    private Integer msg_type;
    private long last_update_time;
    private long gtp_uplink_teid;
    private long gtp_downlink_teid	;
    private String gtp_uuid;

    public Integer getMsg_type() {
        return msg_type;
    }

    public void setMsg_type(Integer msg_type) {
        this.msg_type = msg_type;
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

    public String getGtp_uuid() {
        return gtp_uuid;
    }

    public void setGtp_uuid(String gtp_uuid) {
        this.gtp_uuid = gtp_uuid;
    }

    public long getLast_update_time() {
        return last_update_time;
    }

    public void setLast_update_time(long last_update_time) {
        this.last_update_time = last_update_time;
    }
}
