package com.zdjizhi.pojo;

public class Gtp {

    private String gtp_apn;
    private String gtp_imei;
    private String gtp_imsi;
    private String gtp_phone_number;
    private Long gtp_teid;
    private Integer msg_type;
    private Long last_update_time;


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

    public Long getGtp_teid() {
        return gtp_teid;
    }

    public void setGtp_teid(Long gtp_teid) {
        this.gtp_teid = gtp_teid;
    }

    public Long getLast_update_time() {
        return last_update_time;
    }

    public void setLast_update_time(Long last_update_time) {
        this.last_update_time = last_update_time;
    }
}
