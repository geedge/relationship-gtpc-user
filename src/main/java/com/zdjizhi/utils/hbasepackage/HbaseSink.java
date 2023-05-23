package com.zdjizhi.utils.hbasepackage;


import com.google.common.cache.CacheBuilder;
import com.zdjizhi.common.GtpConfig;
import com.zdjizhi.pojo.Entity;
import com.zdjizhi.pojo.Gtp;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class HbaseSink extends RichSinkFunction<Entity> implements Serializable, SinkFunction<Entity> {
    private Logger log;

    private String hbase_zookeeper_host;
  //  public Map<String, Gtp> gtpConcurrentHashMap = new ConcurrentHashMap<>(80000);
    //public Cache<String, Gtp> gtpConcurrentHashMap = CacheUtil.newLRUCache(4, DateUnit.SECOND.getMillis() * 60);
    public com.google.common.cache.Cache<String, Gtp> gtpConcurrentHashMap  ;

    private Connection connection;
    private Admin admin;

    public HbaseSink(String hbase_zookeeper_host) {
        this.hbase_zookeeper_host = hbase_zookeeper_host;
        gtpConcurrentHashMap=CacheBuilder.newBuilder().expireAfterWrite(GtpConfig.CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS).initialCapacity(100000).maximumSize(GtpConfig.CACHE_MAX_SIZE).build();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log = Logger.getLogger(HbaseSink.class);


            org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_host);
            configuration.set("hbase.rpc.timeout", String.valueOf(GtpConfig.HBASE_RPC_TIMEOUT));
            configuration.set("zookeeper.recovery.retry", "1");
            configuration.set("hbase.client.retries.number", "1");


            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();

            try {

                Table table = connection.getTable(TableName.valueOf(GtpConfig.RELATION_USER_TEID_TABLE_NAME));
                Scan scan = new Scan();
                scan.addColumn("gtp".getBytes(), "uplink_teid".getBytes());
                scan.addColumn("gtp".getBytes(), "downlink_teid".getBytes());
                scan.addColumn("gtp".getBytes(), "apn".getBytes());
                scan.addColumn("gtp".getBytes(), "phone_number".getBytes());
                scan.addColumn("gtp".getBytes(), "imsi".getBytes());
                scan.addColumn("gtp".getBytes(), "imei".getBytes());
                scan.addColumn("gtp".getBytes(), "last_update_time".getBytes());
                scan.addColumn("gtp".getBytes(), "msg_type".getBytes());
                scan.addColumn("common".getBytes(), "vsys_id".getBytes());

                if (GtpConfig.HBASE_SCAN_LIMIT != 0) {
                    scan.setLimit(GtpConfig.HBASE_SCAN_LIMIT);
                }

                ResultScanner scanner = table.getScanner(scan);
                for (Result result : scanner) {
                    if (result.containsColumn("common".getBytes(), "vsys_id".getBytes()) && result.containsColumn("gtp".getBytes(), "uplink_teid".getBytes()) && result.containsColumn("gtp".getBytes(), "downlink_teid".getBytes()) && result.containsColumn("gtp".getBytes(), "msg_type".getBytes()) && result.containsColumn("gtp".getBytes(), "apn".getBytes()) && result.containsColumn("gtp".getBytes(), "last_update_time".getBytes()) && result.containsColumn("gtp".getBytes(), "imei".getBytes()) && result.containsColumn("gtp".getBytes(), "phone_number".getBytes()) && result.containsColumn("gtp".getBytes(), "imsi".getBytes())) {

                        Gtp gtp = new Gtp();
                        String key = Bytes.toString(result.getRow());

                        Long uplink_teid = Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("uplink_teid"))));
                        Long downlink_teid = Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("downlink_teid"))));
                        int msg_type = Bytes.toInt(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("msg_type"))));
                        String apn = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("apn"))));
                        String phone_number = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("phone_number"))));
                        String imsi = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("imsi"))));
                        String imei = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("imei"))));
                        Long last_update_time = Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("gtp"), Bytes.toBytes("last_update_time"))));
                        int vsys_id = Bytes.toInt(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("common"), Bytes.toBytes("vsys_id"))));

                        gtp.setLast_update_time(last_update_time);
                        gtp.setGtp_uplink_teid(uplink_teid);
                        gtp.setGtp_downlink_teid(downlink_teid);
                        gtp.setGtp_apn(apn);
                        gtp.setGtp_phone_number(phone_number);
                        gtp.setGtp_imsi(imsi);
                        gtp.setGtp_imei(imei);
                        gtp.setMsg_type(msg_type);
                        gtp.setGtp_uuid(DigestUtils.md5Hex(gtp.getGtp_uplink_teid() + "|" + gtp.getGtp_downlink_teid()));
                        gtp.setCommon_vsys_id(vsys_id);

                        gtpConcurrentHashMap.put(key, gtp);
                    }
                }
                scanner.close();
            } catch (IOException ioe) {
                log.error("HBaseUtils getAll() is IOException===>{" + ioe + "}<===");
            } catch (RuntimeException e) {
                log.error("HBaseUtils getAll() is Exception===>{" + e + "}<===");
            }

    }

    public void invoke(Entity entity, Context context) throws Exception {
        // 按 project:table 归纳

        Gtp gtp = gtpConcurrentHashMap.getIfPresent(entity.getHashkey());
        if (gtp!=null) {
            if (gtp.getLast_update_time() <= entity.getCommon_recv_time()) {
                if("delete".equals(entity.getGtp_msg_type())){

                    if(gtp.getGtp_uuid().equals(entity.getGtp_uuid())){
                        gtp.setMsg_type(2);
                        ArrayList<Row> rows = new ArrayList<>();
                        //  ArrayList<Row> delrows = getDelRows(gtp);
                        gtp.setLast_update_time(entity.getCommon_recv_time());
                        gtp.setGtp_uplink_teid(entity.getGtp_uplink_teid());
                        gtp.setGtp_downlink_teid(entity.getGtp_downlink_teid());
                        gtp.setGtp_uuid(DigestUtils.md5Hex(gtp.getGtp_uplink_teid() +"|"+ gtp.getGtp_downlink_teid() ));
                        gtp.setCommon_vsys_id(entity.getCommon_vsys_id());
                        gtp.setGtp_apn(entity.getGtp_apn());
                        gtp.setGtp_phone_number(entity.getGtp_phone_number());
                        gtp.setGtp_imsi(entity.getGtp_imsi());
                        gtp.setGtp_imei(entity.getGtp_imei());
                        ArrayList<Row> updaterows = getupdateRows(gtp);
                        rows.addAll(updaterows);
                        updateKnowledgeMessage(rows);
                        updateRelationMessage(entity.getHashkey(), gtp);

                    }
                    else{


                        gtp.setMsg_type(2);
                        ArrayList<Row> rows = new ArrayList<>();
                        gtp.setCommon_vsys_id(entity.getCommon_vsys_id());
                        //  ArrayList<Row> delrows = getDelRows(gtp);
                        gtp.setLast_update_time(entity.getCommon_recv_time());
                        gtp.setGtp_uplink_teid(entity.getGtp_uplink_teid());
                        gtp.setGtp_downlink_teid(entity.getGtp_downlink_teid());
                        gtp.setGtp_uuid(DigestUtils.md5Hex(gtp.getGtp_uplink_teid() +"|"+ gtp.getGtp_downlink_teid() ));
                        gtp.setGtp_apn(entity.getGtp_apn());
                        gtp.setGtp_phone_number(entity.getGtp_phone_number());
                        gtp.setGtp_imsi(entity.getGtp_imsi());
                        gtp.setGtp_imei(entity.getGtp_imei());
                        ArrayList<Row> updaterows = getupdateRows(gtp);
                       // rows.addAll(delrows);
                        rows.addAll(updaterows);
                        updateKnowledgeMessage(rows);
                        //  updateRelationMessage(entity.getHashkey(), gtp);

                    }



                }
                else{

                    if(!gtp.getGtp_uuid().equals(entity.getGtp_uuid()) || !gtp.getGtp_apn().equals(entity.getGtp_apn()) || gtp.getCommon_vsys_id()!=entity.getCommon_vsys_id()){


                        gtp.setMsg_type(1);
                        ArrayList<Row> rows = new ArrayList<>();
                        //  ArrayList<Row> delrows = getDelRows(gtp);
                        gtp.setLast_update_time(entity.getCommon_recv_time());
                        gtp.setGtp_uplink_teid(entity.getGtp_uplink_teid());
                        gtp.setGtp_downlink_teid(entity.getGtp_downlink_teid());
                        gtp.setGtp_uuid(DigestUtils.md5Hex(gtp.getGtp_uplink_teid() +"|"+ gtp.getGtp_downlink_teid() ));
                        gtp.setCommon_vsys_id(entity.getCommon_vsys_id());
                        gtp.setGtp_apn(entity.getGtp_apn());
                        gtp.setGtp_phone_number(entity.getGtp_phone_number());
                        gtp.setGtp_imsi(entity.getGtp_imsi());
                        gtp.setGtp_imei(entity.getGtp_imei());
                        ArrayList<Row> updaterows = getupdateRows(gtp);
                        // rows.addAll(delrows);
                        rows.addAll(updaterows);
                        updateKnowledgeMessage(rows);
                        updateRelationMessage(entity.getHashkey(), gtp);

                    }
                    else {
                        if(entity.getCommon_recv_time()-gtp.getLast_update_time()>GtpConfig.CACHE_UPDATE_SECONDS){


                            gtp.setMsg_type(1);
                            ArrayList<Row> rows = new ArrayList<>();
                            //  ArrayList<Row> delrows = getDelRows(gtp);
                            gtp.setCommon_vsys_id(entity.getCommon_vsys_id());
                            gtp.setLast_update_time(entity.getCommon_recv_time());
                            gtp.setGtp_uplink_teid(entity.getGtp_uplink_teid());
                            gtp.setGtp_downlink_teid(entity.getGtp_downlink_teid());
                            gtp.setGtp_uuid(DigestUtils.md5Hex(gtp.getGtp_uplink_teid() +"|"+ gtp.getGtp_downlink_teid() ));
                            gtp.setGtp_apn(entity.getGtp_apn());
                            gtp.setGtp_phone_number(entity.getGtp_phone_number());
                            gtp.setGtp_imsi(entity.getGtp_imsi());
                            gtp.setGtp_imei(entity.getGtp_imei());
                            ArrayList<Row> updaterows = getupdateRows(gtp);
                            // rows.addAll(delrows);
                            rows.addAll(updaterows);
                            updateKnowledgeMessage(rows);
                            updateRelationMessage(entity.getHashkey(), gtp);
                        }
                    }

                }



            }else{

                if ("delete".equals(entity.getGtp_msg_type())) {


                    gtp.setMsg_type(2);
                    ArrayList<Row> rows = new ArrayList<>();
                    //  ArrayList<Row> delrows = getDelRows(gtp);
                    gtp.setCommon_vsys_id(entity.getCommon_vsys_id());
                    gtp.setLast_update_time(entity.getCommon_recv_time());
                    gtp.setGtp_uplink_teid(entity.getGtp_uplink_teid());
                    gtp.setGtp_downlink_teid(entity.getGtp_downlink_teid());
                    gtp.setGtp_uuid(DigestUtils.md5Hex(gtp.getGtp_uplink_teid() +"|"+ gtp.getGtp_downlink_teid() ));
                    gtp.setGtp_apn(entity.getGtp_apn());
                    gtp.setGtp_phone_number(entity.getGtp_phone_number());
                    gtp.setGtp_imsi(entity.getGtp_imsi());
                    gtp.setGtp_imei(entity.getGtp_imei());
                    ArrayList<Row> updaterows = getupdateRows(gtp);
                    // rows.addAll(delrows);
                    rows.addAll(updaterows);
                    updateKnowledgeMessage(rows);
                }

            }
        } else {


                Gtp gtpobj = new Gtp();

                gtpobj.setCommon_vsys_id(entity.getCommon_vsys_id());
                gtpobj.setLast_update_time(entity.getCommon_recv_time());
                gtpobj.setGtp_uplink_teid(entity.getGtp_uplink_teid());
                gtpobj.setGtp_downlink_teid(entity.getGtp_downlink_teid());
                gtpobj.setGtp_uuid(DigestUtils.md5Hex(gtpobj.getGtp_uplink_teid() +"|"+ gtpobj.getGtp_downlink_teid() ));
                gtpobj.setGtp_apn(entity.getGtp_apn());
                gtpobj.setGtp_phone_number(entity.getGtp_phone_number());
                gtpobj.setGtp_imsi(entity.getGtp_imsi());
                gtpobj.setGtp_imei(entity.getGtp_imei());
                if(!"delete".equals(entity.getGtp_msg_type())) {
                    gtpobj.setMsg_type(1);
                }
                else{
                    gtpobj.setMsg_type(2);

                }
                ArrayList<Row> rows = new ArrayList<>();
                ArrayList<Row> updaterows = getupdateRows(gtpobj);
                rows.addAll(updaterows);
                updateRelationMessage(entity.getHashkey(), gtpobj);
                updateKnowledgeMessage(rows);

        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


    public void updateRelationMessage(String key, Gtp gtp) throws IOException {

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(GtpConfig.RELATION_USER_TEID_TABLE_NAME));
            Put put = new Put(key.getBytes());
            put.addColumn("gtp".getBytes(), "uplink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_uplink_teid()));
            put.addColumn("gtp".getBytes(), "downlink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_downlink_teid()));
            put.addColumn("gtp".getBytes(), "apn".getBytes(), Bytes.toBytes(gtp.getGtp_apn()));
            put.addColumn("gtp".getBytes(), "phone_number".getBytes(), Bytes.toBytes(gtp.getGtp_phone_number()));
            put.addColumn("gtp".getBytes(), "imsi".getBytes(), Bytes.toBytes(gtp.getGtp_imsi()));
            put.addColumn("gtp".getBytes(), "imei".getBytes(), Bytes.toBytes(gtp.getGtp_imei()));
            put.addColumn("gtp".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(gtp.getLast_update_time()));
            put.addColumn("gtp".getBytes(), "msg_type".getBytes(), Bytes.toBytes(gtp.getMsg_type()));
            put.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(gtp.getCommon_vsys_id()));

            table.put(put);
            gtpConcurrentHashMap.put(key, gtp);
        } catch (Exception e) {
            log.error(e.toString());
        } finally {
            table.close();
        }


    }

    public void updateKnowledgeMessage(ArrayList<Row> rows) throws IOException {

        Table tableR = null;
        try {

            tableR = connection.getTable(TableName.valueOf(GtpConfig.GTPC_KNOWLEDGE_BASE_TABLE_NAME));
            Object[] results = new Object[rows.size()];
            tableR.batch(rows, results);
        } catch (Exception e) {
            log.error(e.toString());
        } finally {
            tableR.close();
        }


    }


/*    public ArrayList<Row> getDelRows(Gtp entity) {


        ArrayList<Row> delrows = new ArrayList<>();

        if (!"".equals(entity.getGtp_apn())) {
            String oldapnkey = "3" + new StringBuffer(entity.getGtp_apn()).reverse().toString() + "|" + entity.getGtp_uuid();
            Delete del_apnkey = new Delete(Bytes.toBytes(oldapnkey));
            delrows.add(del_apnkey);
        }
        if (!"".equals(entity.getGtp_phone_number())) {

            String oldpnkey = "2" + new StringBuffer(entity.getGtp_phone_number()).reverse().toString() + "|" + entity.getGtp_uuid();
            Delete del_pnkey = new Delete(Bytes.toBytes(oldpnkey));
            delrows.add(del_pnkey);
        }

        if (!"".equals(entity.getGtp_imsi())) {

            String oldimsikey = "1" + entity.getGtp_imsi() + "|" + entity.getGtp_uuid();
            Delete del_imsikey = new Delete(Bytes.toBytes(oldimsikey));
            delrows.add(del_imsikey);
        }
        if (!"".equals(entity.getGtp_imei())) {

            String oldimeikey = "0" + entity.getGtp_imei() + "|" + entity.getGtp_uuid();
            Delete del_imeikey = new Delete(Bytes.toBytes(oldimeikey));
            delrows.add(del_imeikey);
        }

        return delrows;
    }*/


    public ArrayList<Row> getupdateRows(Gtp gtp) {


        ArrayList<Row> updaterows = new ArrayList<>();

        if (!"".equals(gtp.getGtp_apn())) {
            String apnkey = "3" + new StringBuffer(gtp.getGtp_apn()).reverse().toString() +"|" + gtp.getCommon_vsys_id() + "|" + gtp.getGtp_uuid();
            Put putApn = new Put(apnkey.getBytes());
            putApn.addColumn("gtp".getBytes(), "uplink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_uplink_teid()));
            putApn.addColumn("gtp".getBytes(), "downlink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_downlink_teid()));
            putApn.addColumn("gtp".getBytes(), "apn".getBytes(), Bytes.toBytes(gtp.getGtp_apn()));
            putApn.addColumn("gtp".getBytes(), "phone_number".getBytes(), Bytes.toBytes(gtp.getGtp_phone_number()));
            putApn.addColumn("gtp".getBytes(), "imsi".getBytes(), Bytes.toBytes(gtp.getGtp_imsi()));
            putApn.addColumn("gtp".getBytes(), "imei".getBytes(), Bytes.toBytes(gtp.getGtp_imei()));
            putApn.addColumn("gtp".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(gtp.getLast_update_time()));
            putApn.addColumn("gtp".getBytes(), "msg_type".getBytes(), Bytes.toBytes(gtp.getMsg_type()));
            putApn.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(gtp.getCommon_vsys_id()));
            updaterows.add(putApn);

        }
        if (!"".equals(gtp.getGtp_phone_number())) {
            String pnkey = "2" + new StringBuffer(gtp.getGtp_phone_number()).reverse().toString() +"|" + gtp.getCommon_vsys_id() + "|" + gtp.getGtp_uuid();
            Put putPn = new Put(pnkey.getBytes());
            putPn.addColumn("gtp".getBytes(), "uplink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_uplink_teid()));
            putPn.addColumn("gtp".getBytes(), "downlink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_downlink_teid()));
            putPn.addColumn("gtp".getBytes(), "apn".getBytes(), Bytes.toBytes(gtp.getGtp_apn()));
            putPn.addColumn("gtp".getBytes(), "phone_number".getBytes(), Bytes.toBytes(gtp.getGtp_phone_number()));
            putPn.addColumn("gtp".getBytes(), "imsi".getBytes(), Bytes.toBytes(gtp.getGtp_imsi()));
            putPn.addColumn("gtp".getBytes(), "imei".getBytes(), Bytes.toBytes(gtp.getGtp_imei()));
            putPn.addColumn("gtp".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(gtp.getLast_update_time()));
            putPn.addColumn("gtp".getBytes(), "msg_type".getBytes(), Bytes.toBytes(gtp.getMsg_type()));
            putPn.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(gtp.getCommon_vsys_id()));

            updaterows.add(putPn);

        }
        if (!"".equals(gtp.getGtp_imsi())) {

            String imsikey = "1" + gtp.getGtp_imsi() +"|" + gtp.getCommon_vsys_id() + "|" + gtp.getGtp_uuid();
            Put putImsi = new Put(imsikey.getBytes());
            putImsi.addColumn("gtp".getBytes(), "uplink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_uplink_teid()));
            putImsi.addColumn("gtp".getBytes(), "downlink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_downlink_teid()));
            putImsi.addColumn("gtp".getBytes(), "apn".getBytes(), Bytes.toBytes(gtp.getGtp_apn()));
            putImsi.addColumn("gtp".getBytes(), "phone_number".getBytes(), Bytes.toBytes(gtp.getGtp_phone_number()));
            putImsi.addColumn("gtp".getBytes(), "imsi".getBytes(), Bytes.toBytes(gtp.getGtp_imsi()));
            putImsi.addColumn("gtp".getBytes(), "imei".getBytes(), Bytes.toBytes(gtp.getGtp_imei()));
            putImsi.addColumn("gtp".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(gtp.getLast_update_time()));
            putImsi.addColumn("gtp".getBytes(), "msg_type".getBytes(), Bytes.toBytes(gtp.getMsg_type()));
            putImsi.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(gtp.getCommon_vsys_id()));

            updaterows.add(putImsi);

        }
        if (!"".equals(gtp.getGtp_imei())) {
            String imeikey = "0" + gtp.getGtp_imei() +"|" + gtp.getCommon_vsys_id() + "|" + gtp.getGtp_uuid();
            Put putImei = new Put(imeikey.getBytes());
            putImei.addColumn("gtp".getBytes(), "uplink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_uplink_teid()));
            putImei.addColumn("gtp".getBytes(), "downlink_teid".getBytes(), Bytes.toBytes(gtp.getGtp_downlink_teid()));
            putImei.addColumn("gtp".getBytes(), "apn".getBytes(), Bytes.toBytes(gtp.getGtp_apn()));
            putImei.addColumn("gtp".getBytes(), "phone_number".getBytes(), Bytes.toBytes(gtp.getGtp_phone_number()));
            putImei.addColumn("gtp".getBytes(), "imsi".getBytes(), Bytes.toBytes(gtp.getGtp_imsi()));
            putImei.addColumn("gtp".getBytes(), "imei".getBytes(), Bytes.toBytes(gtp.getGtp_imei()));
            putImei.addColumn("gtp".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(gtp.getLast_update_time()));
            putImei.addColumn("gtp".getBytes(), "msg_type".getBytes(), Bytes.toBytes(gtp.getMsg_type()));
            putImei.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(gtp.getCommon_vsys_id()));

            updaterows.add(putImei);

        }


        return updaterows;
    }
}
