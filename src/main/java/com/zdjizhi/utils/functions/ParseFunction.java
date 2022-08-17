package com.zdjizhi.utils.functions;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.zdjizhi.pojo.Entity;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.MapFunction;


/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */

public class ParseFunction implements MapFunction<String, Entity> {
    private static final Log logger = LogFactory.get();


    @Override
    public Entity map(String message) {


        Entity entity = new Entity();

        try {
                if (StringUtil.isNotBlank(message)) {
                    entity = JSON.parseObject(message,  Entity.class);
                    if(entity.getGtp_apn()==null){

                        entity.setGtp_apn("");
                    }
                    if(entity.getGtp_phone_number()==null){

                        entity.setGtp_phone_number("");
                    }
                    if(entity.getGtp_imei()==null){

                        entity.setGtp_imei("");
                    }
                    if(entity.getGtp_imsi()==null){

                        entity.setGtp_imsi("");
                    }


                    if(!"".equals(entity.getGtp_imei())|| !"".equals(entity.getGtp_imsi())|| !"".equals(entity.getGtp_phone_number())) {


                        String md5Str = DigestUtils.md5Hex(entity.getGtp_imei() + entity.getGtp_imsi() + entity.getGtp_phone_number());
                        entity.setHashkey(md5Str);

                        if(entity.getGtp_uplink_teid()==null || entity.getGtp_uplink_teid()==0){

                            if(entity.getGtp_downlink_teid()==null || entity.getGtp_downlink_teid()==0){

                                entity.setIfError(1);
                                logger.info("teid为空" + message);

                            }
                            else{

                                entity.setGtp_teid(entity.getGtp_downlink_teid());
                            }
                        }else{

                            entity.setGtp_teid(entity.getGtp_uplink_teid());

                        }

                    }
                    else {
                        entity.setHashkey("");
                        entity.setIfError(1);
                        logger.info("三元组为空" + message);

                    }



                }else{

                    entity.setIfError(1);
                    logger.error("数据转换JSON格式异常,原始日志为:" + message);
                }
            } catch (JSONException jse) {
            entity.setIfError(1);
            logger.error("数据转换JSON格式异常,原始日志为:" + message);
            } catch (RuntimeException re) {
            entity.setIfError(1);
            logger.error("GTP日志条件过滤异常,异常信息为：" + re);
            }

        return entity;
    }
}