package com.zdjizhi.topology;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.GtpConfig;
import com.zdjizhi.pojo.Entity;
import com.zdjizhi.utils.functions.FilterNullFunction;
import com.zdjizhi.utils.functions.ParseFunction;
import com.zdjizhi.utils.hbasepackage.HbaseSink;
import com.zdjizhi.utils.kafka.Consumer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author qidaijie
 * @Package com.zdjizhi.topology
 * @Description:
 * @date 2021/5/2016:42
 */
public class GtpRelation {
    private static final Log logger = LogFactory.get();


    public static void main(String[] args) {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment.addSource(Consumer.getKafkaConsumer());

        DataStream<Entity> getObject = streamSource.map(new ParseFunction()).name("ParseJson");

        DataStream<Entity>  filterOriginalData = getObject.filter(new FilterNullFunction()).name("FilterOriginalData");

        KeyedStream<Entity, Tuple1<String>> GtprelationTuple = filterOriginalData.keyBy(new oneKeySelector());


        GtprelationTuple.addSink(new HbaseSink(GtpConfig.HBASE_ZOOKEEPER_SERVERS));

        try {
            environment.execute("RELATIONSHIP-GTPC-USER");
        } catch (Exception e) {
            logger.error("This Flink task start ERROR! Exception information is :" + e);
        }

    }


    public static class oneKeySelector implements KeySelector<Entity, Tuple1<String>> {

        @Override
        public Tuple1<String> getKey(Entity entity) throws Exception {
            return new Tuple1<>(entity.getHashkey());
        }
    }
}
