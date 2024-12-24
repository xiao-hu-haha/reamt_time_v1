package com.retasilesv1;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retasilesv1.func.MapUpdateHbaseDimTableFunc;
import com.retasilesv1.func.ProcessSpiltStreamToHBaseDim;
import com.stream.utils.CdcSourceUtils;
import com.sun.tools.javadoc.Start;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import javafx.scene.web.WebView;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.eclipse.jetty.server.Authentication;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;

import java.lang.reflect.ParameterizedType;

public class DbusCdc2KafkaTopic {


    //自定义两个方法 1. zookeper的服务 2.hbase的命名空间
    private  static final  String CDH_ZOOKEEPRER_SERVE= ConfigUtils.getString("zookeeper.server.host.list");
    private static  final  String CDH_HBASE_NASME_SPACE=ConfigUtils.getString("hbase.namespace");
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        MySqlSource<String> mySQLMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        MySqlSource<String> mySQLDimCdcSource = CdcSourceUtils.getMySQLCdcSource(
                           ConfigUtils.getString("mysql.database.conf"),
                          "gmall_config.table_process_dim",
                      ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                  StartupOptions.initial()
        );
        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLDimCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");

        SingleOutputStreamOperator<JSONObject> cdcMainStreamMap= cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);
        SingleOutputStreamOperator<JSONObject> cdcDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                        .setParallelism(1);
//    cdcMainStreamMap.print("====aaa====================>a");
//  cdcDimStreamMap.print("aaaaaaaaa==============================>");
        SingleOutputStreamOperator<JSONObject> cdcDimClean = cdcDimStreamMap.map(s -> {
//                    System.out.println(s+"=====================>");
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d" .equals(s.getString("op"))) {
                        resJson.put("before", s.getJSONObject("before"));
                    } else {
                        resJson.put("after", s.getJSONObject("after"));
                    }
                    resJson.put("op", s.getString("op"));
                    return resJson;
                })
                 .uid("clean_json_column_map")
                .name("clean_json_column_map");
        cdcDimClean.print();
        //

        SingleOutputStreamOperator<JSONObject> tpDS = cdcDimClean.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPRER_SERVE, CDH_HBASE_NASME_SPACE));
        MapStateDescriptor<String, JSONObject> broadcastDs = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcast = cdcDimClean.broadcast(broadcastDs);
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcMainStreamMap.connect(broadcast);

        connectDs.process(new ProcessSpiltStreamToHBaseDim(broadcastDs));

        env.execute("fvfas");
    }
}
