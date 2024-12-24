package com.retasilesv1.func;


import com.alibaba.fastjson.JSONObject;
import com.retasilesv1.domain.TableProcessDim;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConfigUtils;
import utils.HbaseUtils;
import utils.JdbcUtils;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.retailersv1.func.ProcessSpiltStreamToHBaseDim
 * @Author zhou.han
 * @Date 2024/12/19 22:55
 * @description:
 */
public class ProcessSpiltStreamToHBaseDim extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    private MapStateDescriptor<String,JSONObject> mapDescriptor;
    private HashMap<String, JSONObject> hashMap =  new HashMap<>();

    private org.apache.hadoop.hbase.client.Connection haseconnection;
    private   HbaseUtils  hbaseUtils;
    @Override
    public void open(Configuration parameters) throws Exception {
//        System.out.println(querySQL+"==========querySQL====================>");
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
         String querySQL = "select * from  gmall_config.table_process_dim";
//        System.out.println(connection+"====================>");
        List<JSONObject> jsonObjects = JdbcUtils.queryList(connection, querySQL, JSONObject.class, true);
        //System.out.println("jsonObjects===========================================>"+jsonObjects);
        for (JSONObject jsonObject : jsonObjects) {
            hashMap.put(jsonObject.getString("sourceTable"),jsonObject);
        }

        connection.close();
         HbaseUtils hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        haseconnection=hbaseUtils.getConnection();
    }

    public ProcessSpiltStreamToHBaseDim(MapStateDescriptor<String, JSONObject> mapStageDesc) throws Exception {
        this.mapDescriptor = mapStageDesc;
    }

//    @Override
//    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
//        // 主流逻辑
//        String table = jsonObject.getString("table");
//        // 读取状态
//        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapDescriptor);
//        // 根据table取值
//        TableProcessDim tableProcessDim = broadcastState.get(table);
//
//        if (tableProcessDim == null){
//            tableProcessDim= hashMap.get(table);
//        }
//
//        if (tableProcessDim != null){
//            System.out.println("jsonObject = " + jsonObject);
//            System.out.println("tableProcessDim = " + tableProcessDim);
//            collector.collect(Tuple2.of(jsonObject,tableProcessDim));
//        }
//    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //        // 主流逻辑
        //   System.out.println(jsonObject+"=========================>");
        //{"op":"u","before":{"is_ordered":1,"cart_price":"ECw8","sku_num":1,"create_time":1734009737000,"user_id":"13","sku_id":16,"sku_name":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H RTX3060 冰魄白","id":212,"order_time":1734009746000,"operate_time":1734009746000},"after":{"is_ordered":1,"cart_price":"ECw8","sku_num":1,"create_time":1734009737000,"user_id":"13","sku_id":16,"sku_name":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H RTX3060 冰魄白","id":212,"order_time":1734027927000,"operate_time":1734027927000},"source":{"thread":48,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000008","connector":"mysql","pos":2377936,"name":"mysql_binlog_source","row":0,"ts_ms":1734869615000,"snapshot":"false","db":"gmall","table":"cart_info"},"ts_ms":1734875684039}=========================>
        // {"op":"c","after":{"coupon_id":2,"create_time":1734030761000,"user_id":23,"get_time":1734030761000,"id":214,"coupon_status":"1401"},"source":{"thread":53,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000008","connector":"mysql","pos":2372507,"name":"mysql_binlog_source","row":0,"ts_ms":1734869615000,"snapshot":"false","db":"gmall","table":"coupon_use"},"ts_ms":1734875684039}=========================>
        //{"op":"d","before":{"is_ordered":1,"cart_price":"DIH0","sku_num":2,"create_time":1734026927000,"user_id":"20","sku_id":9,"sku_name":"Apple iPhone 12 (A2404) 64GB 红色 支持移动联通电信5G 双卡双待手机","id":240,"order_time":1734026944000,"operate_time":1734026944000},"source":{"thread":34,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000008","connector":"mysql","pos":2358813,"name":"mysql_binlog_source","row":0,"ts_ms":1734869614000,"snapshot":"false","db":"gmall","table":"cart_info"},"ts_ms":1734875684037}=========================>
        //        // 主流逻辑
        String tableName = jsonObject.getJSONObject("source").getString("table");

        System.out.println(tableName + "====tableName=============>");
        //order_info====tableName=============>
//        // 读取状态

        ReadOnlyBroadcastState<String, JSONObject> broadData = readOnlyContext.getBroadcastState(mapDescriptor);
        // System.out.println(broadData+"===================broadData==============>");
        if (broadData != null || hashMap.get(tableName) != null) {
            if (!jsonObject.getString("op").equals("d")) {
                JSONObject after = jsonObject.getJSONObject("after");
                //  System.out.println(after+"===========after=====>");
               // System.out.println(hashMap + "hashMap=======================>");
                JSONObject jsonObject1 = hashMap.get(tableName);
                 if(jsonObject1!=null)
                 {
                     String sinkTableName = jsonObject1.getString("sinkTable");
                     System.out.println(sinkTableName+"===========adasvafdasdfsdsffasdda=============>");
                     sinkTableName="gmall_env:"+sinkTableName;
                     System.out.println(hashMap);
                     String hbaseRowKey = after.getString(hashMap.get(tableName).getString("sinkRowKey"));
                     System.err.println("hbaseRowKey ->"+hbaseRowKey);
                     Table hbaseConntiontable = haseconnection.getTable(TableName.valueOf(sinkTableName));
                     Put put = new Put(Bytes.toBytes(hbaseRowKey));
                     for (Map.Entry<String, Object> entry : after.entrySet()) {
                         put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(String.valueOf(entry.getValue())));
                     }
                     HbaseUtils hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
//                     if(hbaseUtils.tableIsExists("gmall_env:"+hashMap.get(tableName).getString("sinkTable")))
//                        {
                                     hbaseConntiontable.put(put);
                     System.err.println("put -> "+put.toJSON()+" "+ Arrays.toString(put.getRow()));

//                     }
                 }

                        // 以下是原代码中后续对sinkTableName相关的操作，保持不变，此处省略注释部分代码
                        //


                }
            }
        }


    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        System.out.println(jsonObject+"==========================Broadcast==========================>");
        //{"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_activity_rule","source_table":"activity_rule","sink_columns":"id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level"}}==========================Broadcast==========================>
        //{"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_activity_rule","source_table":"activity_rule","sink_columns":"id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level"}}==========================Broadcast==========================>
        //{"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_activity_rule","source_table":"activity_rule","sink_columns":"id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level"}}==========================Broadcast==========================>
        //{"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_activity_rule","source_table":"activity_rule","sink_columns":"id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level"}}==========================Broadcast==========================>
        //{"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_base_category2","source_table":"base_category2","sink_columns":"id,name,category1_id"}}==========================Broadcast==========================>
        //{"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_base_trademark","source_table":"base_trademark","sink_columns":"id,tm_name"}}==========================Broadcast==========================>
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapDescriptor);
        String op = jsonObject.getString("op");
          if(jsonObject.containsKey("after"))
          {
              String sourceTable = jsonObject.getJSONObject("after").getString("source_table");
                  if("d".equals(op))
                  {
                        broadcastState.remove(sourceTable);
                  }else {
                       broadcastState.put(sourceTable,jsonObject);
                  }
          }

    }
    public  void  close()  throws Exception{
             super.close();
          haseconnection.close();
    }
//    @Override
//    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapDescriptor);
//        broadcastState.put(tableProcessDim.getSourceTable(),tableProcessDim);
//        // 如果配置表删除了，要从状态移出去数据
//        String op = tableProcessDim.getOp();
//        if ("d".equals(op)){
//            broadcastState.remove(tableProcessDim.getSourceTable());
//            hashMap.remove(tableProcessDim.getSourceTable());
//        }
//    }
}
