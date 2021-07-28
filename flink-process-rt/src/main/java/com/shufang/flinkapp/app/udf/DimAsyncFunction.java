package com.shufang.flinkapp.app.udf;

import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.util.DimQueryUtil;
import com.shufang.flinkapp.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 这是一个通用的
 *
 * @param <T> 可以处理任何事实数据的维度关联,比如传入的时OrderWide对象，那么输出的就是补齐维度字段后的OrderWide字段
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    ThreadPoolExecutor pool = null;
    String tableName = null;
    // 方便从外部，传入查询的表名字
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    // 对线程池进行初始化操作，直接利用创建的ThreadPoolUtil工具类
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("");
        pool = ThreadPoolUtil.getInstance();
    }


    /**
     * 发送异步请求的方法
     *
     * @param input        流中的事实数据
     * @param resultFuture 异步处理请求之后返回的结果
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        //直接利用线程池，创建线程执行异步操作
        pool.submit(new Runnable() {
            @Override
            public void run() {
                //TODO 这里就是异步的逻辑，我们需要将事实数据与维度数据进行关联操作，任意事实与任意维度关联的通用逻辑，不能写死
                try {
                    long start = System.currentTimeMillis();
                    //1 首先获取维度数据
                    JSONObject jsonObject = DimQueryUtil.queryDimWithCache(tableName, getKey(input));
                    System.out.println("维度数据为：" + jsonObject);
                    //2 关联维度
                    if (!Objects.isNull(jsonObject)) {
                        join(input,jsonObject);
                    }
                    System.out.println("关联维度之后的数据为：" + input);
                    long end = System.currentTimeMillis();

                    System.out.println("异步IO关联该数据消耗 " + (end-start) + "ms~");
                    //3 最终将数据传给下一个环节
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("维度关联异常：" + input);
                }
            }
        });
    }
}
