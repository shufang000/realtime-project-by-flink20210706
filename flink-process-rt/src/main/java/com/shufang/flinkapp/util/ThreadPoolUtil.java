package com.shufang.flinkapp.util;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 这是一个创建单例线程池对象的工具类,保证线程安全，
 * <p>
 * 创建单例模式步骤：
 * 1、私有化对象属性，并设置成static
 * 2、私有化构造器
 * 2、创建静态方法，为了线程安全，可以添加锁！！配合双重判断机制
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;

    // TODO 一个典型的懒汉式单例模式，双重判断+加锁
    public static ThreadPoolExecutor getInstance() {
        //双重判断保证效率，只有pool==null的时候才执行加锁的代码
        if (Objects.isNull(pool)) {
            //加锁保证线程安全
            synchronized (ThreadPoolUtil.class) {
                if (Objects.isNull(pool)) {
                    pool = new ThreadPoolExecutor(
                            3,  //初始化的线程的个数
                            20, //当线程不够用的情况下会创建新的线程，最多为20个
                            30, //线程空闲的最长时长为 300s
                            TimeUnit.SECONDS, //定义时间单位
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE) //如果任务来了之后还没分配线程，那么将任务添加到阻塞队列
                    );
                }

            }
        }
        return pool;
    }

    public static void main(String[] args) {
        //java.util.concurrent.ThreadPoolExecutor@737c3850[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
        //java.util.concurrent.ThreadPoolExecutor@737c3850[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
        new Thread(new Runnable() {
            @Override
            public void run() {
                ThreadPoolExecutor instance = ThreadPoolUtil.getInstance();
                //instance.submit(new Runnable()) 处理请求
                System.out.println(instance);
            }
        }, "a").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                ThreadPoolExecutor instance = ThreadPoolUtil.getInstance();
                System.out.println(instance);
            }
        }, "b").start();


    }

}
