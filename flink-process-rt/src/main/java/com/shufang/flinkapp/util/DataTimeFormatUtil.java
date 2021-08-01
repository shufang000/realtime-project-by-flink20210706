package com.shufang.flinkapp.util;


import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * TODO 这是一个对String、Long、Date类型的数据进行转换的工具类，首先默认的想到的时SimpleDateFormat，但是该对象在多线程访问的时候可能存在
 *  线程安全的问题：因爲在parse的方法底層會調用：calendar.setTime(startDate);
 *  這裏可以采用，JDK8引入的 DateTimeFormatter
 *  DateTimeFormatter是一个线程安全的日期处理类型
 */
public class DataTimeFormatUtil {

    public static final DateTimeFormatter formattor = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    /*public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");*/
    /*
    当前存在线程安全问题
    public static Long getTsByString(String datetime) {
        Long ts = null;
        try {
            Date date = sdf.parse(datetime);
            ts = date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("解析失敗！" + datetime);
        }

        return ts;
    }*/


    /**
     * 将日期类型转换成String类型
     * @param date 传入的date类型的参数
     * @return 返回一个String类型的 yyyy-MM-dd HH:mm:ss的字符串
     */
    public static String dateToStr(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formattor.format(localDateTime);
    }


    /**
     * 从一个字符串的日期格式，解析成一个Long类型的时间戳格式
     * @param dateStr 字符串
     * @return Long时间戳
     */
    public static Long getTsFromDateStr(String dateStr){
        LocalDateTime dateTime = LocalDateTime.parse(dateStr, formattor);
        return dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static void main(String[] args) {

        System.out.println(getTsFromDateStr("2020-12-31 12:21:36"));
        System.out.println(dateToStr(new Date()));
    }
}
