package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class UDFMonthDiff extends UDF {
    /**
     * 获取两个日期相差的月数
     *
     * @param startDate 较大的日期
     * @param endDate   较小的日期
     * @return 如果d1>d2返回 月数差 否则返回0
     * @throws ParseException
     */
    public String evaluate(String startDate, String endDate) throws ParseException {
        String reMonNum = null;
        int monthNum = 0;
        if ("".equals(startDate) || "".equals(endDate) || startDate == null || endDate == null) {
            reMonNum = null;
        } else {
            Date d1 = parseDate(startDate);
            Date d2 = parseDate(endDate);
            Calendar c1 = Calendar.getInstance();
            Calendar c2 = Calendar.getInstance();
            c1.setTime(d2);
            c2.setTime(d1);
            if (c1.getTimeInMillis() < c2.getTimeInMillis()) return "0";
            int year1 = c1.get(Calendar.YEAR);
            int year2 = c2.get(Calendar.YEAR);
            int month1 = c1.get(Calendar.MONTH);
            int month2 = c2.get(Calendar.MONTH);
            int day1 = c1.get(Calendar.DAY_OF_MONTH);
            int day2 = c2.get(Calendar.DAY_OF_MONTH);
            // 获取年的差值 假设 d1 = 2015-8-16  d2 = 2011-9-30
            int yearInterval = year1 - year2;
            // 如果 d1的 月-日 小于 d2的 月-日 那么 yearInterval-- 这样就得到了相差的年数
            if (month1 < month2 || month1 == month2 && day1 < day2) yearInterval--;
            // 获取月数差值
            int monthInterval = (month1 + 12) - month2;
            if (day1 < day2) monthInterval--;
            monthInterval %= 12;

            monthNum = yearInterval * 12 + monthInterval;
            //开始日期加月份数
            c2.add(Calendar.MONTH, monthNum);

            Date d11 = c2.getTime();
            Date d22 = c1.getTime();
            //如果当添加的月份数在在endDate之后，则月份数加1
            if (d22.after(d11)) {
                monthNum += 1;
            }
            reMonNum = monthNum + "";
        }

        return reMonNum;

    }

    private Date parseDate(String dateString) throws ParseException {
        final String[] dataPatterns = {"yyyy-MM-dd", "yyyy/MM/dd", "yyyyMMdd"};
        SimpleDateFormat sdf;
        Date date = null;
        for (String dataPattern : dataPatterns) {
            try {
                sdf = new SimpleDateFormat(dataPattern);
                date = sdf.parse(dateString);
                break;
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }
        if (date == null) {
            throw new ParseException("Invalid date format: " + dateString, 0);
        }
        return date;
    }

    public static void main(String[] args) throws Exception {
        UDFMonthDiff UDFMonthDiff = new UDFMonthDiff();
        System.out.println(UDFMonthDiff.evaluate("2012-02-29", "2012-03-30"));
        System.out.println(UDFMonthDiff.evaluate("20120229", "2012-03-30"));
        System.out.println(UDFMonthDiff.evaluate("2012/02/29", "20120330"));
        System.out.println(UDFMonthDiff.evaluate("2012-02-29", "2012/03/30"));
    }
}
