package ftp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	/**
	 * 返回当前时间字符串 格式
	 * @return
	 */
	public static String getNowDateStr(){
		String rstr="";
		SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		rstr=sf.format(new Date());
		return rstr;
	}

	public static String getNowDateYYYY_MM_DD(){
		String rstr="";
		SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd");
		rstr=sf.format(new Date());
		return rstr;
	}
	public static String getNowDateYYYYMMDD(){
		String rstr="";
		SimpleDateFormat sf=new SimpleDateFormat("yyyyMMdd");
		rstr=sf.format(new Date());
		return rstr;
	}

	/**
	 * 返回当前时间字符串 格式
	 * @return
	 */
	public static String getNowDateStr2(){
		String rstr="";
		SimpleDateFormat sf=new SimpleDateFormat("yyyyMMddHHmmss");
		rstr=sf.format(new Date());
		return rstr;
	}

	public static String getNowDateMinusDay(int day){
		SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal=Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -day);  //减1天
		return sf.format(cal.getTime());
	}

	public static String getNowDateMinusDayYYYYMMDD(int day){
		SimpleDateFormat sf=new SimpleDateFormat("yyyyMMdd");
		Calendar cal=Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -day);  //减1天
		return sf.format(cal.getTime());
	}

	public static String getNowDateMinusDayYYYY_MM_DD(int day){
		SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal=Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -day);  //减1天
		return sf.format(cal.getTime());
	}

	public static String getNowDateMinusDayYYYYMMDDHHmmss(int minute){
		SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal=Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.MINUTE, -minute);  //减1天
		return sf.format(cal.getTime());
	}

	/**
	 * fTime > sTime
	 * @param fTime
	 * @param sTime
	 * @param format
	 * @return
	 */
	public static boolean isBegin(String fTime,String sTime,String format){
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			Date fData = sdf.parse(fTime);
			Date sData = sdf.parse(sTime);
			if(fData.after(sData)){//fd 时间在sd之后
				return true;
			}
		} catch (ParseException e) {
		}
		return false;
	}

	public static void main(String[] args) {
		String s="02.12.2014 20:09:00     http://mm.10086.cn/a/j/4345     15082539902     1558    10.168.4.85";
		String[] ss=s.split("\\s+");
		System.out.println(ss[0]+"|"+ss[1]);
	}
}
