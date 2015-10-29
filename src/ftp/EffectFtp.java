package ftp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import db.DataBaseJdbc;

public class EffectFtp {
	//���url��Ӧmap
	static Map<String,String> activeUrl=new HashMap<String,String>();



	public static void main(String[] args) throws Exception {
	//	insertResult();
		String nowDay=DateUtil.getNowDateYYYY_MM_DD();
		String currentTime =DateUtil.getNowDateStr();
		System.out.println("��������ʱ�䣺  "+currentTime);
		String sql=" SELECT url,max(end_time) end_time FROM active_info WHERE " +
				" date_sub(begin_time,interval 1 day)<'"+nowDay+"' and end_time>'"+nowDay+"' AND status=2 and url is NOT NULL and url <>'' " +
						" group by url ";
	    //test 
	//	String sql ="select active_code ,url,begin_time ,end_time  from active_info where begin_time > '2015-10-20 00:00:00' and begin_time< '2015-10-27 00:00:00' and url is not null and url <> '' ";
		//String sql=" SELECT * FROM active_info WHERE status=2 and url is NOT NULL";
		Connection conn=DataBaseJdbc.get85MysqlConnection();
		Statement stmt=null;
		ResultSet set=null;
		stmt=conn.createStatement();
		set = stmt.executeQuery(sql);
		List<Map<String,String>> file=new ArrayList<Map<String,String>>();
		Map<String,String> row=null;
		while(set.next()){
			if(set.getString("url")!=null&&!"".equals(set.getString("url"))){
				row=new HashMap<String,String>();
				row.put("url", set.getString("url").trim());
				row.put("endTime", set.getString("end_time").trim());
				file.add(row);
			}
		}

		if(set!=null)set.close();
		if(stmt!=null)stmt.close();
		if(conn!=null)conn.close();

		if(file.size()>0){
			//online
			//File createFile=new File("/interface/yangsy/filelog/urllog_"+nowDay+".txt");
			//test
		//	File createFile=new File("H:\\zyx\\urllog_"+nowDay+".txt");
			File createFile=new File("/opt/tomcat_ssyx/ftpProject/ftpupload/urllog_"+nowDay+".txt");
			System.out.println("file uploadpath :"+createFile.getPath());
			FileWriter fw=new FileWriter(createFile);
			for (Map<String, String> map : file) {
//				//test ֻ��ȡһ������
				String data=null;
				String newUrl ="";
				if(map.get("url").trim().contains("https://")){
					data =getDataRegex(map.get("url").trim(),"https://","/");
					if(data == null)
						newUrl=map.get("url").trim().substring(8);
					else newUrl=data;
				}
				else if(map.get("url").trim().contains("http://"))  {
					data =getDataRegex(map.get("url").trim(),"http://","/");
					if(data == null)
						newUrl=map.get("url").trim().substring(7);
					else newUrl=data;
				}
				else newUrl =map.get("url").trim();
//			//	System.out.println("newUrl : "+newUrl);
				fw.write(newUrl+","+map.get("endTime").substring(0,10).trim());
//				//online
//				//	fw.write(map.get("url").trim()+","+map.get("endTime").substring(0,10).trim());
				fw.write("\n");
			}
			fw.close();
			System.out.println("����ʱ����Ҫͳ��url�Ļ  �ļ������ɹ�  �������"+file.size());
			//�����ļ�/thetabin/push/logfile/

			InputStream inputStream = new FileInputStream(createFile);
			//online
//			boolean boo=uploadFile("10.25.88.75",-1,"push","push123","/thetabin/push/","urlconfig.txt",inputStream);
			//test
		//	boolean boo=uploadFile("192.168.163.129",-1,"zyx","zyx123","/home/zyx/ftpupload","urlconfig.txt",inputStream);
		//	boolean boo=uploadFile("10.108.226.124",-1,"wwt008","......","D:\\test\\","urlconfig.txt",inputStream);
			boolean boo=uploadFile("10.112.1.134",-1,"aiapp","as1a1nf0","/d2_data1/aiapp/zyx/ftpProject/push/","urlconfig.txt",inputStream);
			if(boo){
				System.out.println("�ϴ��ɹ���");
			}else{
				System.out.println("�ϴ�ʧ�ܣ�");
			}

			String directoryName=DateUtil.getNowDateMinusDayYYYYMMDD(1)+"/";
			//online
//			File mkdir=new File("/interface/yangsy/getfile/"+directoryName);
			//test
		//	String directoryName1=DateUtil.getNowDateMinusDayYYYYMMDD(1)+"\\";
		//	File mkdir=new File("H:\\zyx\\tt\\"+directoryName1);
			File mkdir=new File("/opt/tomcat_ssyx/ftpProject/ftpdownload/"+directoryName);
			if(!mkdir.exists())mkdir.mkdir();
//			boo=downloadFile("10.25.88.75",-1,"push","push123",
//					"/thetabin/push/url/"+directoryName,"","/interface/yangsy/getfile/"+directoryName);
//		boo=downloadFile("192.168.163.129",-1,"zyx","zyx123",
//					"/home/zyx/ftpdownload/"+directoryName,"","H:\\zyx\\tt\\"+directoryName1);
		// System.out.println(directoryName.toString());
			boo=downloadFile("10.112.1.134",-1,"aiapp","as1a1nf0",
				"/d2_data1/aiapp/zyx/ftpProject/pull/"+directoryName,"","/opt/tomcat_ssyx/ftpProject/ftpdownload/"+directoryName);
			
			if(boo){
				System.out.println("���سɹ���");
			}else{
				System.out.println("����ʧ�ܣ�");
			}

			if(boo){//���سɹ���ʼ�����ļ�
			//	initActiveUrlMap();
				//online
			    //	File[] files=getFiles("/interface/yangsy/getfile/"+directoryName);
			     //test
					File[] files=getFiles("/opt/tomcat_ssyx/ftpProject/ftpdownload/"+directoryName);
			//	 File[] files=getFiles("H:\\zyx\\tt\\"+directoryName1);
				List<Map<String,String>> list=null;
				for (File f : files) {
					list=readFileByLines(f);
					//ɾ��7��֮ǰ�Ĵ�ftp���յ�����
					deleteFtpDataBeforeWeek();
					//�����н��յ���������Db2����������
					insertFtpGetData(list);
					System.out.println("ִ�� �ļ�����:"+list.size());
					insertResult();
				//	insertResultData(list);
				}

				//��������
//				List<Map<String,String>> countActive=getPushActiveCount();
//				if(countActive.size()==0)
//					System.out.println("ͳ��0�����ݲ���mysql��");
//				else System.out.println("ͳ��"+countActive.size()+"�����ݲ���mysql��");
//				 if (countActive !=null && countActive.size() !=0){
//				   for (Map<String, String> map : countActive) {
					updateEffectActive();
//				    }
//				 }
			}
		}else{
			System.out.println("����ʱ��û����Ҫͳ��url�Ļ");
		}

	}
    /**
     * @user : Administrator
     * @Description : �����ݲ���effect_wap_push ��
     * @Date : 2015��10��28��
     */
    public static void insertResult(){
    	String createTime=DateUtil.getNowDateMinusDayYYYY_MM_DD(1)+" 00:00:00";
    	String createDate=DateUtil.getNowDateMinusDayYYYY_MM_DD(1);
    	String sql1="select a.active_code ,a.phone_no,b.url,b.flow_num  from (select active_code , phone_no ,send_ms from aiapp.send_sms_log ) a"
    			+ " join (select * from aiapp.ssyx_effect_ftp_data where create_date ='"+createDate+"') b "
    			+ "on a.phone_no =b.phone_no where a.send_ms like '%'||b.url||'%' ";
    	String sql2="insert into effect_wap_push(active_code,phone_no,push_url,flow_num,create_time ) values(?,?,?,?,?) ";
//    	List<String> list = initActiveUrlMap();
//    	System.out.println("url ��Ϊ�յĻ���� :"+list.size());
    	Connection conn=null;
    	Connection conn1=null;
    	try {
    		conn =DataBaseJdbc.get134MysqlConnection();
    		conn1 =DataBaseJdbc.get85MysqlConnection();
    		conn1.setAutoCommit(false);
			Statement st =conn.createStatement();
			PreparedStatement pst =conn1.prepareStatement(sql2, ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
	     	ResultSet set =	st.executeQuery(sql1);
	     	int i =0;
		    while(set.next()){
		    				pst.setString(1, set.getString(1));
		    		    	pst.setString(2, set.getString(2));
		    		    	pst.setString(3, set.getString(3));
		    		    	pst.setInt(4,  (int)set.getLong(4));
		    		    	pst.setString(5, createTime);
		    		    	pst.addBatch();
		    	 if(i%2000==0){//ÿ2000���ύһ��
            		 pst.executeBatch();
            		 conn1.commit();
            	}
		     i++;
		    }
		    pst.executeBatch();
		    conn1.commit();
		    System.out.println("�ɹ�ƥ�������� �� "+i);
		    if(set!=null) set.close();
		    if(st!=null) st.close();
		    if(conn!=null) conn.close();
		    if(pst!=null) pst.close();
		    if(conn1!=null) conn1.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("����effect_wap_push ����� ===========>" +e.getMessage());
		}
    	
    }
    /**
     * @user : Administrator
     * @Description : ɾ��ssy_effect_ftp_data ����7��֮ǰ������
     * @return
     * @Date : 2015��10��29��
     */
    public static boolean deleteFtpDataBeforeWeek(){
    	boolean b =true;
    	Connection conn=null;
    	Statement st =null;
    	String deleteSql ="delete from aiapp.ssyx_effect_ftp_data where create_date < current date -8 days";
    	try {
    		conn =DataBaseJdbc.get134MysqlConnection();
			st =conn.createStatement();
			st.execute(deleteSql);
			System.out.println("���ssyx_effect_ftp_data 7�����ݳɹ�");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			b=false;
			System.out.println("ɾ��ssyx_effect_ftp_data�����쳣 =========�� "+e.getMessage());
		}finally{
				try {
					if(st !=null) st.close();
					if(conn !=null) conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					b=false;
					System.out.println("�ر����ݿ������쳣 =========�� "+e.getMessage());
				}
		}
    	
    	return b;
    }
	/**
	 * @user : Administrator
	 * @Description : �����ص��������ݶ�����db2���ݿ���
	 * @param list
	 * @return
	 * @Date : 2015��10��27��
	 */
	public static boolean insertFtpGetData(List<Map<String, String>> list){
		String date=DateUtil.getNowDateMinusDayYYYY_MM_DD(1);
		Connection conn=null;
		boolean b=true ;
		if(list!=null){
		try {
			conn =DataBaseJdbc.get134MysqlConnection();
			conn.setAutoCommit(false);
			String initSql="insert into aiapp.ssyx_effect_ftp_data(active_code,phone_no,url,flow_num,create_date) values(?,?,?,?,?) ";
			PreparedStatement prest = conn.prepareStatement(initSql,ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
			for(int j=0;j<list.size();j++){
				prest.setString(1, list.get(j).get("activeCode"));
				prest.setString(2, list.get(j).get("phoneNo"));
				prest.setString(3, list.get(j).get("url"));
				prest.setLong(4, Long.valueOf(list.get(j).get("flow").trim()));
				prest.setString(5, date);
				prest.addBatch();
				     if(j>0){
                	    if(j%2000==0){//ÿ2000���ύһ��
                		 prest.executeBatch();
                		 conn.commit();
                	}
                }
			}
			prest.executeBatch();
			conn.commit();
	        conn.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			b=false;
			System.out.println("sqlException >>>>>>>>"+e1.getMessage());
		}
	
//			for(int j=0;j<list.size();){
//
//				String initSql="insert into aiapp.ssyx_effect_ftp_data(active_code,phone_no,url,flow_num,create_date) values "+
//			    		"('"+list.get(j).get("activeCode")+"',"+
//			    		"'"+list.get(j).get("phoneNo")+"',"+
//			    		"'"+list.get(j).get("url")+"',"+
//			    		""+Long.valueOf(list.get(j).get("flow").trim())+","+
//			    		"'"+date+"' )";
//				System.out.println("sql "+initSql);
//				j++;
//				try {
//					
//					insertDb2(initSql);
//				} catch (Exception e) {
//					// TODO Auto-generated catch b
//					System.out.println("sqlException >>>>>>>>"+e.getMessage());
//					return false;
//				}
//				
//			}
			System.out.println("�����ȡ�ĵ绰����������"+list.size());
		}
		else b=false;
		return b;
	}
	public static boolean insertDb2(String sql) throws SQLException{
		Connection conn=DataBaseJdbc.get134MysqlConnection();
		Statement stmt=null;
		stmt=conn.createStatement();
		int cr=stmt.executeUpdate(sql);
		if(stmt!=null)stmt.close();
		if(conn!=null)conn.close();
		if(cr>0) return true;
		else return false;
	}
	/**
	 * ������ʽ��ȡ�ַ���
	 * @param res
	 * @param beginStr
	 * @param endStr
	 * @return
	 */
	public  static String getDataRegex(String res,String beginStr,String endStr){
    	Pattern p = Pattern.compile(beginStr+"([\\s\\S]*?)"+endStr);  
		Matcher m = p.matcher(res);  
		
		if(m.find())
		{
			return m.group(1);
		}
		return null;

    }

	/**
	 * ����ͳ�����ݱ�
	 * @param map
	 * @throws SQLException
	 */
	public static void updateEffectActive() throws SQLException{
//		int userNum=Integer.parseInt(map.get("pushc"));
//		int flowNum=Integer.parseInt(map.get("sumflow"));
		String create_time =DateUtil.getNowDateMinusDayYYYY_MM_DD(1)+"00:00:00";
//		String sql="update effect_active set tran_count=tran_count+"+userNum+",flow_count=flow_count+"+flowNum+" " +
//				   		" where active_code='"+map.get("activeCode")+"'";
		String sql=" update  effect_active c join (  select a.active_code , a.flow ,a.number  from (select active_code,sum(flow_num) flow, count(1) number from effect_wap_push where create_time ='"+create_time+"' group by active_code) a "
				+ "join (select active_code ,url from active_info  where url is not null and url <> '') b on a.active_code = b.active_code  ) "
				+ "d  on c.active_code = d.active_code  set c.tran_count = c.tran_count + d.number , c.flow_count = flow_count + d.flow ";
    	Connection conn=DataBaseJdbc.get85MysqlConnection();
		Statement stmt=null;
		stmt=conn.createStatement();
		stmt.executeUpdate(sql);
		System.out.println("��Ч������������µ�effec_active�ɹ�");
		if(stmt!=null)stmt.close();
		if(conn!=null)conn.close();
	}

	/**
	 * ��ȡ�����������ͳ��
	 * @return
	 * @throws SQLException
	 */
	public static List<Map<String,String>> getPushActiveCount() throws SQLException{
		List<Map<String,String>> list=new ArrayList<Map<String,String>>();
		String createTime=DateUtil.getNowDateMinusDayYYYY_MM_DD(1)+" 00:00:00";
		String sql="select active_code,count(1) pushc,sum(flow_num) sumflow from effect_wap_push where create_time='"+createTime+"' group by active_code";
		Connection conn=DataBaseJdbc.get85MysqlConnection();
		Statement stmt=null;
		ResultSet set=null;
		stmt=conn.createStatement();
		set = stmt.executeQuery(sql);
		Map<String,String> map=null;
		while(set.next()){
			map=new HashMap<String,String>();
			map.put("activeCode", set.getString("active_code"));
			map.put("pushc", set.getString("pushc"));
			map.put("sumflow", set.getString("sumflow"));
			list.add(map);
		}
		if(set!=null)set.close();
		if(stmt!=null)stmt.close();
		if(conn!=null)conn.close();

		return list;
	}

	/**
     * Description: ��FTP�������ϴ��ļ�
     * @param url FTP������hostname
     * @param port FTP�������˿ڣ����Ĭ�϶˿���д-1
     * @param username FTP��¼�˺�
     * @param password FTP��¼����
     * @param path FTP����������Ŀ¼
     * @param filename �ϴ���FTP�������ϵ��ļ���
     * @param input ������
     * @return �ɹ�����true�����򷵻�false
     */
    public static boolean uploadFile(String url, int port, String username, String password, String path,
        String filename, InputStream input){
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try{
            int reply;
            // ����FTP������
            if (port > -1){
                ftp.connect(url, port);
            }else{
                ftp.connect(url);
            }

            // ��¼FTP
           ftp.login(username, password);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)){
                ftp.disconnect();
                return success;
            }
            //online
            ftp.changeWorkingDirectory(path);
            ftp.deleteFile(filename);//��ɾ��֮ǰ���ļ�������ļ�û����ɾ����ftp�Լ����жϣ�
            ftp.storeFile(filename, input);
            input.close();
            ftp.logout();
            success = true;
        }catch (IOException e){
            success = false;
            System.out.println(e);
        }finally{
            if (ftp.isConnected()){
                try{
                    ftp.disconnect();
                }catch (IOException e){
                    System.out.println(e);
                }
            }
        }
        return success;
    }


    /**
     * Description: ��FTP�����������ļ�
     * @param url FTP������hostname
     * @param port FTP�������˿�
     * @param username FTP��¼�˺�
     * @param password FTP��¼����
     * @param remotePath FTP�������ϵ����·��
     * @param fileName Ҫ���ص��ļ���
     * @param localPath ���غ󱣴浽���ص�·��
     * @return
     */
    public static boolean downloadFile(String url, int port, String username, String password, String remotePath,
        String fileName, String localPath){
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try{
            int reply;
            // ����FTP������
            if (port > -1){
                ftp.connect(url, port);
            }else{
                ftp.connect(url);
            }

            ftp.login(username, password);//��¼
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)){
                ftp.disconnect();
                return success;
            }
               ftp.mkd(remotePath);
            ftp.changeWorkingDirectory(remotePath);//ת�Ƶ�FTP������Ŀ¼
            FTPFile[] fs = ftp.listFiles();
            for (FTPFile ff : fs){
                if ((null==fileName||"".equals(fileName))||ff.getName().equals(fileName)){
                    File localFile = new File(localPath + "" + ff.getName());
                    System.out.println("localFile:"+localFile);
                    if(!localFile.exists()) localFile.createNewFile();
                    OutputStream is = new FileOutputStream(localFile);
                    ftp.retrieveFile(ff.getName(), is);
                    //test
                    is.flush();
                    //
                    is.close();
                }
            }
            ftp.logout();
            success = true;
        }catch (IOException e){
            System.out.println(e);
        }finally{
            if (ftp.isConnected()){
                try{
                    ftp.disconnect();
                }catch (IOException e){
                    System.out.println(e);
                }
            }
        }
        return success;
    }

    public static void resolutionFile(File file) throws Exception{

    }

    public static void insertResultData(List<Map<String,String>> list){
    	initActiveSendPhone();
    	String createTime=DateUtil.getNowDateMinusDayYYYY_MM_DD(1)+" 00:00:00";
    	for (int i = 0; i < list.size(); i++) {
    		if(list.get(i).get("activeCode")==null){
    			System.out.println("���ļ��е�urlû��activeCode��Ӧ");
    			continue;
    		}
    		if(activePhone.get(list.get(i).get("activeCode").trim())==null){
    			System.out.println("��ǰ�δ�������ţ�"+list.get(i).get("activeCode"));
    			continue;
    		}else{
    			if(activePhone.get(list.get(i).get("activeCode").trim()).get(list.get(i).get("phoneNo").trim())==null){
    				System.out.println("��ǰ����δ���Ͷ��ţ�"+list.get(i).get("phoneNo"));
    				continue;
    			}
    		}
    		try {
    		String initSql="insert into effect_wap_push(active_code,phone_no,push_url,flow_num,create_time ) values "+
    		"('"+list.get(i).get("activeCode")+"',"+
    		"'"+list.get(i).get("phoneNo")+"',"+
    		"'"+list.get(i).get("url")+"',"+
    		""+list.get(i).get("flow")+","+
    		"'"+createTime+"' )";
    		System.out.println("sql="+initSql);

			insertMysqlDB(initSql);
			} catch (Exception e) {
					System.out.println("�������ݿ����"+e.getMessage());
			}
    	}
    }


    public static boolean insertMysqlDB(String sql) throws Exception{
    	Connection conn=DataBaseJdbc.get85MysqlConnection();
		Statement stmt=null;
		stmt=conn.createStatement();
		int cr=stmt.executeUpdate(sql);

		if(stmt!=null)stmt.close();
		if(conn!=null)conn.close();
		if(cr>0) return true;
		else return false;
    }
    /**
     * ��ȡ���url֮��Ķ��ڹ�ϵ ��ͳ�Ʒ���������Ҫ�õ�
     * @throws Exception
     */
    public static List<String> initActiveUrlMap(){
  //  	String sql="SELECT * FROM active_info WHERE  status=2 and url is NOT NULL and url <>'' order by begin_time asc";
		String nowDay=DateUtil.getNowDateYYYY_MM_DD();
		String currentTime =DateUtil.getNowDateStr();
    	String sql=" SELECT active_code  FROM active_info WHERE " +
		" date_sub(begin_time,interval 1 day)<'"+nowDay+"' and end_time>'"+nowDay+"' AND status=2 and url is NOT NULL and url <>'' " +
				" group by url ";
    	Connection conn=DataBaseJdbc.get85MysqlConnection();
		Statement stmt=null;
		ResultSet set=null;
		List<String> list=null;
		try {
			stmt=conn.createStatement();
	     	set = stmt.executeQuery(sql);
	      	list=new ArrayList<String>();
		while(set.next()){
			list.add(set.getString("active_code"));
//			if(set.getString("url")!=null&&!"".equals(set.getString("url"))){
//				//test��ȡһ������
//				String data =getDataRegex(set.getString("url").trim(),"http://","/");
//				String newUrl ="";
//				if(set.getString("url").trim().contains("https://")){
//					data =getDataRegex(set.getString("url").trim(),"https://","/");
//					if(data == null)
//						newUrl=set.getString("url").trim().substring(8);
//					else newUrl=data;
//				}
//				else if(set.getString("url").trim().contains("http://")) {
//					data =getDataRegex(set.getString("url").trim(),"http://","/");
//					if(data == null)
//						newUrl=set.getString("url").trim().substring(7);
//					else newUrl=data;
//				}
//				else newUrl =set.getString("url").trim();
//				activeUrl.put(newUrl,set.getString("active_code").trim());
//			}
		  }
	
		} catch (SQLException e) {
			System.out.println("��ʼ������"+e.getMessage());
			return null;
		}finally{

				try {
					if(set!=null)set.close();
					if(stmt!=null)stmt.close();
					if(conn!=null)conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return list;
		//System.out.println("activeUrlactiveUrlactiveUrl:"+activeUrl);
    }

    /**
     * �ļ�׷������
     * @param fileName
     * @param content
     */
    public static void appendFileString(String fileName, String content) {
        try {
            //��һ��д�ļ��������캯���еĵڶ�������true��ʾ��׷����ʽд�ļ�
            FileWriter writer = new FileWriter(fileName, true);
            writer.write(content);
            writer.write("\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static List<Map<String,String>> readFileByLines(File file) {
    	List<Map<String,String>> slist=new ArrayList<Map<String,String>>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            Map<String,String> aum=null;
            String[] ss=null;
            while ((tempString = reader.readLine()) != null) {
            	aum=new HashMap<String,String>();
            	ss=tempString.split("\t");
            //	System.out.println("s0:"+ss[0]+"s1:"+ss[1]+"s2:"+ss[2]);
            	if(ss.length>2){
            		if(ss[1].length()>13)
            		aum.put("phoneNo", ss[1].substring(3));//ɾ�����ţ�+86
            		else 
            		aum.put("phoneNo", ss[1]);
            		aum.put("url", ss[0]);
            		aum.put("flow", ss[2]);
            		addActiveCodeToUrl(aum); //continue;
            		slist.add(aum);
            	}
                System.out.println("line " + line + ": " + "phoneNo :" +aum.get("phoneNo")+" url: "+aum.get("url")+" activeCode "+aum.get("activeCode")+" flow: "+aum.get("flow"));
                line++;
            }
            reader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return slist;
    }

    /**update effect_active  set SEND_COUNT=0,TRAN_COUNT=0,FLOW_COUNT=0
     * url�ͻID��
     * @param map
     */
    public static boolean addActiveCodeToUrl(Map<String,String> map){
    	if(activeUrl.get(map.get("url").trim())!=null){
    		map.put("activeCode", activeUrl.get(map.get("url").trim()));
    		return false;
    	}
    	return true;
    }


    /**
     * ���ǰ�ļ�������������ļ�
     * @param filePath
     * @return
     */
    public static File[] getFiles(String filePath){
    	 File file=new File(filePath);
    	 File[] fileList = file.listFiles();
    	 System.out.println("��Ŀ¼�¶��������"+fileList.length);
    	 for (int i = 0; i < fileList.length; i++) {
    	   if (fileList[i].isFile()) {
    	    System.out.println("��     ����"+fileList[i]);
    	   }
    	   if (fileList[i].isDirectory()) {
    	    System.out.println("�ļ��У�"+fileList[i]);
    	   }
    	 }
    	 return fileList;
    }

    //���浱�죨������־���ڴ��������ͻ���û�����
    static Map<String,Map<String,String>> activePhone=new HashMap<String,Map<String,String>>();

    /**
     * ��ʼ�����췢�ͺ����¼
     * �û��жϵ�������Ƿ��ڣ����Ͷ��ŵĺ����
     */
    public static void initActiveSendPhone(){
    	String sql="select active_code,phone_no from aiapp.send_sms_log where send_date= current date ";
    	Connection conn=DataBaseJdbc.get134MysqlConnection();
		Statement stmt=null;
		ResultSet set=null;
		try {
			stmt=conn.createStatement();
			set = stmt.executeQuery(sql);
			Map<String,String> map=null;
			String active_code="";
			String phone_no="";
			while(set.next()){
				active_code=set.getString("active_code");
				phone_no =set.getString("phone_no");
				map=activePhone.get(active_code);
				if(map==null){
					map=new HashMap<String,String>();
					activePhone.put(active_code, map);
				}
				map.put(phone_no, "");
			}
		}catch(SQLException e) {
			System.out.println("��ѯ������־����"+e);
		}finally {
                try {
                	if (set !=null) set.close();
                	if (stmt!=null) stmt.close();
                	if (conn!=null) conn.close();
                } catch (Exception e1) {
                }
         }
    }

}
