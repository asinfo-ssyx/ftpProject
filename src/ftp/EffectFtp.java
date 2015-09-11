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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import db.DataBaseJdbc;

public class EffectFtp {
	//���url��Ӧmap
	static Map<String,String> activeUrl=new HashMap<String,String>();



	public static void main(String[] args) throws Exception {
		String nowDay=DateUtil.getNowDateYYYY_MM_DD();
		String sql=" SELECT url,max(end_time) end_time FROM active_info WHERE " +
				" date_sub(begin_time,interval 1 day)<'"+nowDay+"' and end_time>'"+nowDay+"' AND status=2 and url is NOT NULL and url <>'' " +
						" group by url ";
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
			File createFile=new File("/interface/yangsy/filelog/urllog_"+nowDay+".txt");
			//File createFile=new File("C:\\Users\\Hero\\Desktop\\urllog_"+nowDay+".txt");
			FileWriter fw=new FileWriter(createFile);
			for (Map<String, String> map : file) {
				fw.write(map.get("url").trim()+","+map.get("endTime").substring(0,10).trim());
				fw.write("\n");
			}
			fw.close();
			System.out.println("����ʱ����Ҫͳ��url�Ļ  �ļ������ɹ�  �������"+file.size());
			//�����ļ�/thetabin/push/logfile/

			InputStream inputStream = new FileInputStream(createFile);

			boolean boo=uploadFile("10.25.88.75",-1,"push","push123","/thetabin/push/","urlconfig.txt",inputStream);
			if(boo){
				System.out.println("�ϴ��ɹ���");
			}else{
				System.out.println("�ϴ�ʧ�ܣ�");
			}

			String directoryName=DateUtil.getNowDateMinusDayYYYYMMDD(1)+"/";
			File mkdir=new File("/interface/yangsy/getfile/"+directoryName);
			if(!mkdir.exists())mkdir.mkdir();
			boo=downloadFile("10.25.88.75",-1,"push","push123",
					"/thetabin/push/url/"+directoryName,"","/interface/yangsy/getfile/"+directoryName);
			if(boo){
				System.out.println("���سɹ���");
			}else{
				System.out.println("����ʧ�ܣ�");
			}

			if(boo){//���سɹ���ʼ�����ļ�
				initActiveUrlMap();
				File[] files=getFiles("/interface/yangsy/getfile/"+directoryName);
				List<Map<String,String>> list=null;
				for (File f : files) {
					list=readFileByLines(f);
					System.out.println("ִ�� �ļ�����:"+list.size());
					insertResultData(list);
				}

				//��������
				List<Map<String,String>> countActive=getPushActiveCount();
				for (Map<String, String> map : countActive) {
					updateEffectActive(map);
				}
			}
		}else{
			System.out.println("����ʱ��û����Ҫͳ��url�Ļ");
		}

	}

	/**
	 * ����ͳ�����ݱ�
	 * @param map
	 * @throws SQLException
	 */
	public static void updateEffectActive(Map<String,String> map) throws SQLException{
		int userNum=Integer.parseInt(map.get("pushc"));
		int flowNum=Integer.parseInt(map.get("sumflow"));
		String sql="update effect_active set tran_count=tran_count+"+userNum+",flow_count=flow_count+"+flowNum+" " +
				   		" where active_code='"+map.get("activeCode")+"'";
    	Connection conn=DataBaseJdbc.get85MysqlConnection();
		Statement stmt=null;
		stmt=conn.createStatement();
		stmt.executeUpdate(sql);
		System.out.println("���»��"+map.get("activeCode"));
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
            ftp.changeWorkingDirectory(path);
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
            ftp.changeWorkingDirectory(remotePath);//ת�Ƶ�FTP������Ŀ¼
            FTPFile[] fs = ftp.listFiles();
            for (FTPFile ff : fs){
                if ((null==fileName||"".equals(fileName))||ff.getName().equals(fileName)){
                    File localFile = new File(localPath + "" + ff.getName());
                    System.out.println("localFile:"+localFile);
                    if(!localFile.exists()) localFile.createNewFile();
                    OutputStream is = new FileOutputStream(localFile);
                    ftp.retrieveFile(ff.getName(), is);
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
    		//System.out.println("sql="+initSql);

			insertMysqlDB(initSql);
			} catch (Exception e) {
					System.out.println("�������ݿ������"+e.getMessage());
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
    public static void initActiveUrlMap(){
    	String sql="SELECT * FROM active_info WHERE  status=2 and url is NOT NULL and url <>''";
    	Connection conn=DataBaseJdbc.get85MysqlConnection();
		Statement stmt=null;
		ResultSet set=null;
		try {
			stmt=conn.createStatement();

		set = stmt.executeQuery(sql);
		while(set.next()){
			if(set.getString("url")!=null&&!"".equals(set.getString("url"))){
				activeUrl.put(set.getString("url").trim(),set.getString("active_code").trim());
			}
		}
		} catch (SQLException e) {
			System.out.println("��ʼ������"+e.getMessage());
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
            	ss=tempString.split("\\s+");
            	if(ss.length>2){
            		aum.put("phoneNo", ss[3]);
            		aum.put("url", ss[2]);
            		aum.put("flow", ss[4]);
            		addActiveCodeToUrl(aum); //continue;
            		slist.add(aum);
            	}
                System.out.println("line " + line + ": " + tempString);
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
			System.out.println("��ѯ������־������"+e);
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