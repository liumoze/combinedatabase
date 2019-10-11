/**
 * 
 */
package combinedatabase;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @ClassName:combin
 * @Description:把一个表中的数据插入到另一个表中，然后删除这个表
 * @author wuxiuhong
 * @date 2019年10月9日
 */
public class combin {
	
	public static void main(String[] args) {

		try {
			addData();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 数据转移
	 * @param startdate 开始日期
	 * @param enddate 结束日期
	 * @throws Exception 
	 */
	public static void addData(){
		
		List oriList = new ArrayList();   //存储history表名，例：logvalue_ups_history
		Map<String, String> failMap = new HashMap<>();     //存储转存失败的表名，例：fail_logvalue_ups_2019_09_08
		String oritablename = "";
		String oristr = "";  
		String failstr = "";
		List typeList = new ArrayList();   //存储截取后的history表名，例：ups
	
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			//建立连接
			conn = JdbcUtils.getConnection();
			//创建语句
			st = conn.createStatement();
			
			String sql1 = "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='123' and TABLE_NAME like 'logvalue_%history'";
			
			//执行语句
			rs = st.executeQuery(sql1);
			
			//获取history表名，例：logvalue_ups_history
			while(rs.next()){
				oristr = rs.getString("TABLE_NAME");
				oriList.add(oristr);
			}
			
			//获取设备类型名，存储到typeList中，例如：ups
			for (int j = 0; j < oriList.size(); j++) {
				oritablename = (String) oriList.get(j);
				typeList.add(oritablename.split("_")[1]);
			}
			
			//对设备类型进行遍历
			for (int j = 0; j < typeList.size(); j++) {
				String sql2 = "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='123' and TABLE_NAME like 'fail_logvalue_"+typeList.get(j)+"%'";
				
				rs = st.executeQuery(sql2);
				if(rs.next()==false)
					continue;
				rs.beforeFirst();
				while(rs.next()){
					failstr += rs.getString("TABLE_NAME")+",";
				}
				failMap.put((String) typeList.get(j), failstr);
				//failstrarr存储每个设备转存失败的表名，例如：fail_logvalue_ups_2019_09_08
				String[] failstrarr = failMap.get(typeList.get(j)).split(",");
				//comfaildate存储每个设备转存失败的表名中的日期，例，2019_09_08
				String comfaildate = "";
				//对String数组failstrarr进行遍历，并判断，如果这个表在输入的要整合的日期范围内就执行插入
				for (int k = 0; k < failstrarr.length; k++) {
					String[] faildatestr = failstrarr[k].split("_");
					comfaildate = faildatestr[3]+"_"+faildatestr[4]+"_"+faildatestr[5];
					         
				    String sql = "insert into "+oriList.get(j)+"(select * from "+failstrarr[k]+") ON DUPLICATE KEY UPDATE "+oriList.get(j)+".id="+failstrarr[k]+".id";
					st.executeUpdate(sql);
					failstr = "";
					
					String sql3 = "DROP TABLE "+failstrarr[k]+"";
					st.executeUpdate(sql3);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			JdbcUtils.free(rs, st, conn);
		}
	} 
}
