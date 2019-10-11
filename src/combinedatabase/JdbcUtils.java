/**
 * 
 */
package combinedatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @ClassName:JdbcUtils
 * @Description:数据库连接工具类
 * @author wuxiuhong
 * @date 2019年10月9日
 */
public final class JdbcUtils {
	//数据库连接名称
	private static String user="root";
    //数据库连接密码
	private static String password="aaaaaa";
    //其中ceims为数据库名称
	private static String url="jdbc:mysql://localhost:3306/123";
    
    private JdbcUtils(){
    	
    }
    
    static{      //静态块只执行一次
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    //获取连接
    public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url,user,password);
	}
    
    //释放连接
    public static void free(ResultSet rs,Statement st,Connection conn) {
		try {
			if (rs!=null) 
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				try {
					if (st!=null) 
						st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}finally{
					if (conn!=null) 
						try {
							conn.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
			}
		}
	}
    
 
