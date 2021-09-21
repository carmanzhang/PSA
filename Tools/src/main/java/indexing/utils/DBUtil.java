package indexing.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBUtil {
    private static Properties properties = new Properties();
    private static final DruidDataSource ds = new DruidDataSource();
    private static Connection conn = null;

    static {
        InputStream in = null;
        try {
            in = DBUtil.class.getClassLoader().getResourceAsStream("jdbc.properties");
            properties.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        ds.setConnectProperties(properties);
    }


    public static Connection getConn(long ...tid) {
        DruidPooledConnection connection = null;
        try {
            connection = ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Connection getFixedConn(long ...tid) {
        if (conn != null) {
            return conn;
        }
        conn = getConn();
        return conn;
    }

    public static void closeConn(Connection conn, long ...tid) {
        if (conn instanceof DruidPooledConnection) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean createTable(Statement statement, String sql) throws SQLException {
        return statement.execute(sql);
    }
}
