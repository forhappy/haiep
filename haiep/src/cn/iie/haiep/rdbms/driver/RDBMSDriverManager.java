package cn.iie.haiep.rdbms.driver;

import java.sql.*;

import oracle.jdbc.pool.OracleDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author forhappy
 * Date: 2012-2-27, 10:52 PM.
 */

/**
 * Create Connection using JDBC.
 */
public class RDBMSDriverManager {
	
	public RDBMSDriverManager() {
	}

	/**
	 * Constructor
	 * 
	 * @param url
	 *            a database url of the form "jdbc:subprotocol:subname".
	 * @param user
	 *            the database user on whose behalf the connection is being made.
	 * @param password
	 *            the user's password.
	 */
	public RDBMSDriverManager(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Attempts to establish a connection to the given database.
	 * 
	 * @return a connection to the URL.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Connection createConnection() throws SQLException {
		Connection conn = null;
		try {
//			Class.forName("com.mysql.jdbc.Driver").newInstance();
//			Class.forName("dm.jdbc.driver.DmDriver").newInstance();
			Class.forName("oracle.jdbc.OracleDriver").newInstance();
			conn = DriverManager.getConnection(this.url, this.user,
					this.password);
			return conn;
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Attempts to establish a connection to the given database URL.
	 * 
	 * @param url
	 *            a database url of the form "jdbc:subprotocol:subname".
	 * @return a connection to the URL
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Connection createConnection(String url) throws SQLException {
		Connection conn = null;
		try {
//			Class.forName("com.mysql.jdbc.Driver").newInstance();
//			Class.forName("dm.jdbc.driver.DmDriver").newInstance();
			Class.forName("oracle.jdbc.OracleDriver").newInstance();
			conn = DriverManager.getConnection(url);
			return conn;
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Attempts to establish a connection to the given database URL. It 
	 * attempts to select an appropriate driver from the set of registered
	 * JDBC drivers.
	 * 
	 * @param url
	 *            a database url of the form "jdbc:subprotocol:subname".
	 * @param info
	 *            a list of arbitrary string tag/value pairs as connection
	 *            arguments; normally at least a "user" and "password" property
	 *            should be included
	 * @return a Connection to the URL
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public static Connection createConnection(String url,
			java.util.Properties prop) throws SQLException {
		Connection conn = null;
		try {
//			Class.forName("com.mysql.jdbc.Driver").newInstance();
//			Class.forName("dm.jdbc.driver.DmDriver").newInstance();
			Class.forName("oracle.jdbc.OracleDriver").newInstance();
			conn = DriverManager.getConnection(url, prop);
			return conn;
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Attempts to establish a connection to the given database URL.
	 * 
	 * @param url
	 *            a database url of the form "jdbc:subprotocol:subname".
	 * @param user
	 *            the database user on whose behalf the connection is being made.
	 * @param password
	 *            the user's password.
	 * @return a connection to the URL.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public static Connection createOracleConnection(String url, String user,
			String password) throws SQLException {
		Connection conn = null;
		try {
			DriverManager.registerDriver( new oracle.jdbc.driver.OracleDriver());
			OracleDataSource ods = new OracleDataSource();
			ods.setURL(url);
			ods.setUser(user);
			ods.setPassword(password);
			conn = ods.getConnection();
			return conn;
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Attempts to establish a connection to the given database URL.
	 * 
	 * @param url
	 *            a database url of the form "jdbc:subprotocol:subname".
	 * @param user
	 *            the database user on whose behalf the connection is being made.
	 * @param password
	 *            the user's password.
	 * @return a connection to the URL.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public static Connection createConnection(String url, String user,
			String password) throws SQLException {
		Connection conn = null;
		try {
//			Class.forName("com.mysql.jdbc.Driver").newInstance();
//			Class.forName("dm.jdbc.driver.DmDriver").newInstance();
			Class.forName("oracle.jdbc.OracleDriver").newInstance();
			conn = DriverManager.getConnection(url, user, password);
			return conn;
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/**
     * Close a Connection, avoid closing if null.
     *
     * @param conn Connection to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(Connection conn) throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * Close a ResultSet, avoid closing if null.
     *
     * @param rs ResultSet to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    /**
     * Close a Statement, avoid closing if null.
     *
     * @param stmt Statement to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(Statement stmt) throws SQLException {
        if (stmt != null) {
            stmt.close();
        }
    }
    
    /**
     * Close a PreparedStatement, avoid closing if null.
     *
     * @param pstmt Statement to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(PreparedStatement pstmt) throws SQLException {
        if (pstmt != null) {
            pstmt.close();
        }
    }

    /**
     * Close a Connection, avoid closing if null and hide
     * any SQLExceptions that occur.
     *
     * @param conn Connection to close.
     */
    public static void closeQuietly(Connection conn) {
        try {
            close(conn);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * Close a Connection, Statement and
     * ResultSet.  Avoid closing if null and hide any
     * SQLExceptions that occur.
     *
     * @param conn Connection to close.
     * @param stmt Statement to close.
     * @param rs ResultSet to close.
     */
    public static void closeQuietly(Connection conn, Statement stmt,
            ResultSet rs) {

        try {
            closeQuietly(rs);
        } finally {
            try {
                closeQuietly(stmt);
            } finally {
                closeQuietly(conn);
            }
        }

    }
    
    /**
     * Close a Connection, Statement and
     * ResultSet.  Avoid closing if null and hide any
     * SQLExceptions that occur.
     *
     * @param conn Connection to close.
     * @param pstmt Statement to close.
     * @param rs ResultSet to close.
     */
    public static void closeQuietly(Connection conn, PreparedStatement pstmt,
            ResultSet rs) {

        try {
            closeQuietly(rs);
        } finally {
            try {
                closeQuietly(pstmt);
            } finally {
                closeQuietly(conn);
            }
        }

    }

    /**
     * Close a ResultSet, avoid closing if null and hide any
     * SQLExceptions that occur.
     *
     * @param rs ResultSet to close.
     */
    public static void closeQuietly(ResultSet rs) {
        try {
            close(rs);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * Close a Statement, avoid closing if null and hide
     * any SQLExceptions that occur.
     *
     * @param stmt Statement to close.
     */
    public static void closeQuietly(Statement stmt) {
        try {
            close(stmt);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }
    
    /**
     * Close a PreparedStatement, avoid closing if null and hide
     * any SQLExceptions that occur.
     *
     * @param pstmt Statement to close.
     */
    public static void closeQuietly(PreparedStatement pstmt) {
        try {
            close(pstmt);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

	/**
	 * Private fields.
	 */
	
	/**
	 * a database url of the form "jdbc:subprotocol:subname".
	 */
	private String url = null;
	
	/**
	 * the database user on whose behalf the connection is being made.
	 */
	private String user = null;
	
	/**
	 * the user's password.
	 */
	private String password = null;
	
	/**
	 * logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(RDBMSDriverManager.class);
}
