package com.cdata.mcp;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.cdata.mcp.StringUtil.isNullOrEmpty;

public class Config {
  private static final String PREFIX = "Prefix";
  private static final String DRIVER = "DriverClass";
  private static final String DRIVER_JAR = "DriverPath";
  private static final String JDBC_URL = "JdbcUrl";
  private static final String TABLES = "Tables";
  private static final String LOG_FILE = "LogFile";

  // following properties will be discovered dynamically from driver
  private static final String ID_QUOTE_OPEN_CHAR = "IDENTIFIER_QUOTE_OPEN_CHAR";
  private static final String ID_QUOTE_CLOSE_CHAR = "IDENTIFIER_QUOTE_CLOSE_CHAR";
  private static final String SUPPORTS_MULTIPLE_CATALOGS = "SUPPORTS_MULTIPLE_CATALOGS";
  private static final String SUPPORTS_MULTIPLE_SCHEMAS = "SUPPORTS_MULTIPLE_SCHEMAS";
  private Properties props = new Properties();
  private Properties sqlInfo = new Properties();
  private Driver driver;
  private String defCatalog;
  private String defSchema;

  public void load(String filepath) throws IOException {
    if (filepath != null && !filepath.isEmpty()) {
      try (FileInputStream fis = new FileInputStream(filepath)) {
        props.load(fis);
      }
    }
    loadFromEnvironment();
  }

  private void loadFromEnvironment() {
    String prefix = System.getenv("CDATA_PREFIX");
    if (prefix != null) {
      props.setProperty(PREFIX, prefix);
    } else if (!props.containsKey(PREFIX)) {
      props.setProperty(PREFIX, "salesforce");
    }
    
    String driverPath = System.getenv("CDATA_DRIVER_PATH");
    if (driverPath != null) {
      props.setProperty(DRIVER_JAR, driverPath);
    } else if (!props.containsKey(DRIVER_JAR)) {
      // Set default to "bundled" to indicate classes are in the classpath
      props.setProperty(DRIVER_JAR, "bundled");
    }
    
    String driverClass = System.getenv("CDATA_DRIVER_CLASS");
    if (driverClass != null) {
      props.setProperty(DRIVER, driverClass);
    } else if (!props.containsKey(DRIVER)) {
      // Set default driver class based on whether we're using bundled JAR
      String jarPath = props.getProperty(DRIVER_JAR);
      if ("bundled".equals(jarPath)) {
        // For bundled JAR, we know it's the Salesforce driver
        props.setProperty(DRIVER, "cdata.jdbc.salesforce.SalesforceDriver");
      }
    }
    
    String jdbcUrl = System.getenv("CDATA_JDBC_URL");
    if (jdbcUrl != null) props.setProperty(JDBC_URL, jdbcUrl);
    
    String tables = System.getenv("CDATA_TABLES");
    if (tables != null) props.setProperty(TABLES, tables);
    
    String logFile = System.getenv("CDATA_LOG_FILE");
    if (logFile != null) props.setProperty(LOG_FILE, logFile);
  }

  public boolean validate(PrintStream errors) {
    boolean result = true;
    if (isNullOrEmpty(getPrefix())) {
      errors.println("The '" + PREFIX + "' option is missing");
      result = false;
    }

    // Driver class is required unless using bundled JAR with known driver
    String driverJar = getDriverJar();
    if (isNullOrEmpty(getDriver())) {
      if (!"bundled".equals(driverJar)) {
        errors.println("The '" + DRIVER + "' option is missing");
        result = false;
      }
    }

    if (isNullOrEmpty(driverJar)) {
      errors.println("The '" + DRIVER_JAR + "' option is missing");
      result = false;
    } else if (!verifyDriverLoad(errors)) {
      result = false;
    }

    if (isNullOrEmpty(getJdbcUrl())) {
      errors.println("The '" + JDBC_URL + "' option is missing");
      result = false;
    } else if (result && !verifyJdbcUrl(errors)) {
      result = false;
    }
    return result;
  }

  public String getServerName() {
    return this.getPrefix();
  }
  public String getServerVersion() {
    return "1.0";
  }
  public String getPrefix() {
    return this.props.getProperty(PREFIX);
  }
  public String getMcpScheme() {
    return getPrefix() + "://";
  }
  public String getDriver() {
    return this.props.getProperty(DRIVER);
  }
  public String getDriverJar() {
    return this.props.getProperty(DRIVER_JAR);
  }
  public String getJdbcUrl() {
    return this.props.getProperty(JDBC_URL);
  }
  public List<Table> getTables() throws SQLException {
    String tables = this.props.getProperty(TABLES);
    if (isNullOrEmpty(tables)) {
      return new ArrayList<>();
    }
    List<Table> entries = Table.parseList(tables);
    return completeTableList(entries);
  }
  public String getIdentifierQuotes() {
    return this.sqlInfo.getProperty(ID_QUOTE_OPEN_CHAR)
        + this.sqlInfo.getProperty(ID_QUOTE_CLOSE_CHAR);
  }
  public boolean supportsMultipleCatalogs() {
    String val = this.sqlInfo.getProperty(SUPPORTS_MULTIPLE_CATALOGS);
    return val.equalsIgnoreCase("YES");
  }
  public boolean supportsMultipleSchemas() {
    String val = this.sqlInfo.getProperty(SUPPORTS_MULTIPLE_SCHEMAS);
    return val.equalsIgnoreCase("YES");
  }
  public String defaultCatalog() {
    return this.defCatalog;
  }
  public String defaultSchema() {
    return this.defSchema;
  }

  public String getLogFile() {
    return this.props.getProperty(LOG_FILE);
  }

  public String quoteIdentifier(String id) {
    String open = this.sqlInfo.getProperty(ID_QUOTE_OPEN_CHAR);
    String close = this.sqlInfo.getProperty(ID_QUOTE_CLOSE_CHAR);
    // TODO: Properly escape things
    return open + id + close;
  }

  public Connection newConnection() throws SQLException {
    return this.driver.connect(this.getJdbcUrl(), new Properties());
  }

  private boolean verifyDriverLoad(PrintStream errors) {
    String driverJar = getDriverJar();
    
    // Check if it's bundled (classes already in classpath)
    if (driverJar.equals("bundled")) {
      try {
        loadDriver();
        return true;
      } catch (Throwable t) {
        String msg = t.getClass().getName() + ": " + t.getMessage();
        errors.println("Attempting to load the bundled JDBC driver failed: " + msg);
        return false;
      }
    }
    
    // First check if it's a bundled resource
    if (driverJar.startsWith("resource:")) {
      try {
        loadDriver();
        return true;
      } catch (Throwable t) {
        String msg = t.getClass().getName() + ": " + t.getMessage();
        errors.println("Attempting to load the bundled JDBC driver failed: " + msg);
        return false;
      }
    }
    
    // Otherwise check if it's a file path
    if (!new File(driverJar).exists()) {
      errors.println("The '" + DRIVER_JAR + "' option is not a valid JAR file");
      return false;
    }
    try {
      loadDriver();
      return true;
    } catch (Throwable t) {
      String msg = t.getClass().getName() + ": " + t.getMessage();
      errors.println("Attempting to load the JDBC driver failed: " + msg);
    }
    return false;
  }

  private boolean verifyJdbcUrl(PrintStream errors) {
    try {
      try (Connection cn = newConnection()) {
      }
      return true;
    } catch ( SQLException ex ) {
      errors.println("Failed to open JDBC connection: " + ex.getMessage());
    }
    return false;
  }

  private void loadDriver() throws Exception {
    String driverJar = this.getDriverJar();
    
    if (driverJar.equals("bundled")) {
      // Classes are already in the classpath, load directly
      Class dc = Class.forName(this.getDriver());
      this.driver = (Driver)dc.getDeclaredConstructor().newInstance();
    } else if (driverJar.startsWith("resource:")) {
      // Load from bundled resources
      String resourcePath = driverJar.substring("resource:".length());
      URL resourceUrl = this.getClass().getClassLoader().getResource(resourcePath);
      if (resourceUrl == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      URLClassLoader ucl = new URLClassLoader(
          new URL[] { resourceUrl },
          this.getClass().getClassLoader()
      );
      Class dc = ucl.loadClass(this.getDriver());
      this.driver = (Driver)dc.getDeclaredConstructor().newInstance();
    } else {
      // Load from file system
      URLClassLoader ucl = new URLClassLoader(
          new URL[] {
              new File(driverJar).toURI().toURL(),
          },
          this.getClass().getClassLoader()
      );
      Class dc = ucl.loadClass(this.getDriver());
      this.driver = (Driver)dc.getDeclaredConstructor().newInstance();
    }

    loadSqlInfo();
  }

  private void loadSqlInfo() throws SQLException {
    try (Connection cn = newConnection()) {
      retrieveSqlInfo(cn);
      if (!this.supportsMultipleCatalogs()) {
        if (!this.supportsMultipleSchemas()) {
          retrieveDefaultCatalogAndSchema(cn);
        } else {
          retrieveDefaultCatalog(cn);
        }
      }
    }
  }

  private void retrieveDefaultCatalogAndSchema(Connection cn) throws SQLException {
    DatabaseMetaData meta = cn.getMetaData();
    try (ResultSet rs = meta.getSchemas()) {
      rs.next();
      this.defCatalog = rs.getString("TABLE_CATALOG");
      this.defSchema = rs.getString("TABLE_SCHEM");
    }
  }

  private void retrieveDefaultCatalog(Connection cn) throws SQLException {
    DatabaseMetaData meta = cn.getMetaData();
    try (ResultSet rs = meta.getCatalogs()) {
      rs.next();
      this.defCatalog = rs.getString(1);
    }
  }

  private void retrieveSqlInfo(Connection cn) throws SQLException {
    try (Statement st = cn.createStatement()) {
      try (ResultSet rs = st.executeQuery("SELECT NAME, VALUE FROM sys_sqlinfo")) {
        while (rs.next()) {
          String key = rs.getString(1);
          String value = rs.getString(2);
          if (value == null) {
            value = "";
          }
          this.sqlInfo.put(key, value);
        }
      }
    }
  }

  private List<Table> completeTableList(List<Table> list) throws SQLException {
    List<Table> result = new ArrayList<>();
    try (Connection cn = newConnection()) {
      DatabaseMetaData meta = cn.getMetaData();
      // Not the most efficient, but oh well
      for (Table t : list) {
        addMatchingTables(t, meta, result);
      }
    }
    return result;
  }

  private void addMatchingTables(Table t, DatabaseMetaData meta, List<Table> result) throws SQLException {
    String catalog = t.hasCatalog() ? t.catalog() : null;
    String schema = t.hasSchema() ? t.schema() : null;
    String name = t.name();
    try (ResultSet rs = meta.getTables(catalog, schema, name, null)) {
      while (rs.next()) {
        catalog = rs.getString("TABLE_CAT");
        schema = rs.getString("TABLE_SCHEM");
        name = rs.getString("TABLE_NAME");
        result.add(new Table(catalog, schema, name));
      }
    }
  }
}
