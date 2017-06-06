package com.telefonica.gbic.global;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * DataQualityToolTestChecker
 * 
 * Run test evaluations (both test results and test variations) in MySQL and
 * store results in test_result table. Returns an Oozie property with the result
 * of the evaluation.
 * 
 * @version 1.0
 * @since 05/07/2016
 */
public class DataQualityToolTestChecker {
    
    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; //$NON-NLS-1$
    
    // Oozie action output properties
    private static final String OOZIE_PROPERTIES = "oozie.action.output.properties"; //$NON-NLS-1$
    
    // Test result flag
    private static final String TESTS_PASSED = GbicGPlatformConfig.getValue("data_quality.file_revision.tests_passed"); //$NON-NLS-1$
    
    // Errors
    private static final String OOZIE_PROP_ERROR = GbicGPlatformConfig.getValue("data_quality.error.oozie_properties"); //$NON-NLS-1$
    
    // Constants
    private static final String PARTITION_SUBS_FIELD = GbicGPlatformConfig.getValue("data_quality.partition_field.substitution"); //$NON-NLS-1$
    private static final String TEST_RESULT_ERROR = GbicGPlatformConfig.getValue("data_quality.tests.test_result.error"); //$NON-NLS-1$
    
    // Queries
    private static final String QUERY_OK_TESTS = 
        " SELECT id_filerevision, "                                                                                             + //$NON-NLS-1$
        "        id_test, "                                                                                                     + //$NON-NLS-1$
        "        test_type, "                                                                                                   + //$NON-NLS-1$
        "        1, "                                                                                                           + //$NON-NLS-1$
        "        test_number_file, "                                                                                            + //$NON-NLS-1$
        "        test_field, "                                                                                                  + //$NON-NLS-1$
        "        test_field_content, "                                                                                          + //$NON-NLS-1$
        "        test_expected_value, "                                                                                         + //$NON-NLS-1$
        "        screen_value, "                                                                                                + //$NON-NLS-1$
        "        warn_threshold, "                                                                                              + //$NON-NLS-1$
        "        error_threshold , "                                                                                            + //$NON-NLS-1$
        "        CASE "                                                                                                         + //$NON-NLS-1$
        "            WHEN test_expected_value = screen_value THEN 'PASS' "                                                      + //$NON-NLS-1$
        "            WHEN (100 * abs(screen_value - test_expected_value) / field_total_value ) < error_threshold THEN 'WARN' "  + //$NON-NLS-1$
        "            ELSE 'ERROR' "                                                                                             + //$NON-NLS-1$
        "        END AS test_state "                                                                                            + //$NON-NLS-1$
        " FROM screen_test "                                                                                                    + //$NON-NLS-1$
        " WHERE test_field != 'rec_total' "                                                                                     + //$NON-NLS-1$
        "   AND id_filerevision = ? ";                                                                                            //$NON-NLS-1$
    
    private static final String QUERY_VARIATION_TESTS = 
        " SELECT *, "                                                                                                                    + //$NON-NLS-1$
        "        CASE "                                                                                                                  + //$NON-NLS-1$
        "            WHEN test_expected_value = test_resulting_value THEN 'PASS'"                                                        + //$NON-NLS-1$
        "            WHEN (100 * abs(test_resulting_value - test_expected_value) / test_resulting_value ) < error_threshold THEN 'WARN'" + //$NON-NLS-1$
        "            ELSE 'ERROR'"                                                                                                       + //$NON-NLS-1$
        "        END AS test_state"                                                                                                      + //$NON-NLS-1$
        " FROM ("                                                                                                                        + //$NON-NLS-1$
        "     SELECT t1.id_filerevision,"                                                                                                + //$NON-NLS-1$
        "            t1.id_test, "                                                                                                       + //$NON-NLS-1$
        "            t1.test_type,"                                                                                                      + //$NON-NLS-1$
        "            1,"                                                                                                                 + //$NON-NLS-1$
        "            t1.test_number_file,"                                                                                               + //$NON-NLS-1$
        "            t1.test_field,"                                                                                                     + //$NON-NLS-1$
        "            t1.screen_field_content,"                                                                                           + //$NON-NLS-1$
        "            if(t2.screen_value IS NULL, t1.screen_value,avg(t2.screen_value)) AS test_expected_value,"                          + //$NON-NLS-1$
        "            t1.screen_value AS test_resulting_value,"                                                                           + //$NON-NLS-1$
        "            t1.warn_threshold,"                                                                                                 + //$NON-NLS-1$
        "            t1.error_threshold "                                                                                                + //$NON-NLS-1$
        "     FROM "                                                                                                                     + //$NON-NLS-1$
        "         screen_test t1 "                                                                                                       + //$NON-NLS-1$
        "     LEFT OUTER JOIN"                                                                                                           + //$NON-NLS-1$
        "         screen_test t2"                                                                                                        + //$NON-NLS-1$
        "     ON t1.screen_field = 'rec_total'"                                                                                          + //$NON-NLS-1$
        "       AND t1.id_fileentity = t2.id_fileentity "                                                                                + //$NON-NLS-1$
        "       AND t1.screen_field = t2.screen_field "                                                                                  + //$NON-NLS-1$
        "       AND t2.content_dt between date_sub(t1.content_dt,INTERVAL ? "+ DataQualityToolTestChecker.PARTITION_SUBS_FIELD +")"      + //$NON-NLS-1$ //$NON-NLS-2$
        "       AND date_sub(t1.content_dt,INTERVAL 1 "+ DataQualityToolTestChecker.PARTITION_SUBS_FIELD +")"                            + //$NON-NLS-1$ //$NON-NLS-2$
        "     GROUP BY "                                                                                                                 + //$NON-NLS-1$
        "       t1.id_filerevision, t1.id_test, t1.test_type, t1.test_number_file, "                                                     + //$NON-NLS-1$
        "       t1.test_field, t1.screen_field_content, t1.screen_value, t1.warn_threshold, t1.error_threshold"                          + //$NON-NLS-1$
        " ) X"                                                                                                                           + //$NON-NLS-1$
        " WHERE test_field='rec_total' AND id_filerevision = ?";                                                                           //$NON-NLS-1$
    
    private static final String QUERY_CHECK_VALIDATION = 
        " SELECT * "                   + //$NON-NLS-1$
        " FROM   test_result "         + //$NON-NLS-1$
        " WHERE  id_filerevision = ?";   //$NON-NLS-1$
    
    private static final String INSERT_TEST_RESULTS = 
        " INSERT INTO test_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?) " + //$NON-NLS-1$
        " ON DUPLICATE KEY UPDATE "                                  + //$NON-NLS-1$
        "   test_expected_value = ?, "                               + //$NON-NLS-1$
        "   test_resulting_value = ?, "                              + //$NON-NLS-1$
        "   warn_threshold = ?, "                                    + //$NON-NLS-1$
        "   error_threshold = ?, "                                   + //$NON-NLS-1$
        "   test_state = ?";                                           //$NON-NLS-1$
    
    // MySQL variables
    private Connection conn = null;
    private PreparedStatement stmt;
    private PreparedStatement stmt2;
    
    // Debug message
    private String msg;
    
    /**
     * Constructor method
     * 
     * @param database
     *            MySQL database URL
     * @param user
     *            MySQL username
     * @param password
     *            MySQL password
     */
    public DataQualityToolTestChecker(String database, String user, String password) {
        msg = "New DQTTChecker";
        try {
            try {
                Class.forName(DataQualityToolTestChecker.JDBC_DRIVER);
            } catch (ClassNotFoundException cnfe) {
                msg = "" + cnfe;
            }
            conn = DriverManager.getConnection(database, user, password);
            
        } catch (SQLException se) {
            // Handle errors for JDBC
            msg = "" + se;
            se.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            msg = "" + e;
            e.printStackTrace();
        }
    }
    
    /**
     * Close connection to MySQL database
     */
    private void closeConnection() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
    
    /**
     * Run all tests over a file_revision
     * 
     * @param revision
     *            id_filerevision to be evaluated
     * @param timeWindowOffset
     *            size of the window to analyze variations
     * @param partitionField
     *            name of the time partition field (MONTH, DAY...)
     */
    private void runRevisionTests(int revision, int timeWindowOffset, String partitionField) {
        this.runOkTests(revision);
        this.runVariationTests(revision,timeWindowOffset, partitionField);
    }
    
    /**
     * Run tests for a given month and store the result in test_result
     * 
     * @param revision
     *            id_filerevision to be evaluated
     */
    private void runOkTests(int revision) {
        ResultSet rs;
        msg = "\nRunning revision tests.......";
        
        try {
            stmt = conn.prepareStatement(DataQualityToolTestChecker.QUERY_OK_TESTS);
            stmt.setInt(1, revision);
            rs = stmt.executeQuery();
            this.insertTestResults(rs);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            msg += "\n" + e; //$NON-NLS-1$
            try {
                throw e;
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }
    
    /**
     * Run variation tests for a given month and a window, and store the result
     * in test_result
     * 
     * @param revision
     *            id_filerevision to be evaluated
     * @param windowSize
     *            size of the time window to be considered
     * @param partitionField
     *            name of the time partition field (MONTH, DAY...)
     */
    private void runVariationTests(int revision, int windowSize, String partitionField) {
        ResultSet rs;
        msg = "\nRunning revision tests.......";
        
        try {
            stmt = conn.prepareStatement(DataQualityToolTestChecker.QUERY_VARIATION_TESTS.replaceAll(DataQualityToolTestChecker.PARTITION_SUBS_FIELD, partitionField));
            stmt.setInt(1, windowSize);
            stmt.setInt(2, revision);
            rs = stmt.executeQuery();
            this.insertTestResults(rs);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            msg += "\n" + e; //$NON-NLS-1$
            try {
                throw e;
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }
    
    /**
     * Insert test results into test_result table
     * 
     * @param rs
     *            ResultSet to be inserted with the results of the test
     * @throws SQLException
     */
    private void insertTestResults(ResultSet rs) throws SQLException {
        stmt2 = conn.prepareStatement(DataQualityToolTestChecker.INSERT_TEST_RESULTS);
        
        while (rs.next()) {
            stmt2.setInt(1,rs.getInt(1));
            stmt2.setInt(2,rs.getInt(2));
            stmt2.setString(3,rs.getString(3));
            stmt2.setInt(4,rs.getInt(4));
            stmt2.setInt(5,rs.getInt(5));
            stmt2.setString(6,rs.getString(6));
            stmt2.setString(7,rs.getString(7));
            stmt2.setDouble(8,rs.getDouble(8));
            stmt2.setDouble(9,rs.getDouble(9));
            stmt2.setFloat(10,rs.getFloat(10));
            stmt2.setFloat(11,rs.getFloat(11));
            stmt2.setString(12,rs.getString(12));
            stmt2.setString(13,rs.getString(8));
            stmt2.setString(14,rs.getString(9));
            stmt2.setString(15,rs.getString(10));
            stmt2.setString(16,rs.getString(11));
            stmt2.setString(17,rs.getString(12));
            stmt2.addBatch();
        }
        
        stmt2.executeBatch();
        stmt2.close();
    }
    
    /**
     * Check the test results for a given revision
     * 
     * @param revision
     *            id_filerevision to be evaluated
     * @return True if the revision has no "ERROR" results
     */
    private boolean isValidFile(int revision) {
        boolean isValid = true;
        ResultSet rs;
        msg = "\nChecking Validation.......";
         
        try {
            stmt = conn.prepareStatement(DataQualityToolTestChecker.QUERY_CHECK_VALIDATION);
            stmt.setInt(1, revision);
            rs = stmt.executeQuery();
            while ( rs.next() ) {
                if ( rs.getString(12).equals(DataQualityToolTestChecker.TEST_RESULT_ERROR) ) {
                    isValid = false;
                    msg += "\n Data Quality Error. File revision: " + rs.getInt(1) + ". Test: " + rs.getString(6);
                }
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            msg += "\n" + e; //$NON-NLS-1$
            try {
                throw e;
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        
        return isValid;
    }
    
    /**
     * DataQualityToolTestChecker main method
     * 
     * @param idFilerevision
     *            id_filerevision to be evaluated
     * @param timeWindowOffset
     *            size of the window to analyze variations
     * @param partitionField
     *            name of the time partition field (MONTH, DAY...)
     * @param database
     *            MySQL database URL
     * @param user
     *            MySQL username
     * @param password
     *            MySQL password
     */
    public static void main(String[] args) {
        DataQualityToolTestChecker dqt = null;
        try {
            // Arguments reading
            int idFilerevision = Integer.valueOf(args[0]);
            int timeWindowOffset = Integer.valueOf(args[1]);
            String partitionField = args[2];
            String database =  args[3];
            String user =  args[4];
            String password =  args[5];
            
            dqt = new DataQualityToolTestChecker(database, user, password);
            
            dqt.runRevisionTests(idFilerevision, timeWindowOffset, partitionField);
            boolean testPassed = dqt.isValidFile(idFilerevision);
            
            String oozieProp = System.getProperty(DataQualityToolTestChecker.OOZIE_PROPERTIES);
            OutputStream os = null;
            if (oozieProp != null) {
                File propFile = new File(oozieProp);
                Properties p = new Properties();
                p.setProperty(DataQualityToolTestChecker.TESTS_PASSED, Boolean.toString(testPassed));
                
                try {
                    os = new FileOutputStream(propFile);
                    p.store(os, ""); //$NON-NLS-1$
                    os.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                throw new RuntimeException(DataQualityToolTestChecker.OOZIE_PROP_ERROR);
            }
            dqt.closeConnection();
            
        } finally {
            System.err.println("DEBUG MESSAGES: " + dqt.msg); 
        }
    }
}
