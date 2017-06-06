package com.telefonica.gbic.global.oozie.action.hadoop;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.text.MessageFormat;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;

import com.telefonica.gbic.global.GbicGPlatformConfig;

/**
 * EL function for fs action executor.
 */
public class GbicELFunctions {
    
    // Paths
    private static final String SCREENS_PATH = GbicGPlatformConfig.getValue("data_quality.screens.path"); //$NON-NLS-1$
    private static final String TESTS_PATH = GbicGPlatformConfig.getValue("data_quality.tests.path"); //$NON-NLS-1$
    private static final String DATA_DIR_PATH = GbicGPlatformConfig.getValue("data_quality.data_dirs.path"); //$NON-NLS-1$
    private static final String PIG_PATH = GbicGPlatformConfig.getValue("data_quality.pig.path"); //$NON-NLS-1$
    private static final String SCREENS_PATH_WITHOUT_VERSION = GbicGPlatformConfig.getValue("data_quality.screens.path_without_version"); //$NON-NLS-1$
    private static final String TESTS_PATH_WITHOUT_VERSION = GbicGPlatformConfig.getValue("data_quality.tests.path_without_version"); //$NON-NLS-1$
    private static final String DATA_DIR_PATH_WITHOUT_VERSION = GbicGPlatformConfig.getValue("data_quality.data_dirs.path_without_version"); //$NON-NLS-1$
    private static final String PIG_PATH_WITHOUT_VERSION = GbicGPlatformConfig.getValue("data_quality.pig.path_without_version"); //$NON-NLS-1$
    
    public static FileSystem getFileSystem(URI uri) throws HadoopAccessorException {
        WorkflowJob workflow = DagELFunctions.getWorkflow();
        String user = workflow.getUser();
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        JobConf conf = has.createJobConf(uri.getAuthority());
        return has.createFileSystem(user, uri, conf);
    }
    
    /**
     * Get file checksum.
     *
     * @param hdfsInbox      Files inbox in hdfs
     * @param ob3            Three letters OB identifier
     * @param version        Project version (can be empty or null if there is no version)
     * @param fileName       Name of the file
     * @param partitionField Name of the date partition field (month, day, etc)
     * @param nominalTime    Day in format yyyy-MM-dd
     * @return String        File checksum (or empty if files do not exist)
     * @throws URISyntaxException
     * @throws IOException
     * @throws Exception
     */
    public static String getFileChecksum(String hdfsInbox, String ob3, String version, String fileName, String partitionField, String nominalTime) throws Exception {
        String dataFilePath = GbicELFunctions.getDataDirPath(hdfsInbox, ob3, version, fileName, partitionField, nominalTime);
        
        URI uri = new URI(dataFilePath);
        String path = uri.getPath();
        FileSystem fs = getFileSystem(uri);
        Path p = new Path(path);
        String checksums = ""; //$NON-NLS-1$
        String hashtext = ""; //$NON-NLS-1$
        
        //Get a string with all the checksums of the files concatenated
        if(fs.exists(p)){
            FileStatus[] listOfStatus = fs.listStatus(p);
            
            for(FileStatus status : listOfStatus){
                checksums+= fs.getFileChecksum(status.getPath()).toString()+"|"; //$NON-NLS-1$
            }
        }
        
        //Get the checksum of the string with all the checksums concatenated
        if (checksums!= null && !checksums.isEmpty()) {
            MessageDigest m;
            m = MessageDigest.getInstance("MD5"); //$NON-NLS-1$
            m.reset();
            m.update(checksums.getBytes());
            byte[] digest = m.digest();
            BigInteger bigInt = new BigInteger(1, digest);
            hashtext = bigInt.toString(16);
            
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext; //$NON-NLS-1$
            }
        }
        
        return hashtext;
    }
    
    /**
     * Oozie EL function to convert every chararcter of a string to uppercase
     *
     * @param str     Input string
     * @return String Converted to uppercase
     */
    public static String toUpperCase(String str){
        return str != null ? str.toUpperCase() : null;
    }
    
    /**
     * Oozie EL function to convert every chararcter of a string to lowercase
     *
     * @param str     Input string
     * @return String Converted to lowercase
     */
    public static String toLowerCase(String str){
        return str != null ? str.toLowerCase() : null;
    }
    
    /**
     * Oozie EL function to get the TestFile path with arguments given
     *
     * @param scriptsPath   Path in hdfs with the script files
     * @param version       Project version (can be empty or null if there is no version)
     * @param fileName      File name owner of the test
     * @param execution     'pre' or 'post' to indicate if it is executed before or after the ETL
     * @param counter       Id of the test
     * @return String       Path of the test file
     */
    public static String getTestFilePath(String scriptsPath, String version, String fileName, String execution, int counter) throws Exception {
        if(version == null || version.isEmpty()){
            return MessageFormat.format(GbicELFunctions.TESTS_PATH_WITHOUT_VERSION,
                                        scriptsPath,
                                        fileName,
                                        execution,
                                        counter);
        } else {
            return MessageFormat.format(GbicELFunctions.TESTS_PATH,
                                        scriptsPath,
                                        version,
                                        fileName,
                                        execution,
                                        counter);
        }
    }
    
    /**
     * Oozie EL function to verify if a test file exists with arguments given
     *
     * @param scriptsPath Path in hdfs with the script files
     * @param version     Project version (can be empty or null if there is no version)
     * @param fileName    File name owner of the test
     * @param execution   'pre' or 'post' to indicate if it is executed before or after the ETL
     * @param counter     Id of the test
     * @return boolean    If the test file exists or not
     */
    public static boolean existsTestFile(String scriptsPath, String version, String fileName, String execution, int counter) throws Exception {
        String testFilePath = GbicELFunctions.getTestFilePath(scriptsPath, version, fileName, execution, counter);
        
        URI uri = new URI(testFilePath);
        String path = uri.getPath();
        FileSystem fs = getFileSystem(uri);
        Path p = new Path(path);
        
        return fs.exists(p);
    }
    
    /**
     * Oozie EL function to get the ScreenFile path with arguments given
     *
     * @param scriptsPath Path in hdfs with the script files
     * @param version     Project version (can be empty or null if there is no version)
     * @param fileName    File name owner of the screen
     * @param execution   'pre' or 'post' to indicate if it is executed before or after the ETL
     * @param screenType  'l' or 'g'
     * @param counter     Id of the screen
     * @return String     Path of the screen file
     */
    public static String getScreenFilePath(String scriptsPath, String version, String fileName, String execution, String screenType, int counter) throws Exception {
        if(version == null || version.isEmpty()){
            return MessageFormat.format(GbicELFunctions.SCREENS_PATH_WITHOUT_VERSION,
                                        scriptsPath,
                                        fileName,
                                        execution,
                                        screenType,
                                        counter);
        } else {
            return MessageFormat.format(GbicELFunctions.SCREENS_PATH,
                                        scriptsPath,
                                        version,
                                        fileName,
                                        execution,
                                        screenType,
                                        counter);
        }
    }
    
    /**
     * Oozie EL function to verify if a screen file exists with arguments given
     *
     * @param scriptsPath Path in hdfs with the script files
     * @param version     Project version (can be empty or null if there is no version)
     * @param fileName    File name owner of the screen
     * @param execution   'pre' or 'post' to indicate if it is executed before or after the ETL
     * @param screenType  'l' or 'g'
     * @param counter     Id of the screen
     * @return boolean    If the screen file exists or not
     */
    public static boolean existsScreenFile(String scriptsPath, String version, String fileName, String execution, String screenType, int counter) throws Exception {
        String screenFilePath = GbicELFunctions.getScreenFilePath(scriptsPath, version, fileName, execution, screenType, counter);
        
        URI uri = new URI(screenFilePath);
        String path = uri.getPath();
        FileSystem fs = getFileSystem(uri);
        Path p = new Path(path);
        
        return fs.exists(p);
    }
    
    /**
     * Oozie EL function to get the data files path with arguments given
     *
     * @param hdfsInbox      Inbox folder in hdfs (with data files)
     * @param ob3            Country ISO three letters code 
     * @param version        Project version (can be empty or null if there is no version)
     * @param fileName       Data file name
     * @param partitionField Name of the date partition field (month, day, etc)
     * @param nominalTime    Month partition of the data (in format yyyy-mm-dd)
     * @return String        Path of the screen file
     */
    public static String getDataDirPath(String hdfsInbox, String ob3, String version, String fileName, String partitionField, String nominalTime) throws Exception {
        if(version == null || version.isEmpty()){
            return MessageFormat.format(GbicELFunctions.DATA_DIR_PATH_WITHOUT_VERSION,
                                        hdfsInbox,
                                        ob3,
                                        fileName,
                                        partitionField,
                                        nominalTime);
        } else {
            return MessageFormat.format(GbicELFunctions.DATA_DIR_PATH,
                                        hdfsInbox,
                                        ob3,
                                        version,
                                        fileName,
                                        partitionField,
                                        nominalTime);
        }
    }
    
    /**
     * Oozie EL function to verify if a pig file exists with arguments given
     *
     * @param scriptsPath Path in hdfs with the script files
     * @param version     Project version (can be empty or null if there is no version)
     * @param fileName    File name owner of the pig
     * @return boolean    If the pig file exists or not
     */
    public static boolean existsPigFile(String scriptsPath, String version, String fileName) throws Exception {
        String pigFilePath = GbicELFunctions.getPigFilePath(scriptsPath, version, fileName);
        
        URI uri = new URI(pigFilePath);
        String path = uri.getPath();
        FileSystem fs = getFileSystem(uri);
        Path p = new Path(path);
        
        return fs.exists(p);
    }
    
    /**
     * Oozie EL function to get a pig path with arguments given
     *
     * @param scriptsPath Path in hdfs with the script files
     * @param version     Project version (can be empty or null if there is no version)
     * @param fileName    File name owner of the pig
     * @return String     Path of the data file
     */
    public static String getPigFilePath(String scriptsPath, String version, String fileName) throws Exception {
        if(version == null || version.isEmpty()){
            return MessageFormat.format(GbicELFunctions.PIG_PATH_WITHOUT_VERSION,
                                        scriptsPath,
                                        fileName);
        } else {
            return MessageFormat.format(GbicELFunctions.PIG_PATH,
                                        scriptsPath,
                                        version,
                                        fileName);
        }
    }
    
}
