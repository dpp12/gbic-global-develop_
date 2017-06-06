package com.telefonica.gbic.global;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import com.mysql.jdbc.Statement;

public class DataQualityTool {
	
	// JDBC driver name and database URL
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; //$NON-NLS-1$
	
	// Oozie action output properties
	private static final String OOZIE_PROPERTIES = "oozie.action.output.properties"; //$NON-NLS-1$
	
	// Constants
	private static final String ID_FILEENTITY = GbicGPlatformConfig.getValue("data_quality.file_entity.id_fileentity"); //$NON-NLS-1$
	private static final String ID_FILEINSTANCE = GbicGPlatformConfig.getValue("data_quality.file_instance.id_fileinstance"); //$NON-NLS-1$
	private static final String ID_FILEREVISION = GbicGPlatformConfig.getValue("data_quality.file_revision.id_filerevision"); //$NON-NLS-1$
	
	// Errors
	private static final String OOZIE_PROP_ERROR = GbicGPlatformConfig.getValue("data_quality.error.oozie_properties"); //$NON-NLS-1$
	
	private Connection conn = null;
	private PreparedStatement stmt;
	
	private String msg;
	
	public DataQualityTool(String database, String user, String password) {
		msg = "New DQT";
		try {
			try {
				Class.forName(DataQualityTool.JDBC_DRIVER);
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
	
	private void closeConnection() {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}
	
	private int getFileEntity(String project, String file) {
		int entityId = -1;
		ResultSet rs;
		msg = "\nGetting file_entity.......";
		String query;
		query = " SELECT id_fileentity "         + //$NON-NLS-1$
		        " FROM file_entity "             + //$NON-NLS-1$
		        " WHERE file_name = ? "          + //$NON-NLS-1$
		        "   AND id_project = ( "         + //$NON-NLS-1$
		        "       SELECT id_project "      + //$NON-NLS-1$
		        "       FROM project "           + //$NON-NLS-1$
		        "       WHERE project_name = ? " + //$NON-NLS-1$
		        "   ) ";                           //$NON-NLS-1$
		
		try {
			stmt = conn.prepareStatement(query);
			stmt.setString(1, file);
			stmt.setString(2, project);
			rs = stmt.executeQuery();
			
			while (rs.next()) {
				entityId = rs.getInt(1);
				msg = "\nGot file_entity: "+entityId;
			}
			stmt.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			msg += "\n" + e; //$NON-NLS-1$
			throw e;
		}
		return entityId;
	}
	
	private int getFileInstance(String project, String file, Date date, int country) {
		int instanceId = -1;
		
		ResultSet rs;
		String query;
		query = " SELECT id_fileinstance "           + //$NON-NLS-1$
		        " FROM file_instance "               + //$NON-NLS-1$
		        " WHERE id_fileentity = ( "          + //$NON-NLS-1$
		        "     SELECT id_fileentity "         + //$NON-NLS-1$
		        "     FROM file_entity "             + //$NON-NLS-1$
		        "     WHERE file_name = ? "          + //$NON-NLS-1$
		        "       AND id_project = ( "         + //$NON-NLS-1$
		        "           SELECT id_project "      + //$NON-NLS-1$
		        "           FROM project "           + //$NON-NLS-1$
		        "           WHERE project_name = ? " + //$NON-NLS-1$
		        "       )"                           + //$NON-NLS-1$
		        " ) "                                + //$NON-NLS-1$
		        " AND content_dt = ? "               + //$NON-NLS-1$
		        " AND gbic_op_id = ( "               + //$NON-NLS-1$
		        "     SELECT gbic_op_id "            + //$NON-NLS-1$
		        "     FROM country "                 + //$NON-NLS-1$
		        "     WHERE gbic_op_id = ? "         + //$NON-NLS-1$
		        " ) ";                                 //$NON-NLS-1$
		
		try {
			msg = "\nGetting file_instance.........";
			
			stmt = conn.prepareStatement(query);
			stmt.setString(1, file);
			stmt.setString(2, project);
			stmt.setDate(3, date);
			stmt.setInt(4, country);
			rs = stmt.executeQuery();
			
			while (rs.next()) {
				instanceId = rs.getInt(1);
				msg = "\nGot file_instance: "+instanceId;
			}
			stmt.close();
			
			if (instanceId == -1) {
				instanceId = this.registerNewFileInstance(project, file, date, country);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return instanceId;
	}
	
	private int registerNewFileInstance(String project, String file, Date date, int country) {
		int instanceId = -1;
		ResultSet rs;
		String query;
		query = " INSERT INTO file_instance "               + //$NON-NLS-1$
		        " (id_fileentity,content_dt,gbic_op_id) "   + //$NON-NLS-1$
		        " SELECT f.id_fileentity, ?, c.gbic_op_id " + //$NON-NLS-1$
		        " FROM ( "                                  + //$NON-NLS-1$
		        "     SELECT id_fileentity "                + //$NON-NLS-1$
		        "     FROM file_entity "                    + //$NON-NLS-1$
		        "     WHERE file_name = ? "                 + //$NON-NLS-1$
		        "       AND id_project = ( "                + //$NON-NLS-1$
		        "         SELECT id_project "               + //$NON-NLS-1$
		        "         FROM project "                    + //$NON-NLS-1$
		        "         WHERE project_name = ? "          + //$NON-NLS-1$
		        "     ) "                                   + //$NON-NLS-1$
		        " ) f "                                     + //$NON-NLS-1$
		        " JOIN ( "                                  + //$NON-NLS-1$
		        "     SELECT gbic_op_id "                   + //$NON-NLS-1$
		        "     FROM country "                        + //$NON-NLS-1$
		        "     WHERE gbic_op_id = ? "                + //$NON-NLS-1$
		        " ) c";                                       //$NON-NLS-1$
		
		try {
			msg = "\nCould not get file_instance. Inserting new one..............";
			
			stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
			stmt.setDate(1, date);
			stmt.setString(2, file);
			stmt.setString(3, project);
			stmt.setInt(4, country);
			stmt.execute();
			
			rs = stmt.getGeneratedKeys();
			while (rs.next()) {
				instanceId = rs.getInt(1);
				msg = "\nInserted file_instance: "+instanceId;
			}
			stmt.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return instanceId;
	}
	
	private int getFileRevision(int instance, long fileSize, String fileChecksum, boolean overwrite) {
		int id_filerevision = 0;
		String db_checksum = ""; //$NON-NLS-1$
		int file_revision_num = 0;
		ResultSet rs;
		String query;
		query = " SELECT id_filerevision, file_checksum, file_revision_num " + //$NON-NLS-1$
		        " FROM file_revision "                                       + //$NON-NLS-1$
		        " WHERE id_fileinstance = ? "                                + //$NON-NLS-1$
		        "   AND file_revision_num = ( "                              + //$NON-NLS-1$
		        "     SELECT max(file_revision_num) "                        + //$NON-NLS-1$
		        "     FROM file_revision "                                   + //$NON-NLS-1$
		        "     WHERE id_fileinstance = ? "                            + //$NON-NLS-1$
		        " ) ";                                                         //$NON-NLS-1$
		
		try {
			msg = "\nGetting file_revision..........................";
			
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, instance);
			stmt.setInt(2, instance);
			rs = stmt.executeQuery();
			
			while (rs.next()) {
				id_filerevision = rs.getInt(1);
				db_checksum = rs.getString(2);
				file_revision_num = rs.getInt(3);
				msg = "\nGot file_revision: "+id_filerevision+" // Checksum: "+db_checksum+" // Revision: "+file_revision_num;
			}
			stmt.close();
			
			if (fileChecksum == null) {
				fileChecksum = ""; //$NON-NLS-1$
			}
			
			if (id_filerevision == 0 || !fileChecksum.equals(db_checksum)){
				file_revision_num++;
				id_filerevision = this.registerNewFileRevision(instance, fileSize, fileChecksum, file_revision_num);
			} else {
				this.updateProcessDateRevision(id_filerevision);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return id_filerevision;
	}
	
	private void updateProcessDateRevision(int id_filerevision) {
		String query;
		query = " UPDATE file_revision "      + //$NON-NLS-1$
		        " SET file_process_date = ? " + //$NON-NLS-1$
		        " WHERE id_filerevision = ? ";  //$NON-NLS-1$
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); //$NON-NLS-1$
		
		Date processDate = Date.valueOf(sdf.format(new java.util.Date()));
		try {
			msg = "\nGot previous revision. Updating process date..............";
			
			stmt = conn.prepareStatement(query);
			stmt.setDate(1, processDate);
			stmt.setInt(2, id_filerevision);
			stmt.executeUpdate();
			
			stmt.close();
			
			msg = "\nUpdated date in file_revision: "+id_filerevision;
			
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private int registerNewFileRevision(int instance, long fileSize, String fileChecksum, int file_revision_num) {
		int revisionId = -1;
		ResultSet rs;
		String query;
		query = " INSERT INTO file_revision "                                                         + //$NON-NLS-1$
		        " (id_fileinstance, file_revision_num, file_process_date, file_size, file_checksum) " + //$NON-NLS-1$
		        " VALUES (?,?,?,?,?)";                                                                 //$NON-NLS-1$
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); //$NON-NLS-1$
		
		Date processDate = Date.valueOf(sdf.format(new java.util.Date()));
		try {
			msg = "\nCould not get file_instance. Inserting new one..............";
			
			stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
			stmt.setInt(1, instance);
			stmt.setInt(2, file_revision_num);
			stmt.setDate(3, processDate);
			stmt.setLong(4, fileSize);
			stmt.setString(5, fileChecksum);
			stmt.execute();
			
			rs = stmt.getGeneratedKeys();
			while (rs.next()) {
				revisionId = rs.getInt(1);
				msg = "\nInserted file_revision: "+revisionId;
			}
			stmt.close();
			
			msg = "\nInserted file_revision: "+revisionId;
			
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return revisionId;
	}
	
	public static void main(String[] args) {
		DataQualityTool dqt = null;
		try {
			// Arguments reading
			String project = args[0];
			String file = args[1];
			Date date = Date.valueOf(args[2]);
			int gbic_op_id = Integer.valueOf(args[3]);
			long fileSize = Long.valueOf(args[4]);
			String fileChecksum = args[5];
			String database = args[6];
			String user = args[7];
			String password = args[8];
			
			dqt = new DataQualityTool(database, user, password);
			boolean overwriteRevision = false;
			
			dqt.msg += "\ncall getFileEntity";
			int fileEntity = dqt.getFileEntity(project, file);
			int fileInstance = dqt.getFileInstance(project, file, date, gbic_op_id);
			int fileRevision = dqt.getFileRevision(fileInstance, fileSize, fileChecksum, overwriteRevision);
			
			String oozieProp = System.getProperty(DataQualityTool.OOZIE_PROPERTIES);
			OutputStream os = null;
			if (oozieProp != null) {
				File propFile = new File(oozieProp);
				Properties p = new Properties();
				p.setProperty(DataQualityTool.ID_FILEENTITY, Integer.toString(fileEntity));
				p.setProperty(DataQualityTool.ID_FILEINSTANCE, Integer.toString(fileInstance));
				p.setProperty(DataQualityTool.ID_FILEREVISION, Integer.toString(fileRevision));
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
				throw new RuntimeException(DataQualityTool.OOZIE_PROP_ERROR);
			}
			dqt.closeConnection();
			
		} finally {
			System.err.println("DEBUG MESSAGES: " + dqt.msg); //$NON-NLS-1$
		}
	}
}
