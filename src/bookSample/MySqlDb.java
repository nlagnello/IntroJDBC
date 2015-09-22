/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bookSample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 *
 * @author Nick
 */
public class MySqlDb{
    
    private Connection conn;
    
    
    public void openConnection(String driverClass, String url, String userName, String password) throws Exception{
        Class.forName (driverClass);
	conn = DriverManager.getConnection(url, userName, password);
    }
    public void closeConnection() throws SQLException{
        conn.close();
    }
    
    public List<Map<String,Object>> findAllRecords(String tableName) throws SQLException {
        
        List<Map<String,Object>> records = new ArrayList<>();
        
        String sql = "SELECT * FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while(rs.next()){
            Map<String, Object> record = new HashMap<>();
            for(int i=1; i<= columnCount; i++){
                record.put(metaData.getColumnName(i), rs.getObject(i));
            }
            records.add(record); 
        }
        
        return records;
    }
    
    public void deleteByPrimaryKey(String tableName, String primaryKey, Object primaryKeyValue) throws SQLException {
        
        
        Statement stmt = conn.createStatement();
        String sql = "DELETE FROM " + tableName + " WHERE " + primaryKey + " = ";
        if(primaryKeyValue instanceof String){
            sql += "'" + primaryKeyValue.toString() + "'";
        }else{
            sql += primaryKeyValue.toString();
        }
        stmt.executeUpdate(sql);
        
        
    }
    
    public void deleteByPrimaryKeyPrepareStatement(String tableName, String primaryKey, Object primaryKeyValue) throws SQLException {
        

        String sql = "DELETE FROM " + tableName + " WHERE " + primaryKey + " = ?";
        
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setObject(1, primaryKeyValue);
        stmt.executeUpdate();
        
        
    }
    
    public void createRecordPrepareStatement(String tableName, List newRecordCols, List newRecordValues) throws SQLException {
        
        Object[] recordColsArray = newRecordCols.toArray();
        Object[] recordValueArray = newRecordValues.toArray();
        String recordColsString = recordColsArray[0].toString();
        String recordValueString = "?";
        for(int i = 1; i < recordColsArray.length; i++){
            recordColsString += ", " + recordColsArray[i].toString(); 
            recordValueString += ", ?"; 
                    
        }
        
        
        String sql = "INSERT INTO " + tableName + " ( " + recordColsString + " ) VALUES (" + recordValueString + ")";
        PreparedStatement stmt = conn.prepareStatement(sql);
        
        for(int i = 0; i < recordColsArray.length; i++){
            stmt.setObject(i+1,recordValueArray[i]);
        }
        stmt.executeUpdate();
    }
    public void createRecord(String tableName, List newRecordCols, List newRecordValues) throws SQLException {
        
        Object[] recordColsArray = newRecordCols.toArray();
        Object[] recordValueArray = newRecordValues.toArray();
        String recordColsString = recordColsArray[0].toString();
        String recordValueString = recordValueArray[0].toString();
        for(int i = 1; i < recordValueArray.length; i++){
            recordColsString += ", '" + recordColsArray[i].toString() + "'"; 
            recordValueString += ", '" + recordValueArray[i].toString() + "'"; 
                    
        }
        
        
        String sql = "INSERT INTO " + tableName + " ( " + recordColsString + " ) VALUES (" + recordValueString + ")";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
    }
    
    public void updateByPrimaryKey(String tableName, String primaryKey, Object primaryKeyValue, List updateRecordCols, List updateRecordValues) throws SQLException {

        Object[] recordKeyArray = updateRecordCols.toArray();
        Object[] recordObjectArray = updateRecordValues.toArray();
        String recordString = recordKeyArray[0].toString() + "= '" + recordObjectArray[0].toString() + "'";
        for(int i = 1; i < recordKeyArray.length; i++){
            recordString += "," + recordKeyArray[i].toString() + "= '" + recordObjectArray[i].toString() + "'";
        }
        
        String sql = "UPDATE " + tableName + " SET "  + recordString + " WHERE " + primaryKey + " = ";
        if(primaryKeyValue instanceof String){
            sql += "'" + primaryKeyValue.toString() + "'";
        }else{
            sql += primaryKeyValue.toString();
        }
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
    }
    public void updateByPrimaryKeyPrepareStatement(String tableName, String primaryKey, Object primaryKeyValue, List updateRecordCols, List updateRecordValues) throws SQLException {

        Object[] recordKeyArray = updateRecordCols.toArray();
        Object[] recordObjectArray = updateRecordValues.toArray();
        String recordString = recordKeyArray[0].toString() + "= ?";
        for(int i = 1; i < recordKeyArray.length; i++){
            recordString += "," + recordKeyArray[i].toString() + "= ?";
        }
        
        String sql = "UPDATE " + tableName + " SET "  + recordString + " WHERE " + primaryKey + " = ?";
       
        
        PreparedStatement stmt = conn.prepareStatement(sql);
        for(int i = 0; i < recordKeyArray.length; i++){
            stmt.setObject(i+1, recordObjectArray[i]);
        }
        stmt.setObject(recordKeyArray.length+1, primaryKeyValue);
        stmt.executeUpdate();
        
    }
    
    public static void main(String[] args) throws Exception {
        MySqlDb db = new MySqlDb();
        db.openConnection("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/book", "root", "admin");
        System.out.println("R - Read: \n");
        List<Map<String, Object>> records = db.findAllRecords("author");
        
        for(Map record:records){
            System.out.println(record);
        }
        
        System.out.println("D - Delete:");
        
//        db.deleteByPrimaryKeyPrepareStatement("author", "author_id", "3");
//        
//        records = db.findAllRecords("author");
//        for(Map record:records){
//            System.out.println(record);
//        }
        System.out.println("Delete = Working");
        System.out.println("C - Create:");
        
/*       
        List newRecordCols = new ArrayList();
        newRecordCols.add("author_name");
        newRecordCols.add("date_created");
        List newRecordValues = new ArrayList();      
        newRecordValues.add("Pekka Rinne");
        newRecordValues.add("1989-04-02");
        db.createRecordPrepareStatement("author", newRecordCols, newRecordValues);
        
        records = db.findAllRecords("author");
        for(Map record:records){
            System.out.println(record);
        }
        */
        
        System.out.println("Create = Working");
        System.out.println("U - Update:");
        
        List updateRecordCols = new ArrayList();
        updateRecordCols.add("author_name");
        updateRecordCols.add("date_created");
        List updateRecordValues = new ArrayList();      
        updateRecordValues.add("Bill Murray");
        updateRecordValues.add("1959-07-29");
        db.updateByPrimaryKeyPrepareStatement("author", "author_id", "4", updateRecordCols, updateRecordValues);
        
        records = db.findAllRecords("author");
        for(Map record:records){
            System.out.println(record);
        }
        
        System.out.println("Update = Working");
        
        db.closeConnection();
    }
    
    
}
