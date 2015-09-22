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
    
    public void createOneRecord(String tableName, List newRecordCols, List newRecordValues) throws SQLException {
        
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
    
    public void updateByPrimaryKey(String tableName, String primaryKey, Object primaryKeyValue, Map<String,Object> updateRecord) throws SQLException {

        Map<String, Object> record = updateRecord;
        Object[] recordKeyArray = record.keySet().toArray();
        Object[] recordObjectArray = record.values().toArray();
        String recordString = recordKeyArray[0].toString() + "= '" + recordObjectArray[0].toString() + "'";
        for(int i = 1; i < recordKeyArray.length-1; i++){
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
        newRecordValues.add("Jared Kowalske");
        newRecordValues.add("1991-08-27");
        db.createOneRecord("author", newRecordCols, newRecordValues);
        
        records = db.findAllRecords("author");
        for(Map record:records){
            System.out.println(record);
        }
        
        */
        System.out.println("Create = Working");
        System.out.println("U - Update:");
//        Map<String,Object> updateRecord = new HashMap();
//        updateRecord.put("author_name", "Bill Smith");
//        db.updateByPrimaryKey("author", "author_id", "3",updateRecord);
//        
//        records = db.findAllRecords("author");
//        for(Map record:records){
//            System.out.println(record);
//        }
        
        System.out.println("Update = Working");
        
        db.closeConnection();
    }
    
    
}
