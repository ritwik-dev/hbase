package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello world!");
        Configuration  conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        try (Admin admin = conn.getAdmin();) {
            TableName tableName = TableName.valueOf("test");
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of("data");
            TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cfd).build();
            admin.createTable(td);
            TableDescriptor[] tables = (TableDescriptor[]) admin.listTableDescriptors().toArray();
            if(tables.length != 1 && Bytes.equals(tableName.getName(),tables[0].getTableName().getName()))
                throw new IOException("Failed create of table");

            try(Table table = conn.getTable(tableName);) {
                for( int i = 1; i<=3;i++){
                    byte[] row = Bytes.toBytes("row" + i);
                    Put put = new Put(row);
                    byte[] columnFamily = Bytes.toBytes("data");
                    byte[] qualifier = Bytes.toBytes(String.valueOf(i));
                    byte[] value = Bytes.toBytes("value" + i);
                    put.addColumn(columnFamily,qualifier,value);
                    table.put(put);
                }
                Get get = new Get(Bytes.toBytes("row1"));
                Result result = table.get(get);
                System.out.println("Get : " + result);
                Scan scan = new Scan();

                try(ResultScanner scanner = table.getScanner(scan);) {
                    for (Result scannerResult : scanner){
                        System.out.println("Scan : "+ scannerResult);
                    }
                }
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        }
    }
}