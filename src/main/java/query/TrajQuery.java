package query;

import model.avro.TrajSegmentAvro;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TrajQuery {

    public static void main(String[] args)
    {
        Configuration config= HBaseConfiguration.create();
        try {
            Connection conn = ConnectionFactory.createConnection(config);
            Table table=conn.getTable(TableName.valueOf("segmentTable"));
            Scan scan=new Scan();
            Filter filter=new ValueFilter(CompareOperator.EQUAL
                    ,new SubstringComparator("20150501120000wt3mf"));
            scan.setFilter(filter);
            ResultScanner rs=table.getScanner(scan);
            for(Result r:rs)
            {
                TrajSegmentAvro segment=Utils.trajSegmentAvroDeserialize(r.getValue(Bytes.toBytes("segmentData")
                        ,Bytes.toBytes("100")));
                System.out.println(segment);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }


    }
}
