package query;

import model.avro.TrajSegmentAvro;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;

public class Utils {
    public static TrajSegmentAvro trajSegmentAvroDeserialize(byte[] serializedBytes)
    {
        DatumReader<TrajSegmentAvro> reader=new SpecificDatumReader<TrajSegmentAvro>(TrajSegmentAvro.getClassSchema());
        BinaryDecoder decoder= DecoderFactory.get().binaryDecoder(serializedBytes,null);
        try {
            return reader.read(null, decoder);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }
}
