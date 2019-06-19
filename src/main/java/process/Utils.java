package process;

import ch.hsr.geohash.GeoHash;
import model.TrajPoint;
import model.avro.SimplePointAvro;
import model.avro.TrajPointAvro;
import model.avro.TrajSegmentAvro;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    public static String timeStamp2Date(long seconds,String format) {
        if(seconds==0L){
            return "";
        }
        if(format == null || format.isEmpty()){
            format = "yyyyMMdd";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(seconds));
    }

    public static long date2TimeStamp(String date_str,String format){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return sdf.parse(date_str).getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    public static TrajPointAvro string2TrajPointAvro(String line)
    {

        String[] tokens = line.split(",");
        if (tokens.length != 8) {
            throw new RuntimeException("Invalid record" + line);
        }
        TrajPointAvro trajPointAvro = new TrajPointAvro();
        try {

            trajPointAvro.setTaxiId(tokens[0]);
            trajPointAvro.setTime(tokens[1]);
            trajPointAvro.setLon(Double.valueOf(tokens[2]));
            trajPointAvro.setLat(Double.valueOf(tokens[3]));
            trajPointAvro.setDirection(Double.valueOf(tokens[4]));
            trajPointAvro.setSpeed(Double.valueOf(tokens[5]));
            trajPointAvro.setPassenger(Integer.valueOf(tokens[6]));
            trajPointAvro.setUtc(date2TimeStamp(tokens[1],"yyyy-MM-dd HH:mm:ss"));
            trajPointAvro.setCellID(GeoHash.withCharacterPrecision(trajPointAvro.getLat()
                    ,trajPointAvro.getLon(),4).toBase32());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return trajPointAvro;

    }

    public static byte[]serialize(TrajSegmentAvro record)
    {
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        BinaryEncoder encoder=null;
        encoder=EncoderFactory.get().binaryEncoder(out,encoder);
        DatumWriter<TrajSegmentAvro>writer=new SpecificDatumWriter<>(record.getSchema());
        try{
            writer.write(record,encoder);
            encoder.flush();
            out.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    public static byte[]serializeSimplePoint(SimplePointAvro record)
    {
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        BinaryEncoder encoder=null;
        encoder=EncoderFactory.get().binaryEncoder(out,encoder);
        DatumWriter<SimplePointAvro>writer=new SpecificDatumWriter<>(record.getSchema());
        try{
            writer.write(record,encoder);
            encoder.flush();
            out.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    public static byte[]serializeTrajPoint(TrajPointAvro record)
    {
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        BinaryEncoder encoder=null;
        encoder=EncoderFactory.get().binaryEncoder(out,encoder);
        DatumWriter<TrajPointAvro>writer=new SpecificDatumWriter<>(record.getSchema());
        try{
            writer.write(record,encoder);
            encoder.flush();
            out.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return out.toByteArray();
    }
}
