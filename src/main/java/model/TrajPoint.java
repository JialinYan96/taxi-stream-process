package model;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

public class TrajPoint {
    private String taxiId;
    private String time;
    private double lon;
    private double lat;
    private double direction;
    private double speed;
    private int passenger;
    private long utc;




    public TrajPoint(String id, String time, double lon, double lat, double direction, double speed, int passenger, long utc) {
        this.taxiId = id;
        this.time = time;
        this.lon = lon;
        this.lat = lat;
        this.direction = direction;
        this.speed = speed;
        this.passenger = passenger;
        this.utc = utc;
    }

    public TrajPoint()
    {

    }

    public static TrajPoint fromString(String line) {
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] tokens = line.split(",");
        if (tokens.length != 8) {
            throw new RuntimeException("Invalid record" + line);
        }
        TrajPoint trajPoint = new TrajPoint();
        try {
            trajPoint.setTaxiId(tokens[0]);
            trajPoint.setTime(tokens[1]);
            trajPoint.setLon(Double.valueOf(tokens[2]));
            trajPoint.setLat(Double.valueOf(tokens[3]));
            trajPoint.setDirection(Double.valueOf(tokens[4]));
            trajPoint.setSpeed(Double.valueOf(tokens[5]));
            trajPoint.setPassenger(Integer.valueOf(tokens[6]));
            trajPoint.setUtc(dateFormat.parse(tokens[1]).getTime());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return trajPoint;
    }




    public String getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(String id) {
        this.taxiId = id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getDirection() {
        return direction;
    }

    public void setDirection(double direction) {
        this.direction = direction;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getPassenger() {
        return passenger;
    }

    public void setPassenger(int passenger) {
        this.passenger = passenger;
    }

    public long getUtc() {
        return utc;
    }

    public void setUtc(long utc) {
        this.utc = utc;
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(this.taxiId).append(",");
        sb.append(this.time).append(",");
        sb.append(Double.toString(this.lon)).append(",");
        sb.append(Double.toString(this.lat)).append(",");
        sb.append(Double.toString(this.direction)).append(",");
        sb.append(Double.toString(this.speed)).append(",");
        sb.append(Integer.toString(this.passenger)).append(",");
        sb.append(this.utc);
        return sb.toString();
    }

    public long getEventTime()
    {
        return this.utc;
    }


}
