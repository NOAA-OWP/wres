package wres.io.reading.wrds;

import java.util.List;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties( ignoreUnknown = true )
public class Member
{
    String identifier;
    String units;

    List<List<DataPoint>> dataPointsList;

    public List<List<DataPoint>> getDataPointsList()
    {
        return dataPointsList;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public String getUnits()
    {
        return units;
    }

    //For the AHPS API, which includes a list of lists of data points.
    public void setDataPointsList( List<List<DataPoint>> dataPointsList )
    {
        this.dataPointsList = dataPointsList;
    }

    //For the observation API, which seres a list of data points.
    public void setDataPoints( List<DataPoint> dataPoints )
    {
        dataPointsList = new ArrayList<>();
        dataPointsList.add(dataPoints);
    }

    public void setIdentifier( String identifier )
    {
        this.identifier = identifier;
    }

    public void setUnits( String units)
    {
        this.units = units;
    }

}
