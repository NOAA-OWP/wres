package wres.io.reading.wrds;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties( ignoreUnknown = true )
public class Member
{
    String identifier;
    List<List<DataPoint>> dataPointsList;

    public List<List<DataPoint>> getDataPointsList()
    {
        return dataPointsList;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public void setDataPointsList( List<List<DataPoint>> dataPointsList )
    {
        this.dataPointsList = dataPointsList;
    }

    public void setIdentifier( String identifier )
    {
        this.identifier = identifier;
    }

}
