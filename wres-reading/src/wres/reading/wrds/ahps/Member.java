package wres.reading.wrds.ahps;

import java.util.List;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A time-series member.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class Member
{
    private String identifier;
    private String units;
    private List<List<DataPoint>> dataPointsList;

    /**
     * @return the data points
     */
    public List<List<DataPoint>> getDataPointsList()
    {
        return dataPointsList;
    }

    /**
     * @return the identifier
     */
    public String getIdentifier()
    {
        return identifier;
    }

    /**
     * @return the units
     */
    public String getUnits()
    {
        return units;
    }

    /**
     * Sets the data points/
     * @param dataPointsList the data points
     */
    public void setDataPointsList( List<List<DataPoint>> dataPointsList )
    {
        this.dataPointsList = dataPointsList;
    }

    /**
     * Sets the data points.
     * @param dataPoints the data points
     */
    public void setDataPoints( List<DataPoint> dataPoints )
    {
        dataPointsList = new ArrayList<>();
        dataPointsList.add( dataPoints );
    }

    /**
     * Sets the identifier.
     * @param identifier the identifier
     */
    public void setIdentifier( String identifier )
    {
        this.identifier = identifier;
    }

    /**
     * Sets the units.
     * @param units the units
     */
    public void setUnits( String units )
    {
        this.units = units;
    }

}
