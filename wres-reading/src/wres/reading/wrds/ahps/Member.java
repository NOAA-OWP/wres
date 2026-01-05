package wres.reading.wrds.ahps;

import java.util.List;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * A time-series member.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@Getter
@Setter
public class Member
{
    private String identifier;
    private String units;
    private List<List<DataPoint>> dataPointsList;

    /**
     * Sets the data points.
     * @param dataPoints the data points
     */
    public void setDataPoints( List<DataPoint> dataPoints )
    {
        dataPointsList = new ArrayList<>();
        dataPointsList.add( dataPoints );
    }
}
