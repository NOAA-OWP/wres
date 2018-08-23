package wres.io.reading.wrds;

import java.time.Instant;
import java.time.OffsetDateTime;

import wres.util.Strings;

public class Forecast
{
    public Location getLocation()
    {
        return location;
    }

    public void setBasisTime( String basisTime )
    {
        if ( Strings.hasValue(basisTime))
        {
            this.basisTime = OffsetDateTime.parse( basisTime );
        }
    }

    public void setDistributor( String distributor )
    {
        this.distributor = distributor;
    }

    public void setIssuedTime( String issuedTime )
    {
        if(Strings.hasValue( issuedTime ))
        {
            this.issuedTime = OffsetDateTime.parse(issuedTime);
        }
    }

    public void setIssuer( String issuer )
    {
        this.issuer = issuer;
    }

    public void setLocation( Location location )
    {
        this.location = location;
    }

    public void setProducer( String producer )
    {
        this.producer = producer;
    }

    public void setType( String type )
    {
        this.type = type;
    }

    public void setUnits( ForecastUnits units )
    {
        this.units = units;
    }

    public ForecastUnits getUnits()
    {
        return units;
    }

    public OffsetDateTime getBasisTime()
    {
        return basisTime;
    }

    public String getDistributor()
    {
        return distributor;
    }

    public OffsetDateTime getIssuedTime()
    {
        return issuedTime;
    }

    public String getIssuer()
    {
        return issuer;
    }

    public String getProducer()
    {
        return producer;
    }

    public String getType()
    {
        return type;
    }

    public ParameterCodes getParameterCodes()
    {
        return parameterCodes;
    }

    public void setParameterCodes( ParameterCodes parameterCodes )
    {
        this.parameterCodes = parameterCodes;
    }

    public void setDataPointsList( DataPoint[] dataPointsList )
    {
        this.dataPointsList = dataPointsList;
    }

    public DataPoint[] getDataPointsList()
    {
        return dataPointsList;
    }

    public Member[] getMembers()
    {
        return members;
    }

    public void setMembers( Member[] members )
    {
        this.members = members;
    }

    Location location;
    String producer;
    String issuer;
    String distributor;
    String type;
    OffsetDateTime basisTime;
    OffsetDateTime issuedTime;
    ForecastUnits units;
    ParameterCodes parameterCodes;
    Member[] members;
    DataPoint[] dataPointsList;
}
