package wres.io.reading.wrds;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;

import wres.util.Strings;

@JsonIgnoreProperties( ignoreUnknown = true )
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

    @JsonAlias({"timeseries"})
    Member[] members;


    @Override
    public String toString()
    {
        String locationName = "Unknown Location";

        if (this.getLocation() != null)
        {
            locationName = this.getLocation().toString();
        }

        String releaseDate = "released at unknown time.";

        if (this.getBasisTime() != null)
        {
            releaseDate = "with a basis time of " + this.getBasisTime().toString();
        }
        else if (this.getIssuedTime() != null)
        {
            releaseDate = "issued at " + this.getIssuedTime().toString();
        }
        return "Forecast for " + locationName + ", " + releaseDate;
    }
}
