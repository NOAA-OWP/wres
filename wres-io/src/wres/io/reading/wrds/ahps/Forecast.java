package wres.io.reading.wrds.ahps;

import java.time.OffsetDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;

/**
 * A forecast.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class Forecast
{
    /**
     * @return the location
     */
    public Location getLocation()
    {
        return location;
    }

    /**
     * Sets the basis time.
     * @param basisTime the basis time
     */
    public void setBasisTime( String basisTime )
    {
        if ( Objects.nonNull( basisTime ) && ! basisTime.isBlank() )
        {
            this.basisTime = OffsetDateTime.parse( basisTime );
        }
    }

    /**
     * Sets the distributor.
     * @param distributor the distributor
     */
    public void setDistributor( String distributor )
    {
        this.distributor = distributor;
    }

    /**
     * Sets the issued time.
     * @param issuedTime the issued time
     */
    public void setIssuedTime( String issuedTime )
    {
        if ( Objects.nonNull( issuedTime ) && !issuedTime.isBlank() )
        {
            this.issuedTime = OffsetDateTime.parse( issuedTime );
        }
    }

    /**
     * Sets the issuer.
     * @param issuer the issuer
     */
    public void setIssuer( String issuer )
    {
        this.issuer = issuer;
    }

    /**
     * Sets the location.
     * @param location the location.
     */

    public void setLocation( Location location )
    {
        this.location = location;
    }

    /**
     * Sets the producer.
     *
     * @param producer the producer.
     */
    public void setProducer( String producer )
    {
        this.producer = producer;
    }

    /**
     * Sets the type.
     * @param type the type
     */
    public void setType( String type )
    {
        this.type = type;
    }

    /**
     * Sets the units.
     * @param units the units
     */
    public void setUnits( ForecastUnits units )
    {
        this.units = units;
    }

    /**
     * @return the units
     */
    public ForecastUnits getUnits()
    {
        return units;
    }

    /**
     * @return the basis time
     */
    public OffsetDateTime getBasisTime()
    {
        return basisTime;
    }

    /**
     * @return the distributor
     */
    public String getDistributor()
    {
        return distributor;
    }

    /**
     * @return the issued time
     */
    public OffsetDateTime getIssuedTime()
    {
        return issuedTime;
    }

    /**
     * @return the issuer
     */
    public String getIssuer()
    {
        return issuer;
    }

    /**
     * @return the producer
     */
    public String getProducer()
    {
        return producer;
    }

    /**
     * @return the type
     */
    public String getType()
    {
        return type;
    }

    /**
     * @return the parameter codes
     */
    public ParameterCodes getParameterCodes()
    {
        return parameterCodes;
    }

    /**
     * Sets the parameter codes.
     * @param parameterCodes the parameter codes
     */

    public void setParameterCodes( ParameterCodes parameterCodes )
    {
        this.parameterCodes = parameterCodes;
    }

    /**
     * @return the members
     */

    public Member[] getMembers()
    {
        return members;
    }

    /**
     * Sets the members.
     * @param members the members
     */
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

    @JsonAlias( { "timeseries" } )
    Member[] members;

    @Override
    public String toString()
    {
        String locationName = "Unknown Location";

        if ( this.getLocation() != null )
        {
            locationName = this.getLocation().toString();
        }

        String releaseDate = "released at unknown time.";

        if ( this.getBasisTime() != null )
        {
            releaseDate = "with a basis time of " + this.getBasisTime().toString();
        }
        else if ( this.getIssuedTime() != null )
        {
            releaseDate = "issued at " + this.getIssuedTime().toString();
        }
        return "Forecast for " + locationName + ", " + releaseDate;
    }
}
