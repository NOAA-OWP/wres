package wres.reading.wrds.ahps;

import java.time.OffsetDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A forecast.
 */

@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
public class Forecast
{
    @Setter
    Location location;

    @Setter
    String issuer;

    @Setter
    String distributor;

    @Setter
    String type;

    @Setter
    ForecastUnits units;

    @Setter
    ParameterCodes parameterCodes;

    @Setter
    String producer;

    @Setter
    @JsonAlias( { "timeseries" } )
    Member[] members;

    OffsetDateTime basisTime;

    OffsetDateTime issuedTime;

    OffsetDateTime generationTime;

    /**
     * Sets the basis time.
     * @param basisTime the basis time
     */
    public void setBasisTime( String basisTime )
    {
        if ( Objects.nonNull( basisTime )
             && !basisTime.isBlank() )
        {
            this.basisTime = OffsetDateTime.parse( basisTime );
        }
    }

    /**
     * Sets the issued time.
     * @param issuedTime the issued time
     */
    public void setIssuedTime( String issuedTime )
    {
        if ( Objects.nonNull( issuedTime )
             && !issuedTime.isBlank() )
        {
            this.issuedTime = OffsetDateTime.parse( issuedTime );
        }
    }

    /**
     * Sets the generation time.
     * @param generationTime the generation time
     */
    public void setGenerationTime( String generationTime )
    {
        if ( Objects.nonNull( generationTime )
             && !generationTime.isBlank() )
        {
            this.generationTime = OffsetDateTime.parse( generationTime );
        }
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "location", this.getLocation() )
                .append( "issuer", this.getIssuer() )
                .append( "distributor", this.getDistributor() )
                .append( "type", this.getType() )
                .append( "units", this.getUnits() )
                .append( "parameterCodes", this.getParameterCodes() )
                .append( "producer", this.getProducer() )
                .append( "members", this.getMembers() )
                .append( "issuedTime", this.getIssuedTime() )
                .append( "basisTime", this.getBasisTime() )
                .append( "generationTime", this.getGenerationTime() )
                .toString();
    }
}
