package wres.reading.wrds.geography;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Deployment information.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public record DeploymentInformation( String version )
{
    /**
     * Creates an instance.
     * @param version the version
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public DeploymentInformation( @JsonProperty( "version" ) String version ) //NOSONAR
    {
        this.version = version;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "version", version )
                .toString();
    }
}
