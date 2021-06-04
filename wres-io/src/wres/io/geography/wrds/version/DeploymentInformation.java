package wres.io.geography.wrds.version;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties( ignoreUnknown = true )
public class DeploymentInformation
{
    private final String version;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public DeploymentInformation( 
                             @JsonProperty( "version" ) 
                             String version 
                           )
    {
        this.version = version;
    }
    
    public String getVersion()
    {
        return version;
    }


    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "version", version )
                                                                            .toString();
    }
}
