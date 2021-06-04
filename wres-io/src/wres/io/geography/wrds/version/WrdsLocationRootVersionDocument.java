package wres.io.geography.wrds.version;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import wres.io.geography.wrds.WrdsLocationInformation;

/*
Parse deployment and version information from JSON that starts like this: 
{
    "_metrics": {
        "location_count": 1,
        "model_tracing_api_call": 0.008093118667602539,
        "total_request_time": 1.7286653518676758
    },
    "_warnings": [],
    "_documentation": {
        "swagger URL": "http://***REMOVED***.***REMOVED***.***REMOVED***/docs/location/v3.0/swagger/"
    },
    "deployment": {
        "api_url": "https://***REMOVED***.***REMOVED***.***REMOVED***/api/location/v3.0/metadata/nws_lid/OGCN2/",
        "stack": "prod",
        "version": "v3.1.0"
    },...
}

Note that older API will not include deployment, so I need to allow for a null deployment object.
 */
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class WrdsLocationRootVersionDocument
{

    private final DeploymentInformation deploymentInfo;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public WrdsLocationRootVersionDocument( @JsonProperty( "deployment" )
                                     DeploymentInformation deploymentInfo )
    {
        this.deploymentInfo = deploymentInfo;
    }
    
    public DeploymentInformation getDeploymentInfo()
    {
        return this.deploymentInfo;
    }
    
    public boolean isDeploymentInfoPresent()
    {
        return (this.deploymentInfo != null);
    }


    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "deployment", deploymentInfo )
                .toString();
    }
    
}
