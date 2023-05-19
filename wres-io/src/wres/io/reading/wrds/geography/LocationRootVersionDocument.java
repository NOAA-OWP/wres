package wres.io.reading.wrds.geography;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
Parse deployment and version information from JSON that starts like this:
{
    "_metrics": {
        "location_count": 1,
        "model_tracing_api_call": 0.008093118667602539,
        "total_request_time": 1.7286653518676758
    },
    "_warnings": [],
    "_documentation": {
        "swagger URL": "http://redactedv/docs/location/v3.0/swagger/"
    },
    "deployment": {
        "api_url": "https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/",
        "stack": "prod",
        "version": "v3.1.0"
    },...
}
 */
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class LocationRootVersionDocument
{
    /** Deployment information. */
    private final DeploymentInformation deploymentInfo;

    /**
     * Creates an instance.
     * @param deploymentInfo deployment information
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    LocationRootVersionDocument( @JsonProperty( "deployment" )
                                     DeploymentInformation deploymentInfo )
    {
        this.deploymentInfo = deploymentInfo;
    }

    /**
     * @return the deployment information
     */
    public DeploymentInformation getDeploymentInfo()
    {
        return this.deploymentInfo;
    }

    /**
     * @return whether deployment information is present
     */
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
