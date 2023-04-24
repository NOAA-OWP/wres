package wres.io.reading.wrds.thresholds.v3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Rating curve information.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class RatingCurveInfo implements Serializable
{
    @Serial
    private static final long serialVersionUID = 1947722676033834492L;

    /** Location ID. */
    @JsonProperty( "location_id" )
    private String locationId;
    /** Type of ID. */
    @JsonProperty( "id_type" )
    private String idType;
    /** Source. */
    private String source;
    /** Description. */
    private String description;
    /** Interpolation scheme. */
    @JsonProperty( "interpolation_method" )
    /** Interpolation method. */
    private String interpolationMethod;
    /** Interpolation description. */
    @JsonProperty( "interpolation_description" )
    private String interpolationDescription;

    /**
     * @return the location ID
     */
    public String getLocationId()
    {
        return this.locationId;
    }

    /**
     * @return the identifier type
     */
    public String getIdType()
    {
        return idType;
    }

    /**
     * @return the sources
     */
    public String getSource()
    {
        if ( Objects.isNull( this.source ) || this.source.equals( "None" ) )
        {
            return null;
        }
        return this.source;
    }

    /**
     * @return the description
     */
    public String getDescription()
    {
        return this.description;
    }

    /**
     * @return the interpolation method
     */
    public String getInterpolationMethod()
    {
        return this.interpolationMethod;
    }

    /**
     * @return the interpolation description
     */
    public String getInterplolationDescription()
    {
        return this.interpolationDescription;
    }

    /**
     * Sets trhe location ID.
     * @param locationId the location ID
     */
    public void setLocationId( String locationId )
    {
        this.locationId = locationId;
    }

    /**
     * Sets the identifier type.
     * @param idType the identifier type
     */
    public void setIdType( String idType )
    {
        this.idType = idType;
    }

    /**
     * Sets the sources.
     * @param source the source
     */
    public void setSource( String source )
    {
        this.source = source;
    }

    /**
     * Sets the description.
     * @param description the description
     */
    public void setDescription( String description )
    {
        this.description = description;
    }
}
