package wres.io.reading.wrds.thresholds.v2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Threshold metadata.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class ThresholdMetadata implements Serializable
{
    @Serial
    private static final long serialVersionUID = 3373661366972125521L;

    /** Location ID. */
    @JsonProperty( "location_id" )
    private String locationId;
    /** NWS LID. */
    @JsonProperty( "nws_lid" )
    private String nwsLid;
    /** USGS site code. */
    @JsonProperty( "usgs_site_code" )
    private String usgsSiteCode;
    /** NWS feature ID. */
    @JsonProperty( "nwm_feature_id" )
    private String nwmFeatureId;
    /** Feature ID type. */
    @JsonProperty( "id_type" )
    private String idType;
    /** Threshold source. */
    @JsonProperty( "threshold_source" )
    private String thresholdSource;
    /** Threshold source description. */
    @JsonProperty( "threshold_source_description" )
    private String thresholdSourceDescription;
    /** Rating source. */
    @JsonProperty( "rating_source" )
    private String ratingSource;
    /** Rating source description. */
    @JsonProperty( "rating_source_description" )
    private String ratingSourceDescription;
    /** Stage unit. */
    @JsonProperty( "stage_unit" )
    private String stageUnit;
    /** Flow unit. */
    @JsonProperty( "flow_unit" )
    private String flowUnit;
    /** Rating. */
    private Map<String, String> rating;

    /**
     * @return the location ID
     */
    public String getLocationId()
    {
        return this.locationId;
    }

    /**
     * @return the type of ID
     */
    public String getIdType()
    {
        return idType;
    }

    /**
     * @return the threshold source
     */
    public String getThresholdSource()
    {
        if ( Objects.isNull( this.thresholdSource ) || this.thresholdSource.equals( "None" ) )
        {
            return null;
        }
        return this.thresholdSource;
    }

    /**
     * @return the NWS LID
     */
    public String getNwsLid()
    {
        if ( this.nwsLid == null || this.nwsLid.equals( "None" ) )
        {
            return null;
        }
        return nwsLid;
    }

    /**
     * Sets the NWS LID
     * @param nwsLid the NWS LID
     */
    public void setNwsLid( String nwsLid )
    {
        this.nwsLid = nwsLid;
    }

    /**
     * @return the USGS site code
     */
    public String getUsgsSiteCode()
    {
        if ( this.usgsSiteCode == null || this.usgsSiteCode.equals( "None" ) )
        {
            return null;
        }
        return usgsSiteCode;
    }

    /**
     * Sets the USGS site code
     * @param usgsSiteCode the USGS site code
     */
    public void setUsgsSiteCode( String usgsSiteCode )
    {
        this.usgsSiteCode = usgsSiteCode;
    }

    /**
     * @return the NWM feature ID
     */
    public String getNwmFeatureId()
    {
        if ( this.nwmFeatureId == null || this.nwmFeatureId.equals( "None" ) )
        {
            return null;
        }
        return nwmFeatureId;
    }

    /**
     * Sets the NWM feature ID
     * @param nwmFeatureId the NWM feature ID
     */
    public void setNwmFeatureId( String nwmFeatureId )
    {
        this.nwmFeatureId = nwmFeatureId;
    }

    /**
     * @return the threshold source description
     */
    public String getThresholdSourceDescription()
    {
        return this.thresholdSourceDescription;
    }

    /**
     * Sets the location ID
     * @param locationId the location ID
     */
    public void setLocationId( String locationId )
    {
        this.locationId = locationId;
    }

    /**
     * Sets the ID type.
     * @param idType the ID type
     */
    public void setIdType( String idType )
    {
        this.idType = idType;
    }

    /**
     * Sets the threshold source.
     * @param thresholdSource the threshold source
     */
    public void setThresholdSource( String thresholdSource )
    {
        this.thresholdSource = thresholdSource;
    }

    /**
     * Sets the threshold source description.
     * @param thresholdSourceDescription the threshold source description
     */
    public void setThresholdSourceDescription( String thresholdSourceDescription )
    {
        this.thresholdSourceDescription = thresholdSourceDescription;
    }

    /**
     * @return the ratings source
     */
    public String getRatingSource()
    {
        if ( Objects.isNull( this.ratingSource ) || this.ratingSource.equals( "None" ) )
        {
            return null;
        }
        return ratingSource;
    }

    /**
     * Sets the ratings source.
     * @param ratingSource the ratings source
     */
    public void setRatingSource( String ratingSource )
    {
        this.ratingSource = ratingSource;
    }

    /**
     * @return the ratings source description
     */
    public String getRatingSourceDescription()
    {
        return ratingSourceDescription;
    }

    /**
     * Sets the ratings source description
     * @param ratingSourceDescription the ratings source description
     */
    public void setRatingSourceDescription( String ratingSourceDescription )
    {
        this.ratingSourceDescription = ratingSourceDescription;
    }

    /**
     * @return the stage unit
     */
    public String getStageUnit()
    {
        return stageUnit;
    }

    /**
     * Sets the stage unit.
     * @param stageUnit the stage unit
     */
    public void setStageUnit( String stageUnit )
    {
        this.stageUnit = stageUnit;
    }

    /**
     * @return the flow unit
     */
    public String getFlowUnit()
    {
        return flowUnit;
    }

    /**
     * Sets the flow unit.
     * @param flowUnit the flow unit
     */
    public void setFlowUnit( String flowUnit )
    {
        this.flowUnit = flowUnit;
    }

    /**
     * @return the rating
     */
    public Map<String, String> getRating()
    {
        return rating;
    }

    /**
     * Sets the rating.
     * @param rating the rating
     */
    public void setRating( Map<String, String> rating )
    {
        this.rating = rating;
    }
}
