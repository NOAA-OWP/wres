package wres.reading.wrds.thresholds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

/**
 * Threshold metadata.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
class ThresholdMetadata implements Serializable
{
    @Serial
    private static final long serialVersionUID = -4195161592990949335L;
    /** Data type. */
    @JsonProperty( "data_type" )
    private String dataType;
    /** NWS LID. */
    @JsonProperty( "nws_lid" )
    private String nwsLid;
    /** USGS site code. */
    @JsonProperty( "usgs_site_code" )
    private String usgsSiteCode;
    /** NWM feature ID. */
    @JsonProperty( "nwm_feature_id" )
    private String nwmFeatureId;
    /** Threshold type. */
    @JsonProperty( "threshold_type" )
    private String thresholdType;
    /** Threshold source. */
    @JsonProperty( "threshold_source" )
    private String thresholdSource;
    /** Threshold source description. */
    @JsonProperty( "threshold_source_description" )
    private String thresholdSourceDescription;
    /** Stage units. */
    @JsonProperty( "stage_units" )
    private String stageUnits;
    /** Flow units. */
    @JsonProperty( "flow_units" )
    private String flowUnits;
    /** Calculated flow units. */
    @JsonProperty( "calc_flow_units" )
    private String calcFlowUnits;
    /** Units. */
    private String units;

    /**
     * @return the data type
     */
    String getDataType()
    {
        return dataType;
    }

    /**
     * Sets the data type.
     * @param dataType the data type
     */
    void setDataType( String dataType )
    {
        this.dataType = dataType;
    }

    /**
     * @return the NWS LID
     */
    String getNwsLid()
    {
        return nwsLid;
    }

    /**
     * Sets the NWS LID.
     * @param nwsLid the NWS LID
     */
    void setNwsLid( String nwsLid )
    {
        this.nwsLid = nwsLid;
    }

    /**
     * @return the USGS site code
     */
    String getUsgsSideCode()
    {
        return usgsSiteCode;
    }

    /**
     * Sets the USGS site code.
     * @param usgsSiteCode the USGS site code
     */
    void setUsgsSideCode( String usgsSiteCode )
    {
        this.usgsSiteCode = usgsSiteCode;
    }

    /**
     * @return the NWM feature ID
     */
    String getNwmFeatureId()
    {
        return nwmFeatureId;
    }

    /**
     * Sets the NWM feature ID.
     * @param nwmFeatureId the NWM feature ID
     */
    void setNwmFeatureId( String nwmFeatureId )
    {
        this.nwmFeatureId = nwmFeatureId;
    }

    /**
     * @return the threshold type
     */
    String getThresholdType()
    {
        return thresholdType;
    }

    /**
     * Sets the threshold type.
     * @param thresholdType the threshold type
     */
    void setThresholdType( String thresholdType )
    {
        this.thresholdType = thresholdType;
    }

    /**
     * @return the threshold store
     */
    String getThresholdSource()
    {
        return thresholdSource;
    }

    /**
     * Sets the threshold source.
     * @param thresholdSource the threshold source
     */
    void setThresholdSource( String thresholdSource )
    {
        this.thresholdSource = thresholdSource;
    }

    /**
     * @return the threshold source description.
     */
    String getThresholdSourceDescription()
    {
        return thresholdSourceDescription;
    }

    /**
     * Sets the threshold source description.
     * @param thresholdSourceDescription the threshold source description.
     */

    void setThresholdSourceDescription( String thresholdSourceDescription )
    {
        this.thresholdSourceDescription = thresholdSourceDescription;
    }

    /**
     * @return the stage units.
     */
    String getStageUnits()
    {
        return stageUnits;
    }

    /**
     * Sets the stage units.
     * @param stageUnits the stage units
     */
    void setStageUnits( String stageUnits )
    {
        this.stageUnits = stageUnits;
    }

    /**
     * @return the flow units.
     */
    String getFlowUnits()
    {
        return flowUnits;
    }

    /**
     * Sets the flow units.
     * @param flowUnits the flow units
     */
    void setFlowUnits( String flowUnits )
    {
        this.flowUnits = flowUnits;
    }

    /**
     * @return the calculated flow units
     */
    String getCalcFlowUnits()
    {
        return calcFlowUnits;
    }

    /**
     * Sets the calculated flow units.
     * @param calcFlowUnits the calculated flow units
     */
    void setCalcFlowUnits( String calcFlowUnits )
    {
        this.calcFlowUnits = calcFlowUnits;
    }

    /**
     * @return the units
     */
    String getUnits()
    {
        return units;
    }

    /**
     * Sets the units.
     * @param units the units
     */
    void setUnits( String units )
    {
        this.units = units;
    }
}
