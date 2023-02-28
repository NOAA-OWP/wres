package wres.io.thresholds.wrds.v3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

/**
 * Threshold metadata.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class GeneralThresholdMetadata implements Serializable
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
    public String getDataType()
    {
        return dataType;
    }

    /**
     * Sets the data type.
     * @param dataType the data type
     */
    public void setDataType( String dataType )
    {
        this.dataType = dataType;
    }

    /**
     * @return the NWS LID
     */
    public String getNwsLid()
    {
        return nwsLid;
    }

    /**
     * Sets the NWS LID.
     * @param nwsLid the NWS LID
     */
    public void setNwsLid( String nwsLid )
    {
        this.nwsLid = nwsLid;
    }

    /**
     * @return the USGS site code
     */
    public String getUsgsSideCode()
    {
        return usgsSiteCode;
    }

    /**
     * Sets the USGS site code.
     * @param usgsSiteCode the USGS site code
     */
    public void setUsgsSideCode( String usgsSiteCode )
    {
        this.usgsSiteCode = usgsSiteCode;
    }

    /**
     * @return the NWM feature ID
     */
    public String getNwmFeatureId()
    {
        return nwmFeatureId;
    }

    /**
     * Sets the NWM feature ID.
     * @param nwmFeatureId the NWM feature ID
     */
    public void setNwmFeatureId( String nwmFeatureId )
    {
        this.nwmFeatureId = nwmFeatureId;
    }

    /**
     * @return the threshold type
     */
    public String getThresholdType()
    {
        return thresholdType;
    }

    /**
     * Sets the threshold type.
     * @param thresholdType the threshold type
     */
    public void setThresholdType( String thresholdType )
    {
        this.thresholdType = thresholdType;
    }

    /**
     * @return the threshold store
     */
    public String getThresholdSource()
    {
        return thresholdSource;
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
     * @return the threshold source description.
     */
    public String getThresholdSourceDescription()
    {
        return thresholdSourceDescription;
    }

    /**
     * Sets the threshold source description.
     * @param thresholdSourceDescription the threshold source description.
     */

    public void setThresholdSourceDescription( String thresholdSourceDescription )
    {
        this.thresholdSourceDescription = thresholdSourceDescription;
    }

    /**
     * @return the stage units.
     */
    public String getStageUnits()
    {
        return stageUnits;
    }

    /**
     * Sets the stage units.
     * @param stageUnits the stage units
     */
    public void setStageUnits( String stageUnits )
    {
        this.stageUnits = stageUnits;
    }

    /**
     * @return the flow units.
     */
    public String getFlowUnits()
    {
        return flowUnits;
    }

    /**
     * Sets the flow units.
     * @param flowUnits the flow units
     */
    public void setFlowUnits( String flowUnits )
    {
        this.flowUnits = flowUnits;
    }

    /**
     * @return the calculated flow units
     */
    public String getCalcFlowUnits()
    {
        return calcFlowUnits;
    }

    /**
     * Sets the calculated flow units.
     * @param calcFlowUnits the calculated flow units
     */
    public void setCalcFlowUnits( String calcFlowUnits )
    {
        this.calcFlowUnits = calcFlowUnits;
    }

    /**
     * @return the units
     */
    public String getUnits()
    {
        return units;
    }

    /**
     * Sets the units.
     * @param units the units
     */
    public void setUnits( String units )
    {
        this.units = units;
    }
}
