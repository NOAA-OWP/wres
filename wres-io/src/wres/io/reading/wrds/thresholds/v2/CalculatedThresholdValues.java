package wres.io.reading.wrds.thresholds.v2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents threshold values that were formed as part of a calculation
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class CalculatedThresholdValues implements Serializable
{

    @Serial
    private static final long serialVersionUID = 9113179713864369110L;

    /** The calculated measurement for low flow rate. */
    @JsonProperty( "low_flow" )
    private String lowFlow;

    /** The calculated measurement for bankfull flow rate. */
    @JsonProperty( "bankfull_flow" )
    private String bankfullFlow;

    /** The calculated measurement for action flow rate. */
    @JsonProperty( "action_flow" )
    private String actionFlow;

    /** The calculated measurement for minor flow rate. */
    @JsonProperty( "minor_flow" )
    private String minorFlow;

    /** The calculated measurement for moderate flow rate. */
    @JsonProperty( "moderate_flow" )
    private String moderateFlow;

    /** The calculated measurement for major flow rate. */
    @JsonProperty( "major_flow" )
    private String majorFlow;

    /** The calculated measurement for record flow rate. */
    @JsonProperty( "record_flow" )
    private String recordFlow;

    /** The calculated measurement for low stage height. */
    @JsonProperty( "low_stage" )
    private String lowStage;

    /** The calculated measurement for bankfull stage height. */
    @JsonProperty( "bankfull_stage" )
    private String bankfullStage;

    /** The calculated measurement for action stage height. */
    @JsonProperty( "action_stage" )
    private String actionStage;

    /** The calculated measurement for the height of the minor stage. */
    @JsonProperty( "minor_stage" )
    private String minorStage;

    /** The calculated measurement for the height of the moderate stage. */
    @JsonProperty( "moderate_stage" )
    private String moderateStage;

    /** The calculated measurement for the height of the major stage. */
    @JsonProperty( "major_stage" )
    private String majorStage;

    /** The calculated measurement for the height of the record stage. */
    @JsonProperty( "record_stage" )
    private String recordStage;

    /**
     * Gets the actual low flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getLowFlow()
    {
        if ( this.lowFlow == null || this.lowFlow.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( lowFlow );
    }

    /**
     * Sets the low flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param lowFlow The rate for low flow conditions
     */
    public void setLowFlow( String lowFlow )
    {
        this.lowFlow = lowFlow;
    }

    /**
     * Gets the actual bankfull flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getBankfullFlow()
    {
        if ( this.bankfullFlow == null || this.bankfullFlow.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.bankfullFlow );
    }

    /**
     * Sets the bankfull flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param bankfullFlow The rate for bankfull flow conditions
     */
    public void setBankfullFlow( String bankfullFlow )
    {
        this.bankfullFlow = bankfullFlow;
    }

    /**
     * Gets the actual action flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getActionFlow()
    {
        if ( this.actionFlow == null || this.actionFlow.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.actionFlow );
    }

    /**
     * Sets the action flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param actionFlow The rate for action flow conditions
     */
    public void setActionFlow( String actionFlow )
    {
        this.actionFlow = actionFlow;
    }

    /**
     * Gets the actual minor flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMinorFlow()
    {
        if ( this.minorFlow == null || this.minorFlow.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.minorFlow );
    }

    /**
     * Sets the minor flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param minorFlow The rate for minor flow conditions
     */
    public void setMinorFlow( String minorFlow )
    {
        this.minorFlow = minorFlow;
    }

    /**
     * Gets the actual moderate flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getModerateFlow()
    {
        if ( this.moderateFlow == null || this.moderateFlow.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.moderateFlow );
    }

    /**
     * Sets the moderate flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param moderateFlow The rate for moderate flow conditions
     */
    public void setModerateFlow( String moderateFlow )
    {
        this.moderateFlow = moderateFlow;
    }

    /**
     * Gets the actual major flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMajorFlow()
    {
        if ( this.majorFlow == null || this.majorFlow.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.majorFlow );
    }

    /**
     * Sets the major flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param majorFlow The rate for major flow conditions
     */
    public void setMajorFlow( String majorFlow )
    {
        this.majorFlow = majorFlow;
    }

    /**
     * Gets the actual record flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getRecordFlow()
    {
        if ( this.recordFlow == null || this.recordFlow.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.recordFlow );
    }

    /**
     * Sets the record flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param recordFlow The rate for record flow conditions
     */
    public void setRecordFlow( String recordFlow )
    {
        this.recordFlow = recordFlow;
    }

    /**
     * Gets the actual low stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getLowStage()
    {
        if ( this.lowStage == null || this.lowStage.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.lowStage );
    }

    /**
     * Sets the value for low stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param lowStage The height for low stage conditions
     */
    public void setLowStage( String lowStage )
    {
        this.lowStage = lowStage;
    }

    /**
     * Gets the actual bankfull stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getBankfullStage()
    {
        if ( this.bankfullStage == null || this.bankfullStage.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.bankfullStage );
    }

    /**
     * Sets the value for bankfull stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param bankfullStage The height for low stage conditions
     */
    public void setBankfullStage( String bankfullStage )
    {
        this.bankfullStage = bankfullStage;
    }

    /**
     * Gets the actual action stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getActionStage()
    {
        if ( this.actionStage == null || this.actionStage.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.actionStage );
    }

    /**
     * Sets the value for action stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param actionStage The height for action stage conditions
     */
    public void setActionStage( String actionStage )
    {
        this.actionStage = actionStage;
    }

    /**
     * Gets the actual minor stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMinorStage()
    {
        if ( this.minorStage == null || this.minorStage.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.minorStage );
    }

    /**
     * Sets the value for minor stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param minorStage The height for minor stage conditions
     */
    public void setMinorStage( String minorStage )
    {
        this.minorStage = minorStage;
    }

    /**
     * Gets the actual moderate stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getModerateStage()
    {
        if ( this.moderateStage == null || this.moderateStage.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.moderateStage );
    }

    /**
     * Sets the value for moderate stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param moderateStage The height for moderate stage conditions
     */
    public void setModerateStage( String moderateStage )
    {
        this.moderateStage = moderateStage;
    }

    /**
     * Gets the actual major stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMajorStage()
    {
        if ( this.majorStage == null || this.majorStage.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.majorStage );
    }

    /**
     * Sets the value for major stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param majorStage The height for minor stage conditions
     */
    public void setMajorStage( String majorStage )
    {
        this.majorStage = majorStage;
    }

    /**
     * Gets the actual record stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getRecordStage()
    {
        if ( this.recordStage == null || this.recordStage.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        return Double.parseDouble( this.recordStage );
    }

    /**
     * Sets the value for record stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param recordStage The height for record stage conditions
     */
    public void setRecordStage( String recordStage )
    {
        this.recordStage = recordStage;
    }
}
