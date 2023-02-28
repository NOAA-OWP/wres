package wres.io.thresholds.wrds.v3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.datamodel.units.UnitMapper;
import wres.io.thresholds.wrds.WRDSThresholdType;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;

/**
 * Represents the combined elements that defined an atomic set of thresholds.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class GeneralThresholdDefinition implements Serializable
{
    @Serial
    private static final long serialVersionUID = -7094110343140389566L;

    /**
     * Metadata describing where the values are valid and who produced them
     */
    GeneralThresholdMetadata metadata;

    /**
     * The stage values presented by the all-in-one NWS API.
     */
    @JsonProperty( "stage_values" )
    GeneralThresholdValues stageValues;

    /**
     * The flow values presented by the all-in-one NWS API.
     */
    @JsonProperty( "flow_values" )
    GeneralThresholdValues flowValues;

    /**
     * The calculated flow values presented by the all-in-one NWS API.
     */
    @JsonProperty( "calc_flow_values" )
    GeneralThresholdValues calcFlowValues;

    /**
     * The general values presented by other APIs, including the NWS
     * threshold API when stage or flow is specifically requested.
     */
    GeneralThresholdValues values;

    /**
     * @return the metadata
     */

    public GeneralThresholdMetadata getMetadata()
    {
        return metadata;
    }

    /**
     * Sets the metadata.
     * @param metadata the metadata
     */
    public void setMetadata( GeneralThresholdMetadata metadata )
    {
        this.metadata = metadata;
    }

    /**
     * @return the stage values
     */
    public GeneralThresholdValues getStageValues()
    {
        return stageValues;
    }

    /**
     * Sets the stage values.
     * @param stageValues the stage values
     */
    public void setStageValues( GeneralThresholdValues stageValues )
    {
        this.stageValues = stageValues;
    }

    /**
     * @return the flow values
     */
    public GeneralThresholdValues getFlowValues()
    {
        return flowValues;
    }

    /**
     * Sets the flow values
     * @param flowValues the flow values
     */
    public void setFlowValues( GeneralThresholdValues flowValues )
    {
        this.flowValues = flowValues;
    }

    /**
     * @return the calculated flow values
     */
    public GeneralThresholdValues getCalcFlowValues()
    {
        return calcFlowValues;
    }

    /**
     * Sets the calculated flow values.
     * @param calcFlowValues the calculated flow values
     */
    public void setCalcFlowValues( GeneralThresholdValues calcFlowValues )
    {
        this.calcFlowValues = calcFlowValues;
    }

    /**
     * @return the threshold values
     */
    public GeneralThresholdValues getValues()
    {
        return values;
    }

    /**
     * Sets the threshold values.
     * @param values the values
     */
    public void setValues( GeneralThresholdValues values )
    {
        this.values = values;
    }

    /**
     * @return The threshold source or null if it is not specified.
     */
    public String getThresholdProvider()
    {
        String thresholdProvider = null;
        if ( Objects.nonNull( this.getMetadata() ) )
        {
            thresholdProvider = this.getMetadata().getThresholdSource();
        }

        return thresholdProvider;
    }

    /**
     * @return the WRDS location
     */
    public WrdsLocation getLocation()
    {
        return new WrdsLocation( this.metadata.getNwmFeatureId(),
                                 this.metadata.getUsgsSideCode(),
                                 this.metadata.getNwsLid() );
    }

    /**
     *
     * @return Null if no rating curve is found in the response.  Otherwise,
     * it returns the rating curve's source.  The rating curve info is currently
     * found inside of the calc_flow_values for NWS thresholds only.
     */
    public String getRatingProvider()
    {
        String ratingProvider = null;
        if ( Objects.nonNull( this.getCalcFlowValues() ) )
        {

            ratingProvider = this.getCalcFlowValues().getRatingCurve().getSource();
        }

        return ratingProvider;
    }

    /**
     *
     * @param thresholdType the threshold type
     * @param thresholdOperator the threshold operator
     * @param dataType the data type
     * @param getCalculated If true, then calculated will be used for flow thresholds if available; 
     * otherwise the original flow thresholds are used.  This flag is ignored for all other thresholds.
     * @param desiredUnitMapper the unit mapper
     * @return a map of thresholds by location.  This is a singleton map: only one location will be
     * returned at most.  
     */
    public Map<WrdsLocation, Set<ThresholdOuter>> getThresholds( WRDSThresholdType thresholdType,
                                                                 ThresholdConstants.Operator thresholdOperator,
                                                                 ThresholdConstants.ThresholdDataType dataType,
                                                                 boolean getCalculated,
                                                                 UnitMapper desiredUnitMapper )
    {
        WrdsLocation location = this.getLocation();
        Map<String, ThresholdOuter> thresholdMap = new HashMap<>();

        //If either of these is null, then it is not used. 
        //Be sure to check for null before attempting to use.
        Map<String, Double> calculatedThresholds = null;
        Map<String, Double> originalThresholds = null;

        //These are the measurement unit operators based on a desired unit,
        //passed in, and the String units associated with the threshold.
        DoubleUnaryOperator originalUnitConversionOperator = null;
        DoubleUnaryOperator calculatedUnitConversionOperator = null;

        //Point the two maps and identify the unit operator appropriately.
        //Unified schema values takes precedence over all others.
        if ( getValues() != null && !getValues().getThresholdValues().isEmpty() )
        {
            originalThresholds = getValues().getThresholdValues();
            originalUnitConversionOperator =
                    desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata().getUnits() ).getUnit() );
        }

        //When values is not used, then we are looking at NWS thresholds,
        //which come with stage, flow, and calculated flow options. Select
        //based on provided threshold type.
        else if ( thresholdType.equals( WRDSThresholdType.STAGE ) )
        {
            if ( getStageValues() != null )
            {
                originalThresholds = getStageValues().getThresholdValues();
                originalUnitConversionOperator =
                        desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata().getStageUnits() )
                                                                        .getUnit() );
            }
        }
        else
        {
            if ( getFlowValues() != null )
            {
                originalThresholds = getFlowValues().getThresholdValues();
                originalUnitConversionOperator =
                        desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata().getFlowUnits() )
                                                                        .getUnit() );
            }

            if ( getCalculated && this.getCalcFlowValues() != null )
            {
                calculatedThresholds = getCalcFlowValues().getThresholdValues();
                calculatedUnitConversionOperator =
                        desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata()
                                                                                 .getCalcFlowUnits() )
                                                                        .getUnit() );
            }
        }

        //First, the original thresholds go into the map.
        if ( originalThresholds != null )
        {
            for ( Entry<String, Double> threshold : originalThresholds.entrySet() )
            {
                if ( threshold.getValue() != null )
                {
                    double value = originalUnitConversionOperator.applyAsDouble( threshold.getValue() );
                    thresholdMap.put( threshold.getKey(),
                                      ThresholdOuter.of(
                                              OneOrTwoDoubles.of( value ),
                                              thresholdOperator,
                                              dataType,
                                              threshold.getKey(),
                                              MeasurementUnit.of( desiredUnitMapper.getDesiredMeasurementUnitName() ) ) );
                }
            }
        }

        //Then we overwrite the original with calculated.
        if ( calculatedThresholds != null )
        {
            for ( Entry<String, Double> threshold : calculatedThresholds.entrySet() )
            {
                if ( threshold.getValue() != null )
                {
                    //Build the label.  It will be the threshold key unless that's already used.
                    //If it is already used, then the rating curve source will be added.
                    String label =
                            getCalcFlowValues().getRatingCurve().getSource() + " " + threshold.getKey();
                    double value = calculatedUnitConversionOperator.applyAsDouble( threshold.getValue() );
                    thresholdMap.put( label,
                                      ThresholdOuter.of(
                                              OneOrTwoDoubles.of( value ),
                                              thresholdOperator,
                                              dataType,
                                              label,
                                              MeasurementUnit.of( desiredUnitMapper.getDesiredMeasurementUnitName() ) ) );
                }
            }
        }

        return Map.of( location, new HashSet<>( thresholdMap.values() ) );
    }
}
