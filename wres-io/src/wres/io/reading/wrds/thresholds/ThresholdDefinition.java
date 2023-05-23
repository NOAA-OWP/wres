package wres.io.reading.wrds.thresholds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.DoubleValue;

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.pools.MeasurementUnit;
import wres.io.reading.wrds.geography.Location;
import wres.datamodel.units.UnitMapper;
import wres.statistics.generated.Threshold;

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
class ThresholdDefinition implements Serializable
{
    @Serial
    private static final long serialVersionUID = -7094110343140389566L;

    /**
     * Metadata describing where the values are valid and who produced them
     */
    private ThresholdMetadata metadata;

    /**
     * The stage values presented by the all-in-one NWS API.
     */
    @JsonProperty( "stage_values" )
    private ThresholdValues stageValues;

    /**
     * The flow values presented by the all-in-one NWS API.
     */
    @JsonProperty( "flow_values" )
    private ThresholdValues flowValues;

    /**
     * The calculated flow values presented by the all-in-one NWS API.
     */
    @JsonProperty( "calc_flow_values" )
    private ThresholdValues calcFlowValues;

    /**
     * The general values presented by other APIs, including the NWS
     * threshold API when stage or flow is specifically requested.
     */
    private ThresholdValues values;

    /**
     * @return the metadata
     */

    ThresholdMetadata getMetadata()
    {
        return metadata;
    }

    /**
     * Sets the metadata.
     * @param metadata the metadata
     */
    void setMetadata( ThresholdMetadata metadata )
    {
        this.metadata = metadata;
    }

    /**
     * @return the stage values
     */
    ThresholdValues getStageValues()
    {
        return stageValues;
    }

    /**
     * Sets the stage values.
     * @param stageValues the stage values
     */
    void setStageValues( ThresholdValues stageValues )
    {
        this.stageValues = stageValues;
    }

    /**
     * @return the flow values
     */
    ThresholdValues getFlowValues()
    {
        return flowValues;
    }

    /**
     * Sets the flow values
     * @param flowValues the flow values
     */
    void setFlowValues( ThresholdValues flowValues )
    {
        this.flowValues = flowValues;
    }

    /**
     * @return the calculated flow values
     */
    ThresholdValues getCalcFlowValues()
    {
        return calcFlowValues;
    }

    /**
     * Sets the calculated flow values.
     * @param calcFlowValues the calculated flow values
     */
    void setCalcFlowValues( ThresholdValues calcFlowValues )
    {
        this.calcFlowValues = calcFlowValues;
    }

    /**
     * @return the threshold values
     */
    ThresholdValues getValues()
    {
        return values;
    }

    /**
     * Sets the threshold values.
     * @param values the values
     */
    void setValues( ThresholdValues values )
    {
        this.values = values;
    }

    /**
     * @return The threshold source or null if it is not specified.
     */
    String getThresholdProvider()
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
    Location getLocation()
    {
        return new Location( this.metadata.getNwmFeatureId(),
                             this.metadata.getUsgsSideCode(),
                             this.metadata.getNwsLid() );
    }

    /**
     *
     * @return Null if no rating curve is found in the response.  Otherwise,
     * it returns the rating curve's source.  The rating curve info is currently
     * found inside of the calc_flow_values for NWS thresholds only.
     */
    String getRatingProvider()
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
     * otherwise the original flow thresholds are used.  This flag is ignored for all other thresholds.
     * @param desiredUnitMapper the unit mapper
     * @return a map of thresholds by location.  This is a singleton map: only one location will be
     * returned at most.  
     */
    Map<Location, Set<Threshold>> getThresholds( ThresholdType thresholdType,
                                                 ThresholdOperator thresholdOperator,
                                                 ThresholdOrientation dataType,
                                                 UnitMapper desiredUnitMapper )
    {
        Location location = this.getLocation();
        Map<String, Threshold> thresholdMap = new HashMap<>();

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
        if ( this.getValues() != null && !this.getValues().getThresholdValues()
                                              .isEmpty() )
        {
            originalThresholds = getValues().getThresholdValues();
            originalUnitConversionOperator =
                    desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata()
                                                                             .getUnits() )
                                                                    .getUnit() );
        }

        //When values is not used, then we are looking at NWS thresholds,
        //which come with stage, flow, and calculated flow options. Select
        //based on provided threshold type.
        else if ( thresholdType.equals( ThresholdType.STAGE ) )
        {
            if ( this.getStageValues() != null )
            {
                originalThresholds = getStageValues().getThresholdValues();
                originalUnitConversionOperator =
                        desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata()
                                                                                 .getStageUnits() )
                                                                        .getUnit() );
            }
        }
        else
        {
            if ( this.getFlowValues() != null )
            {
                originalThresholds = getFlowValues().getThresholdValues();
                originalUnitConversionOperator =
                        desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata()
                                                                                 .getFlowUnits() )
                                                                        .getUnit() );
            }

            if ( this.getCalcFlowValues() != null )
            {
                calculatedThresholds = this.getCalcFlowValues()
                                           .getThresholdValues();
                calculatedUnitConversionOperator =
                        desiredUnitMapper.getUnitMapper( MeasurementUnit.of( this.getMetadata()
                                                                                 .getCalcFlowUnits() )
                                                                        .getUnit() );
            }
        }

        // First, the original thresholds go into the map.
        this.addOriginalThresholds( originalThresholds,
                                    originalUnitConversionOperator,
                                    thresholdOperator,
                                    dataType,
                                    desiredUnitMapper,
                                    thresholdMap );

        // Then we overwrite the original with calculated.
        this.addCalculatedThresholds( calculatedThresholds,
                                      calculatedUnitConversionOperator,
                                      thresholdOperator,
                                      dataType,
                                      desiredUnitMapper,
                                      thresholdMap );

        return Map.of( location, new HashSet<>( thresholdMap.values() ) );
    }

    /**
     * Adds the original thresholds to the prescribed map.
     * @param originalThresholds the original thresholds
     * @param originalUnitConversionOperator the original threshold unit conversion operator
     * @param thresholdOperator the threshold operator
     * @param dataType the data type
     * @param desiredUnitMapper the desired unit mapper
     * @param thresholdMap the threshold map to mutate
     */
    private void addOriginalThresholds( Map<String, Double> originalThresholds,
                                        DoubleUnaryOperator originalUnitConversionOperator,
                                        ThresholdOperator thresholdOperator,
                                        ThresholdOrientation dataType,
                                        UnitMapper desiredUnitMapper,
                                        Map<String, Threshold> thresholdMap )
    {
        //First, the original thresholds go into the map.
        if ( originalThresholds != null )
        {
            for ( Entry<String, Double> threshold : originalThresholds.entrySet() )
            {
                if ( threshold.getValue() != null )
                {
                    double value = originalUnitConversionOperator.applyAsDouble( threshold.getValue() );
                    Threshold next = this.getValueThreshold( value,
                                                             thresholdOperator,
                                                             dataType,
                                                             threshold.getKey(),
                                                             desiredUnitMapper.getDesiredMeasurementUnitName() );
                    thresholdMap.put( threshold.getKey(), next );
                }
            }
        }
    }

    /**
     * Adds the original thresholds to the prescribed map.
     * @param calculatedThresholds the calculated thresholds
     * @param calculatedUnitConversionOperator the calculated threshold unit conversion operator
     * @param thresholdOperator the threshold operator
     * @param dataType the data type
     * @param desiredUnitMapper the desired unit mapper
     * @param thresholdMap the threshold map to mutate
     */
    private void addCalculatedThresholds( Map<String, Double> calculatedThresholds,
                                          DoubleUnaryOperator calculatedUnitConversionOperator,
                                          ThresholdOperator thresholdOperator,
                                          ThresholdOrientation dataType,
                                          UnitMapper desiredUnitMapper,
                                          Map<String, Threshold> thresholdMap )
    {
        if ( calculatedThresholds != null )
        {
            for ( Entry<String, Double> threshold : calculatedThresholds.entrySet() )
            {
                if ( threshold.getValue() != null )
                {
                    //Build the label.  It will be the threshold key unless that's already used.
                    //If it is already used, then the rating curve source will be added.
                    String label = this.getCalcFlowValues()
                                       .getRatingCurve()
                                       .getSource()
                                   + " "
                                   + threshold.getKey();
                    double value = calculatedUnitConversionOperator.applyAsDouble( threshold.getValue() );
                    Threshold next = this.getValueThreshold( value,
                                                             thresholdOperator,
                                                             dataType,
                                                             label,
                                                             desiredUnitMapper.getDesiredMeasurementUnitName() );
                    thresholdMap.put( label, next );
                }
            }
        }
    }

    /**
     * Creates a value threshold from the inputs.
     * @param threshold the threshold
     * @param thresholdOperator the threshold operator
     * @param dataType the threshold data type
     * @param name the threshold name
     * @param unit the threshold unit
     * @return the threshold
     */

    private Threshold getValueThreshold( double threshold,
                                         ThresholdOperator thresholdOperator,
                                         ThresholdOrientation dataType,
                                         String name,
                                         String unit )
    {
        return Threshold.newBuilder()
                        .setLeftThresholdValue( DoubleValue.of( threshold ) )
                        .setOperator( Threshold.ThresholdOperator.valueOf( thresholdOperator.name() ) )
                        .setDataType( Threshold.ThresholdDataType.valueOf( dataType.name() ) )
                        .setName( name )
                        .setThresholdValueUnits( unit )
                        .build();
    }
}
