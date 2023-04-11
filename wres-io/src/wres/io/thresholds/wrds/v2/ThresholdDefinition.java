package wres.io.thresholds.wrds.v2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.DoubleValue;

import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.datamodel.units.UnitMapper;
import wres.io.thresholds.wrds.WRDSThresholdType;
import wres.statistics.generated.Threshold;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;

/**
 * Represents the combined elements that defined an atomic set of thresholds
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class ThresholdDefinition implements Serializable
{
    @Serial
    private static final long serialVersionUID = 7011802048254614431L;

    /** Original values. */
    @JsonProperty( "original_values" )
    OriginalThresholdValues originalValues;
    /** Calculated values. */
    @JsonProperty( "calculated_values" )
    CalculatedThresholdValues calculatedValues;

    /** Metadata describing where the values are valid and who produced them. */
    ThresholdMetadata metadata;

    /**
     * @return The metadata for the combined set of thresholds
     */
    public ThresholdMetadata getMetadata()
    {
        return metadata;
    }

    /**
     * @return the location
     */
    public WrdsLocation getLocation()
    {
        return new WrdsLocation(
                this.metadata.getNwmFeatureId(),
                this.metadata.getUsgsSiteCode(),
                this.metadata.getNwsLid()
        );
    }

    /**
     * Sets the metadata for the definition
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param metadata The metadata for the combined set of thresholds
     */
    public void setMetadata( ThresholdMetadata metadata )
    {
        this.metadata = metadata;
    }

    /**
     * Gets the original, unprocessed set of threshold values
     * <br>
     *     <b>NOTE:</b> Many thresholds are calculated rather than observed
     *
     * @return The set of measured thresholds
     */
    public OriginalThresholdValues getOriginalValues()
    {
        return originalValues;
    }

    /**
     * Sets the original values.
     * @param originalValues the original values
     */
    public void setOriginalValues( OriginalThresholdValues originalValues )
    {
        this.originalValues = originalValues;
    }

    /**
     * @return the calculated values
     */
    public CalculatedThresholdValues getCalculatedValues()
    {
        return calculatedValues;
    }

    /**
     * Sets the calculated values.
     * @param calculatedValues the calculated values
     */
    public void setCalculatedValues( CalculatedThresholdValues calculatedValues )
    {
        this.calculatedValues = calculatedValues;
    }

    /**
     * @return the threshold provider
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
     * @return the ratings provider
     */
    public String getRatingProvider()
    {
        String ratingProvider = null;
        if ( Objects.nonNull( this.getMetadata() ) )
        {
            ratingProvider = this.getMetadata().getRatingSource();
        }

        return ratingProvider;
    }

    /**
     * Gets the low flow threshold.
     * @param getCalculated is true to return the calculated flow, false for regular
     * @param unitMapper the unit mapper
     * @return the low flow
     */
    public Double getLowFlow( boolean getCalculated, UnitMapper unitMapper )
    {
        Double flow;

        if ( getCalculated && this.getCalculatedValues().getLowFlow() != null )
        {
            flow = this.getCalculatedValues().getLowFlow();
        }
        else
        {
            flow = this.getOriginalValues().getLowFlow();
        }

        if ( flow == null )
        {
            return null;
        }

        return this.getFlowUnitConversion( unitMapper ).applyAsDouble( flow );
    }

    /**
     * Gets the low stage.
     * @param getCalculated is true to return the calculated stage, false for regular
     * @param unitMapper the unit mapper
     * @return the low stage
     */
    public Double getLowStage( boolean getCalculated, UnitMapper unitMapper )
    {
        Double stage;

        if ( getCalculated && this.getCalculatedValues().getLowStage() != null )
        {
            stage = this.getCalculatedValues().getLowStage();
        }
        else
        {
            stage = this.getOriginalValues().getLowStage();
        }

        if ( stage == null )
        {
            return null;
        }

        return this.getStageUnitConversion( unitMapper ).applyAsDouble( stage );
    }

    /**
     * Gets the action flow.
     * @param getCalculated is true to return the calculated flow, false for regular
     * @param unitMapper the unit mapper
     * @return the action flow
     */
    public Double getActionFlow( boolean getCalculated, UnitMapper unitMapper )
    {
        Double flow;

        if ( getCalculated && this.getCalculatedValues().getActionFlow() != null )
        {
            flow = this.getCalculatedValues().getActionFlow();
        }
        else
        {
            flow = this.getOriginalValues().getActionFlow();
        }

        if ( flow == null )
        {
            return null;
        }

        return this.getFlowUnitConversion( unitMapper ).applyAsDouble( flow );
    }

    /**
     * Gets the action stage.
     * @param getCalculated is true to return the calculated stage, false for regular
     * @param unitMapper the unit mapper
     * @return the action stage
     */

    public Double getActionStage( boolean getCalculated, UnitMapper unitMapper )
    {
        Double stage;

        if ( getCalculated && this.getCalculatedValues().getActionStage() != null )
        {
            stage = this.getCalculatedValues().getActionStage();
        }
        else
        {
            stage = this.getOriginalValues().getActionStage();
        }

        if ( stage == null )
        {
            return null;
        }

        return this.getStageUnitConversion( unitMapper ).applyAsDouble( stage );
    }

    /**
     * Gets the minor flood flow.
     * @param getCalculated is true to return the calculated minor flood flow, false for regular
     * @param unitMapper the unit mapper
     * @return the minor flood flow
     */

    public Double getMinorFlow( boolean getCalculated, UnitMapper unitMapper )
    {
        Double flow;

        if ( getCalculated && this.getCalculatedValues().getMinorFlow() != null )
        {
            flow = this.getCalculatedValues().getMinorFlow();
        }
        else
        {
            flow = this.getOriginalValues().getMinorFlow();
        }

        if ( flow == null )
        {
            return null;
        }

        return this.getFlowUnitConversion( unitMapper ).applyAsDouble( flow );
    }

    /**
     * Gets the minor flood stage.
     * @param getCalculated is true to return the calculated minor flood stage, false for regular
     * @param unitMapper the unit mapper
     * @return the minor flood stage
     */

    public Double getMinorStage( boolean getCalculated, UnitMapper unitMapper )
    {
        Double stage;

        if ( getCalculated && this.getCalculatedValues().getMinorStage() != null )
        {
            stage = this.getCalculatedValues().getMinorStage();
        }
        else
        {
            stage = this.getOriginalValues().getMinorStage();
        }

        if ( stage == null )
        {
            return null;
        }

        return this.getStageUnitConversion( unitMapper ).applyAsDouble( stage );
    }

    /**
     * Gets the moderate flood flow.
     * @param getCalculated is true to return the calculated moderate flood flow, false for regular
     * @param unitMapper the unit mapper
     * @return the moderate flood flow
     */

    public Double getModerateFlow( boolean getCalculated, UnitMapper unitMapper )
    {
        Double flow;

        if ( getCalculated && this.getCalculatedValues().getModerateFlow() != null )
        {
            flow = this.getCalculatedValues().getModerateFlow();
        }
        else
        {
            flow = this.getOriginalValues().getModerateFlow();
        }

        if ( flow == null )
        {
            return null;
        }

        return this.getFlowUnitConversion( unitMapper ).applyAsDouble( flow );
    }

    /**
     * Gets the moderate flood stage.
     * @param getCalculated is true to return the calculated moderate flood stage, false for regular
     * @param unitMapper the unit mapper
     * @return the moderate flood stage
     */

    public Double getModerateStage( boolean getCalculated, UnitMapper unitMapper )
    {
        Double stage;

        if ( getCalculated && this.getCalculatedValues().getModerateStage() != null )
        {
            stage = this.getCalculatedValues().getModerateStage();
        }
        else
        {
            stage = this.getOriginalValues().getModerateStage();
        }

        if ( stage == null )
        {
            return null;
        }

        return this.getStageUnitConversion( unitMapper ).applyAsDouble( stage );
    }

    /**
     * Gets the major flood flow.
     * @param getCalculated is true to return the calculated major flood flow, false for regular
     * @param unitMapper the unit mapper
     * @return the major flood flow
     */
    public Double getMajorFlow( boolean getCalculated, UnitMapper unitMapper )
    {
        Double flow;

        if ( getCalculated && this.getCalculatedValues().getMajorFlow() != null )
        {
            flow = this.getCalculatedValues().getMajorFlow();
        }
        else
        {
            flow = this.getOriginalValues().getMajorFlow();
        }

        if ( flow == null )
        {
            return null;
        }

        return this.getFlowUnitConversion( unitMapper ).applyAsDouble( flow );
    }

    /**
     * Gets the major flood stage.
     * @param getCalculated is true to return the calculated major flood stage, false for regular
     * @param unitMapper the unit mapper
     * @return the major flood stage
     */

    public Double getMajorStage( boolean getCalculated, UnitMapper unitMapper )
    {
        Double stage;

        if ( getCalculated && this.getCalculatedValues().getMajorStage() != null )
        {
            stage = this.getCalculatedValues().getMajorStage();
        }
        else
        {
            stage = this.getOriginalValues().getMajorStage();
        }

        if ( stage == null )
        {
            return null;
        }

        return this.getStageUnitConversion( unitMapper ).applyAsDouble( stage );
    }

    /**
     * Gets the bankful flow.
     * @param getCalculated is true to return the calculated bankful flow, false for regular
     * @param unitMapper the unit mapper
     * @return the bankful flow
     */
    public Double getBankfulFlow( boolean getCalculated, UnitMapper unitMapper )
    {
        Double flow;

        if ( getCalculated && this.getCalculatedValues().getBankfullFlow() != null )
        {
            flow = this.getCalculatedValues().getBankfullFlow();
        }
        else
        {
            flow = this.getOriginalValues().getBankfullFlow();
        }

        if ( flow == null )
        {
            return null;
        }

        return this.getFlowUnitConversion( unitMapper ).applyAsDouble( flow );
    }

    /**
     * Gets the bankful stage.
     * @param getCalculated is true to return the calculated bankful stage, false for regular
     * @param unitMapper the unit mapper
     * @return the bankful stage
     */

    public Double getBankfulStage( boolean getCalculated, UnitMapper unitMapper )
    {
        Double stage;

        if ( getCalculated && this.getCalculatedValues().getBankfullStage() != null )
        {
            stage = this.getCalculatedValues().getBankfullStage();
        }
        else
        {
            stage = this.getOriginalValues().getBankfullStage();
        }

        if ( stage == null )
        {
            return null;
        }

        return this.getStageUnitConversion( unitMapper ).applyAsDouble( stage );
    }

    /**
     * Gets the record flood flow.
     * @param getCalculated is true to return the calculated record flood flow, false for regular
     * @param unitMapper the unit mapper
     * @return the record flood flow
     */
    public Double getRecordFlow( boolean getCalculated, UnitMapper unitMapper )
    {
        Double flow;

        if ( getCalculated && this.getCalculatedValues().getRecordFlow() != null )
        {
            flow = this.getCalculatedValues().getRecordFlow();
        }
        else
        {
            flow = this.getOriginalValues().getRecordFlow();
        }

        if ( flow == null )
        {
            return null;
        }

        return this.getFlowUnitConversion( unitMapper ).applyAsDouble( flow );
    }

    /**
     * Gets the record flood stage.
     * @param getCalculated is true to return the calculated record flood stage, false for regular
     * @param unitMapper the unit mapper
     * @return the record flood stage
     */
    public Double getRecordStage( boolean getCalculated, UnitMapper unitMapper )
    {
        Double stage;

        if ( getCalculated && this.getCalculatedValues().getRecordStage() != null )
        {
            stage = this.getCalculatedValues().getRecordStage();
        }
        else
        {
            stage = this.getOriginalValues().getRecordStage();
        }

        if ( stage == null )
        {
            return null;
        }

        return this.getStageUnitConversion( unitMapper ).applyAsDouble( stage );
    }

    /**
     * @return the flow measurement unit
     */
    public MeasurementUnit getFlowMeasurementUnit()
    {
        return MeasurementUnit.of( this.getMetadata().getFlowUnit() );
    }

    /**
     * @return the stage measurement unit
     */
    public MeasurementUnit getStageMeasurementUnit()
    {
        return MeasurementUnit.of( this.getMetadata().getStageUnit() );
    }

    /**
     * The thresholds by feature.
     * @param thresholdType the threshold type
     * @param thresholdOperator the threshold operator
     * @param dataType the data type
     * @param getCalculated whether the thresholds are calculated
     * @param desiredUnitMapper the desired unit mapper
     * @return the thresholds
     */

    public Map<WrdsLocation, Set<Threshold>> getThresholds( WRDSThresholdType thresholdType,
                                                            ThresholdConstants.Operator thresholdOperator,
                                                            ThresholdConstants.ThresholdDataType dataType,
                                                            boolean getCalculated,
                                                            UnitMapper desiredUnitMapper
    )
    {
        WrdsLocation location = this.getLocation();

        Set<Threshold> thresholds;

        if ( thresholdType.equals( WRDSThresholdType.FLOW ) )
        {
            thresholds = this.getFlowThresholds( getCalculated, thresholdOperator, dataType, desiredUnitMapper );
        }
        else
        {
            thresholds = this.getStageThresholds( getCalculated, thresholdOperator, dataType, desiredUnitMapper );
        }
        return Map.of( location, thresholds );
    }

    private Set<Threshold> getFlowThresholds( boolean getCalculated,
                                              ThresholdConstants.Operator thresholdOperator,
                                              ThresholdConstants.ThresholdDataType dataType,
                                              UnitMapper desiredUnitMapper
    )
    {
        Set<Threshold> thresholds = new HashSet<>();

        Double low = this.getLowFlow( getCalculated, desiredUnitMapper );
        Double action = this.getActionFlow( getCalculated, desiredUnitMapper );
        Double minor = this.getMinorFlow( getCalculated, desiredUnitMapper );
        Double moderate = this.getModerateFlow( getCalculated, desiredUnitMapper );
        Double major = this.getMajorFlow( getCalculated, desiredUnitMapper );
        Double bankfull = this.getBankfulFlow( getCalculated, desiredUnitMapper );
        Double recordFlow = this.getRecordFlow( getCalculated, desiredUnitMapper );

        if ( low != null )
        {
            Threshold threshold = this.getValueThreshold( low,
                                                          thresholdOperator,
                                                          dataType,
                                                          "low",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( action != null )
        {
            Threshold threshold = this.getValueThreshold( action,
                                                          thresholdOperator,
                                                          dataType,
                                                          "action",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( minor != null )
        {
            Threshold threshold = this.getValueThreshold( minor,
                                                          thresholdOperator,
                                                          dataType,
                                                          "minor",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( moderate != null )
        {
            Threshold threshold = this.getValueThreshold( moderate,
                                                          thresholdOperator,
                                                          dataType,
                                                          "moderate",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( major != null )
        {
            Threshold threshold = this.getValueThreshold( major,
                                                          thresholdOperator,
                                                          dataType,
                                                          "major",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( bankfull != null )
        {
            Threshold threshold = this.getValueThreshold( bankfull,
                                                          thresholdOperator,
                                                          dataType,
                                                          "bankfull",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( recordFlow != null )
        {
            Threshold threshold = this.getValueThreshold( recordFlow,
                                                          thresholdOperator,
                                                          dataType,
                                                          "record",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        return thresholds;
    }

    private Set<Threshold> getStageThresholds(
            boolean getCalculated,
            ThresholdConstants.Operator thresholdOperator,
            ThresholdConstants.ThresholdDataType dataType,
            UnitMapper desiredUnitMapper
    )
    {
        Set<Threshold> thresholds = new HashSet<>();

        Double low = this.getLowStage( getCalculated, desiredUnitMapper );
        Double action = this.getActionStage( getCalculated, desiredUnitMapper );
        Double minor = this.getMinorStage( getCalculated, desiredUnitMapper );
        Double moderate = this.getModerateStage( getCalculated, desiredUnitMapper );
        Double major = this.getMajorStage( getCalculated, desiredUnitMapper );
        Double bankfull = this.getBankfulStage( getCalculated, desiredUnitMapper );
        Double recordStage = this.getRecordStage( getCalculated, desiredUnitMapper );

        if ( low != null )
        {
            Threshold threshold = this.getValueThreshold( low,
                                                          thresholdOperator,
                                                          dataType,
                                                          "low",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( action != null )
        {
            Threshold threshold = this.getValueThreshold( action,
                                                          thresholdOperator,
                                                          dataType,
                                                          "action",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( minor != null )
        {
            Threshold threshold = this.getValueThreshold( minor,
                                                          thresholdOperator,
                                                          dataType,
                                                          "minor",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( moderate != null )
        {
            Threshold threshold = this.getValueThreshold( moderate,
                                                          thresholdOperator,
                                                          dataType,
                                                          "moderate",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( major != null )
        {
            Threshold threshold = this.getValueThreshold( major,
                                                          thresholdOperator,
                                                          dataType,
                                                          "major",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( bankfull != null )
        {
            Threshold threshold = this.getValueThreshold( bankfull,
                                                          thresholdOperator,
                                                          dataType,
                                                          "bankfull",
                                                          desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        if ( recordStage != null )
        {
            Threshold threshold = this.getValueThreshold( recordStage,
                                                                       thresholdOperator,
                                                                       dataType,
                                                                       "record",
                                                                       desiredUnitMapper.getDesiredMeasurementUnitName() );
            thresholds.add( threshold );
        }

        return thresholds;
    }

    /**
     * Gets a stage unit converter.
     * @param mapper the mapper
     * @return the raw converter
     */

    private DoubleUnaryOperator getStageUnitConversion( UnitMapper mapper )
    {
        return mapper.getUnitMapper( this.getStageMeasurementUnit().getUnit() );
    }

    /**
     * Gets a flow unit converter.
     * @param mapper the mapper
     * @return the raw converter
     */

    private DoubleUnaryOperator getFlowUnitConversion( UnitMapper mapper )
    {
        return mapper.getUnitMapper( this.getFlowMeasurementUnit().getUnit() );
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
                                               ThresholdConstants.Operator thresholdOperator,
                                               ThresholdConstants.ThresholdDataType dataType,
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
