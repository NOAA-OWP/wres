package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;

import org.apache.qpid.server.model.preferences.Preference;

/**
 * Represents the combined elements that defined an atomic set of thresholds
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeneralThresholdDefinition implements Serializable {
    /**
     * Metadata describing where the values are valid and who produced them
     */
    GeneralThresholdMetadata metadata;
    
    /**
     * The stage values presented by the all-in-one NWS API.
     */
    GeneralThresholdValues stage_values;
    
    
    /**
     * The flow values presented by the all-in-one NWS API.
     */
    GeneralThresholdValues flow_values;
    
    
    /**
     * The calculated flow values presented by the all-in-one NWS API.
     */
    GeneralThresholdValues calc_flow_values;
    
    
    /**
     * The general values presented by other APIs, including the NWS
     * threshold API when stage or flow is specifically requested.
     */
    GeneralThresholdValues values;
    

    public GeneralThresholdMetadata getMetadata()
    {
        return metadata;
    }

    public void setMetadata( GeneralThresholdMetadata metadata )
    {
        this.metadata = metadata;
    }

    public GeneralThresholdValues getStage_values()
    {
        return stage_values;
    }

    public void setStage_values( GeneralThresholdValues stage_values )
    {
        this.stage_values = stage_values;
    }

    public GeneralThresholdValues getFlow_values()
    {
        return flow_values;
    }

    public void setFlow_values( GeneralThresholdValues flow_values )
    {
        this.flow_values = flow_values;
    }

    public GeneralThresholdValues getCalc_flow_values()
    {
        return calc_flow_values;
    }

    public void setCalc_flow_values( GeneralThresholdValues calc_flow_values )
    {
        this.calc_flow_values = calc_flow_values;
    }

    public GeneralThresholdValues getValues()
    {
        return values;
    }

    public void setValues( GeneralThresholdValues values )
    {
        this.values = values;
    }

    public String getThresholdProvider() {
        String thresholdProvider = null;
        if( Objects.nonNull( this.getMetadata() ) )
        {
            thresholdProvider = this.getMetadata().getThreshold_source();
        }
        
        return thresholdProvider;
    }

    public WrdsLocation getLocation() {
        return new WrdsLocation(
                this.metadata.getNwm_feature_id(),
                this.metadata.getUsgs_site_code(),
                this.metadata.getNws_lid(),
                null
        );
    }
    
    public String getRatingProvider() 
    {
        String ratingProvider = null;
        if( Objects.nonNull( this.getCalc_flow_values() ) )
        {
            
            ratingProvider = this.getCalc_flow_values().getRating_curve().getSource();
        }
        
        return ratingProvider;
    }

    
    /**
     * 
     * @param thresholdType
     * @param thresholdOperator
     * @param dataType
     * @param getCalculated If true, then calculated will be used for flow thresholds if available; 
     * otherwise the original flow thresholds are used.  This flag is ignored for all other thresholds.
     * @param desiredUnitMapper
     * @return
     */
    public Map<WrdsLocation, Set<ThresholdOuter>> getThresholds(
            WRDSThresholdType thresholdType,
            ThresholdConstants.Operator thresholdOperator,
            ThresholdConstants.ThresholdDataType dataType,
            boolean getCalculated,
            UnitMapper desiredUnitMapper
    )
    {
        WrdsLocation location = this.getLocation();
        Set<ThresholdOuter> thresholds = new HashSet<>();
        Map<String, ThresholdOuter> thresholdMap = new HashMap<>();
        
        //If either of these is null, then it is not used. 
        //Be sure to check for null before attempting to use.
        Map<String, Double> calculatedThresholds = null;
        Map<String, Double> originalThresholds = null;
        
        //These are the measurement unit operators based on a desired unit,
        //passed in, and the String units associated with the threshold.
        DoubleUnaryOperator originalUnitConversionOperator = null;
        DoubleUnaryOperator calculatedUnitConversionOperator = null;
       
        //TODO We might want to add additional protections using fields
        //in metadata to make sure the thresholdType and units make sense.
        
        //Point the two maps and identify the unit operator appropriately.
        //Unified schema values takes precedence over all others.
        if (getValues() != null && !getValues().getThresholdValues().isEmpty())
        {
            originalThresholds = getValues().getThresholdValues();
            originalUnitConversionOperator = desiredUnitMapper.getUnitMapper(MeasurementUnit.of(this.getMetadata().getUnits()).getUnit());
        }
        
        //When values is not used, then we are looking at NWS thresholds,
        //which come with stage, flow, and calculated flow options. Seelct
        //based on provided trheshold type.
        else if (thresholdType.equals(WRDSThresholdType.STAGE))
        {
            if (getStage_values() != null)
            {
                originalThresholds = getStage_values().getThresholdValues();
                originalUnitConversionOperator = desiredUnitMapper.getUnitMapper(MeasurementUnit.of(this.getMetadata().getStage_units()).getUnit());
            }
        }
        else 
        {
            if (getFlow_values() != null)
            {
                originalThresholds = getFlow_values().getThresholdValues();
                originalUnitConversionOperator = desiredUnitMapper.getUnitMapper(MeasurementUnit.of(this.getMetadata().getFlow_units()).getUnit());
            }
            
            if (getCalculated)
            {
                if (getCalc_flow_values() != null)
                {
                    calculatedThresholds = getCalc_flow_values().getThresholdValues();
                    calculatedUnitConversionOperator = desiredUnitMapper.getUnitMapper(MeasurementUnit.of(this.getMetadata().getCalc_flow_units()).getUnit());
                }
            }
        }
        
        //First, the original thresholds go into the map.
        if (originalThresholds != null)
        {
            for (Entry<String, Double> threshold : originalThresholds.entrySet())
            {
                if (threshold.getValue() != null)
                {
                    double value = originalUnitConversionOperator.applyAsDouble(threshold.getValue());
                    thresholdMap.put(threshold.getKey(),
                        ThresholdOuter.of(
                            OneOrTwoDoubles.of(value),
                            thresholdOperator,
                            dataType,
                            threshold.getKey(),
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                        )
                    );
                }
            }
        }
        
        //Then we overwrite the original with calculated.
        if (calculatedThresholds != null)
        {
            for (Entry<String, Double> threshold : calculatedThresholds.entrySet())
            {
                if (threshold.getValue() != null)
                {
                    double value = calculatedUnitConversionOperator.applyAsDouble(threshold.getValue());
                    thresholdMap.put(threshold.getKey(),
                        ThresholdOuter.of(
                            OneOrTwoDoubles.of(value),
                            thresholdOperator,
                            dataType,
                            threshold.getKey(),
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                        )
                    );
                }
            }
        }
        
        return Map.of( location, new HashSet<>(thresholdMap.values()) );
    }

}