package wres.engine.statistics.metric;

import java.util.EnumMap;
import java.util.Objects;

import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdOperator;
import wres.datamodel.MetricConstants;
import wres.datamodel.Threshold.Operator;

/**
 * A helper class that maps constructs in {@link ProjectConfig}, including enumerations. In particular, maps between 
 * named metrics in {@link MetricConstants} and those with a corresponding {@link Enum#name()} in 
 * {@link MetricConfigName}. Also maps between operators in {@link ThresholdOperator} and those in {@link Operator}.
 * 
 * TODO: consider moving this class to wres.datamodel and requiring the wres.datamodel to be aware of wres.config.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class ConfigMapper
{

    /**
     * Map between names
     */

    private static final EnumMap<MetricConfigName, MetricConstants> NAME_MAP = new EnumMap<>( MetricConfigName.class );

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} or null if the input is
     * {@link MetricConfigName#ALL_VALID}. Throws an exception if no such mapping is available. 
     * 
     * @param configName the name in the {@link MetricConfigName}
     * @return the corresponding name in the {@link MetricConstants}
     * @throws MetricConfigurationException if the configName is not mapped or the input is null
     */

    public static MetricConstants from( MetricConfigName configName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( configName ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a name metric." );
        }
        buildMap();
        //All valid metrics
        if ( configName.equals( MetricConfigName.ALL_VALID ) )
        {
            return null;
        }
        if ( !NAME_MAP.containsKey( configName ) )
        {
            throw new MetricConfigurationException( " Unable to find a metric with a configured identifier of "
                                                    + "'" + configName + "'." );
        }
        return NAME_MAP.get( configName );
    }

    /**
     * Maps between threshold operators in {@link ThresholdOperator} and those in {@link Operator}.
     * 
     * @param configName the input {@link ThresholdOperator}
     * @return the corresponding {@link Operator}.
     * @throws MetricConfigurationException if the configName is not mapped or the input is null
     */

    public static Operator from( ThresholdOperator configName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( configName ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a name operator." );
        }
        switch ( configName )
        {
            case LESS_THAN:
                return Operator.LESS;
            case GREATER_THAN:
                return Operator.GREATER;
            case LESS_THAN_OR_EQUAL_TO:
                return Operator.LESS_EQUAL;
            case GREATER_THAN_OR_EQUAL_TO:
                return Operator.GREATER_EQUAL;
            default:
                throw new MetricConfigurationException( "Unrecognized threshold operator in project configuration '"
                                                        + configName + "'." );
        }
    }    
    
    /**
     * Builds the mapping between the {@link MetricConstants} and the {@link MetricConfigName} 
     */

    private static void buildMap()
    {
        //Lazy population
        if ( NAME_MAP.isEmpty() )
        {
            //Match on name
            for ( MetricConfigName nextConfig : MetricConfigName.values() )
            {
                for ( MetricConstants nextMetric : MetricConstants.values() )
                {
                    if ( nextConfig.name().equals( nextMetric.name() ) )
                    {
                        NAME_MAP.put( nextConfig, nextMetric );
                        break;
                    }
                }
            }
        }
    }

    /**
     * Hidden constructor.
     */

    private ConfigMapper()
    {
    }

}
