package wres.engine.statistics.metric.config;

import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.ThresholdOperator;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold.Operator;

/**
 * A helper class for interpreting and using the {@link ProjectConfig} in the context of verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */

public final class MetricConfigHelper
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
            throw new MetricConfigurationException( "Unable to map a null input identifier to a named metric." );
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
     * Returns the metric data input type from the {@link ProjectConfig}.
     * 
     * @param config the {@link ProjectConfig}
     * @return the {@link MetricInputGroup} based on the {@link ProjectConfig}
     * @throws MetricConfigurationException if the input type is not recognized
     */
    
    public static MetricInputGroup getInputType( ProjectConfig config ) throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to generate metrics." );
        DatasourceType type = config.getInputs().getRight().getType();
        switch ( type )
        {
            case ENSEMBLE_FORECASTS:
                return MetricInputGroup.ENSEMBLE;
            case SINGLE_VALUED_FORECASTS:
            case SIMULATIONS:
                return MetricInputGroup.SINGLE_VALUED;
            default:
                throw new MetricConfigurationException( "Unable to interpret the input type '" + type
                                                        + "' when attempting to process the metrics " );
        }
    }

    /**
     * <p>Returns a list of all supported metrics given the input {@link ProjectConfig}. Specifically, checks the 
     * {@link ProjectConfig} for the data type of the right-side and for any thresholds, returning metrics as 
     * follows:</p>
     * <ol>
     * <li>If the right side contains {@link DatasourceType#ENSEMBLE_FORECASTS} and thresholds are defined: returns
     * all metrics that consume {@link MetricInputGroup#ENSEMBLE}, {@link MetricInputGroup#SINGLE_VALUED} and
     * {@link MetricInputGroup#DISCRETE_PROBABILITY}</li>
     * <li>If the right side contains {@link DatasourceType#ENSEMBLE_FORECASTS} and thresholds are not defined: returns
     * all metrics that consume {@link MetricInputGroup#ENSEMBLE} and {@link MetricInputGroup#SINGLE_VALUED}</li>
     * <li>If the right side contains {@link DatasourceType#SINGLE_VALUED_FORECASTS} and thresholds are defined: returns
     * all metrics that consume {@link MetricInputGroup#SINGLE_VALUED} and {@link MetricInputGroup#DICHOTOMOUS}</li>
     * <li>If the right side contains {@link DatasourceType#SINGLE_VALUED_FORECASTS} and thresholds are not defined: 
     * returns all metrics that consume {@link MetricInputGroup#SINGLE_VALUED}.</li>
     * </ol>
     * 
     * TODO: implement multicategory metrics.
     * @param config the {@link ProjectConfig}
     * @return a list of all metrics that are compatible with the project configuration  
     * @throws MetricConfigurationException if the configuration is invalid
     */
    
    public static Set<MetricConstants> getAllValidMetricsFromConfig( ProjectConfig config )
            throws MetricConfigurationException
    {
        Set<MetricConstants> returnMe;
        MetricInputGroup group = getInputType( config );
        switch ( group )
        {
            case ENSEMBLE:
                returnMe = MetricConfigHelper.getMetricsForEnsembleInput( config );
                break;
            case SINGLE_VALUED:
                returnMe = MetricConfigHelper.getMetricsForSingleValuedInput( config );
                break;
            default:
                throw new MetricConfigurationException( "Unexpected input identifier '" + group + "'." );
        }
        //Remove CRPSS if no baseline is available
        DataSourceConfig baseline = config.getInputs().getBaseline();
        if ( Objects.isNull( baseline ) )
        {
            returnMe.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        }
        //Disallow non-score metrics when pooling window configuration is present, until this 
        //is supported
        PoolingWindowConfig windows = config.getPair().getIssuedDatesPoolingWindow();
        if ( Objects.nonNull( windows ) )
        {
            returnMe.removeIf( a -> ! ( a.isInGroup( MetricOutputGroup.SCORE )
                                        || a.isInGroup( MetricOutputGroup.VECTOR ) ) );
        }
        return returnMe;
    }

    /**
     * Returns a set of {@link MetricConstants} from a {@link ProjectConfig}. If the {@link ProjectConfig} contains
     * the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics are returned that are consistent
     * with the configuration. 
     * 
     * TODO: consider interpreting configured metrics in combination with {@link MetricConfigName#ALL_VALID} as 
     * overrides to be removed from the {@link MetricConfigName#ALL_VALID} metrics.
     * 
     * @param config the project configuration
     * @return a set of {@link MetricConstants}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */
    
    public static Set<MetricConstants> getMetricsFromConfig( ProjectConfig config ) throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to generate metrics." );
        //Obtain the list of metrics
        List<MetricConfigName> metricsConfig = config.getMetrics()
                                                     .getMetric()
                                                     .stream()
                                                     .map( MetricConfig::getName )
                                                     .collect( Collectors.toList() );
        Set<MetricConstants> metrics = new TreeSet<>();
        //All valid metrics
        if ( metricsConfig.contains( MetricConfigName.ALL_VALID ) )
        {
            metrics = getAllValidMetricsFromConfig( config );
        }
        //Explicitly configured metrics
        else
        {
            for ( MetricConfigName metric : metricsConfig )
            {
                metrics.add( from( metric ) );
            }
        }
        return metrics;
    }

    /**
     * Returns true if the input {@link Outputs} has thresholds configured, false otherwise.
     * 
     * @param metrics the {@link MetricsConfig} configuration
     * @return true if the project configuration has thresholds configured, false otherwise
     */
    
    public static boolean hasThresholds( MetricsConfig metrics )
    {
        //Global thresholds
        if ( Objects.nonNull( metrics.getProbabilityThresholds() )
             || Objects.nonNull( metrics.getValueThresholds() ) )
        {
            return true;
        }
        //Local thresholds
        for ( MetricConfig metric : metrics.getMetric() )
        {
            if ( Objects.nonNull( metric.getProbabilityThresholds() )
                 || Objects.nonNull( metric.getValueThresholds() ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#ENSEMBLE}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#ENSEMBLE}
     */
    
    private static Set<MetricConstants> getMetricsForEnsembleInput( ProjectConfig config )
    {
        Set<MetricConstants> returnMe = new TreeSet<>();
        returnMe.addAll( MetricInputGroup.ENSEMBLE.getMetrics() );
        returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );
        if ( hasThresholds( config.getMetrics() ) )
        {
            returnMe.addAll( MetricInputGroup.DISCRETE_PROBABILITY.getMetrics() );
        }
        return returnMe;
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#SINGLE_VALUED}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#SINGLE_VALUED}
     */
    
    private static Set<MetricConstants> getMetricsForSingleValuedInput( ProjectConfig config )
    {
        Set<MetricConstants> returnMe = new TreeSet<>();
        returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );
        if ( hasThresholds( config.getMetrics() ) )
        {
            returnMe.addAll( MetricInputGroup.DICHOTOMOUS.getMetrics() );
        }
        return returnMe;
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

    private MetricConfigHelper()
    {
    }    

}
