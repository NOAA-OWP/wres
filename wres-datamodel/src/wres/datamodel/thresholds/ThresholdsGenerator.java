package wres.datamodel.thresholds;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.config.MetricConfigException;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdsByMetric.Builder;

/**
 * Reads thresholds from project declaration.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ThresholdsGenerator
{

    /**
     * Reads the internally configured thresholds within a project declaration, if any.
     * 
     * @param projectConfig the project configuration
     * @return the thresholds, if any
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if the projectConfig is null
     */

    public static ThresholdsByMetric getThresholdsFromConfig( ProjectConfig projectConfig )
    {

        Objects.requireNonNull( projectConfig, "Specify a non-null project configuration." );

        // Find the units associated with the pairs
        MeasurementUnit units = null;
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            units = MeasurementUnit.of( projectConfig
                                                     .getPair()
                                                     .getUnit() );
        }

        // Builder 
        Builder builder = new Builder();

        // Iterate through the metric groups
        for ( MetricsConfig nextGroup : projectConfig.getMetrics() )
        {

            ThresholdsGenerator.addThresholdsToBuilderForOneMetricGroup( projectConfig,
                                                                         nextGroup,
                                                                         builder,
                                                                         units );
        }

        return builder.build();
    }

    /**
     * Adds thresholds to the input builder for one metric group.
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @param builder the builder
     * @param units the optional units for value thresholds
     * @throws MetricConfigException if the threshold configuration is incorrect
     */

    private static void addThresholdsToBuilderForOneMetricGroup( ProjectConfig projectConfig,
                                                                 MetricsConfig metricsConfig,
                                                                 Builder builder,
                                                                 MeasurementUnit units )
    {

        // Find the metrics
        Set<MetricConstants> metrics = DataFactory.getMetricsFromMetricsConfig( metricsConfig, projectConfig );

        // No explicit thresholds, add an "all data" threshold
        if ( metricsConfig.getThresholds().isEmpty() )
        {
            for ( MetricConstants next : metrics )
            {
                Set<ThresholdOuter> allData = ThresholdsGenerator.getAdjustedThresholds( next,
                                                                                         Collections.emptySet(),
                                                                                         ThresholdConstants.ThresholdGroup.VALUE );

                builder.addThresholds( Collections.singletonMap( next, allData ),
                                       ThresholdConstants.ThresholdGroup.VALUE );
            }
        }

        // Iterate through any explicit thresholds
        for ( ThresholdsConfig nextThresholds : metricsConfig.getThresholds() )
        {
            // Thresholds
            Set<ThresholdOuter> thresholds =
                    ThresholdsGenerator.getInternalThresholdsFromThresholdsConfig( nextThresholds, units );

            // Build the thresholds map per metric
            Map<MetricConstants, Set<ThresholdOuter>> thresholdsMap = new EnumMap<>( MetricConstants.class );

            // Type of thresholds
            ThresholdConstants.ThresholdGroup thresholdType = ThresholdConstants.ThresholdGroup.PROBABILITY;
            if ( Objects.nonNull( nextThresholds.getType() ) )
            {
                thresholdType = DataFactory.getThresholdGroup( nextThresholds.getType() );
            }

            // Adjust the thresholds, adding "all data" where required, then append
            for ( MetricConstants next : metrics )
            {
                thresholdsMap.put( next,
                                   ThresholdsGenerator.getAdjustedThresholds( next,
                                                                              thresholds,
                                                                              thresholdType ) );
            }

            // Add the thresholds
            builder.addThresholds( thresholdsMap, thresholdType );
        }
    }

    /**
     * Returns an adjusted set of thresholds for the input thresholds. Adjustments are made for the specific metric.
     * Notably, an "all data" threshold is added for metrics that support this, and thresholds are removed for metrics
     * that do not support them.
     * 
     * @param metric the metric
     * @param thresholds the raw thresholds
     * @param type the threshold type
     * @return the adjusted thresholds
     * @throws NullPointerException if either input is null
     */

    private static Set<ThresholdOuter> getAdjustedThresholds( MetricConstants metric,
                                                              Set<ThresholdOuter> thresholds,
                                                              ThresholdConstants.ThresholdGroup type )
    {
        Objects.requireNonNull( metric, "Specify a non-null metric." );

        Objects.requireNonNull( thresholds, "Specify non-null thresholds." );

        Objects.requireNonNull( type, "Specify a non-null threshold type." );

        if ( type == ThresholdConstants.ThresholdGroup.PROBABILITY_CLASSIFIER )
        {
            return thresholds;
        }

        Set<ThresholdOuter> returnMe = new HashSet<>();

        // All data only
        if ( ! metric.isAThresholdMetric() )
        {
            return Set.of( ThresholdOuter.ALL_DATA );
        }

        // Otherwise, add the input thresholds
        returnMe.addAll( thresholds );

        // Add all data for appropriate types
        if ( metric.isInGroup( SampleDataGroup.ENSEMBLE ) || metric.isInGroup( SampleDataGroup.SINGLE_VALUED )
             || metric.isInGroup( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            returnMe.add( ThresholdOuter.ALL_DATA );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Obtains a set of {@link ThresholdOuter} from a {@link ThresholdsConfig} for thresholds that are configured internally
     * and not sourced externally. In other words, {@link ThresholdsConfig#getCommaSeparatedValuesOrSource()} must 
     * return a string of thresholds and not a {@link ThresholdsConfig.Source}.
     * 
     * @param thresholds the thresholds configuration
     * @param units optional units for non-probability thresholds
     * @return a set of thresholds (possibly empty)
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if either input is null
     */

    private static Set<ThresholdOuter> getInternalThresholdsFromThresholdsConfig( ThresholdsConfig thresholds,
                                                                                  MeasurementUnit units )
    {
        Objects.requireNonNull( thresholds, "Specify non-null thresholds configuration." );

        Set<ThresholdOuter> returnMe = new HashSet<>();

        Operator operator = Operator.GREATER;

        // Operator specified
        if ( Objects.nonNull( thresholds.getOperator() ) )
        {
            operator = DataFactory.getThresholdOperator( thresholds );
        }

        ThresholdConstants.ThresholdDataType dataType = ThresholdConstants.ThresholdDataType.LEFT;

        // Operator specified
        if ( Objects.nonNull( thresholds.getApplyTo() ) )
        {
            dataType = DataFactory.getThresholdDataType( thresholds.getApplyTo() );
        }

        // Must be internally sourced: thresholds with global scope should be provided directly 
        Object values = thresholds.getCommaSeparatedValuesOrSource();

        // Default to ThresholdType.PROBABILITY
        ThresholdType type = ThresholdType.PROBABILITY;
        if ( Objects.nonNull( thresholds.getType() ) )
        {
            type = thresholds.getType();
        }

        // String = internal source
        if ( values instanceof String )
        {
            returnMe.addAll( ThresholdsGenerator.getThresholdsFromCommaSeparatedValues( values.toString(),
                                                                                        operator,
                                                                                        dataType,
                                                                                        type != ThresholdType.VALUE,
                                                                                        units ) );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a list of {@link ThresholdOuter} from a comma-separated string. Specify the type of {@link ThresholdOuter}
     * required.
     * 
     * @param inputString the comma-separated input string
     * @param oper the operator
     * @param dataType the data type to which the threshold applies
     * @param areProbs is true to generate probability thresholds, false for ordinary thresholds
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     * @throws MetricConfigException if the thresholds are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<ThresholdOuter> getThresholdsFromCommaSeparatedValues( String inputString,
                                                                              Operator oper,
                                                                              ThresholdConstants.ThresholdDataType dataType,
                                                                              boolean areProbs,
                                                                              MeasurementUnit units )
    {
        Objects.requireNonNull( inputString, "Specify a non-null input string." );

        Objects.requireNonNull( oper, "Specify a non-null operator." );

        Objects.requireNonNull( dataType, "Specify a non-null data type." );

        //Parse the double values
        List<Double> addMe =
                Arrays.stream( inputString.split( "," ) ).map( Double::parseDouble ).collect( Collectors.toList() );
        Set<ThresholdOuter> returnMe = new TreeSet<>();

        //Between operator
        if ( oper == Operator.BETWEEN )
        {
            if ( addMe.size() < 2 )
            {
                throw new MetricConfigException( "At least two values are required to compose a "
                                                 + "threshold that operates between a lower and an upper bound." );
            }
            for ( int i = 0; i < addMe.size() - 1; i++ )
            {
                if ( areProbs )
                {
                    returnMe.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( addMe.get( i ),
                                                                                             addMe.get( i
                                                                                                        + 1 ) ),
                                                                         oper,
                                                                         dataType,
                                                                         units ) );
                }
                else
                {
                    returnMe.add( ThresholdOuter.of( OneOrTwoDoubles.of( addMe.get( i ),
                                                                         addMe.get( i + 1 ) ),
                                                     oper,
                                                     dataType,
                                                     units ) );
                }
            }
        }
        //Other operators
        else
        {
            if ( areProbs )
            {
                addMe.forEach( threshold -> returnMe.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( threshold ),
                                                                                                 oper,
                                                                                                 dataType,
                                                                                                 units ) ) );
            }
            else
            {
                addMe.forEach( threshold -> returnMe.add( ThresholdOuter.of( OneOrTwoDoubles.of( threshold ),
                                                                             oper,
                                                                             dataType,
                                                                             units ) ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Do not construct.
     */

    private ThresholdsGenerator()
    {
    }

}
