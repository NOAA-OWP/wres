package wres.datamodel.thresholds;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.config.xml.MetricConfigException;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.OneOrTwoDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.datamodel.pools.MeasurementUnit;

/**
 * Reads thresholds from project declaration.
 *
 * @author James Brown
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

    public static Set<ThresholdOuter> getThresholdsFromConfig( ProjectConfig projectConfig )
    {

        Objects.requireNonNull( projectConfig, "Specify a non-null project configuration." );

        // Find the units associated with the pairs
        MeasurementUnit units = null;
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            units = MeasurementUnit.of( projectConfig.getPair()
                                                     .getUnit() );
        }

        Set<ThresholdOuter> thresholds = new TreeSet<>();

        // Add an all data threshold by default
        thresholds.add( ThresholdOuter.ALL_DATA );

        // Iterate through the metric groups
        MeasurementUnit finalUnits = units;
        for ( MetricsConfig nextGroup : projectConfig.getMetrics() )
        {
            Set<ThresholdOuter> nextThresholds
                    = nextGroup.getThresholds()
                               .stream()
                               .flatMap( next -> ThresholdsGenerator.getThresholdsFromThresholdsConfig( next,
                                                                                                        finalUnits )
                                                                    .stream() )
                               .collect( Collectors.toSet() );
            thresholds.addAll( nextThresholds );
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Obtains a set of {@link ThresholdOuter} from a {@link ThresholdsConfig} for thresholds that are configured
     * internally and not sourced externally. In other words,
     * {@link ThresholdsConfig#getCommaSeparatedValuesOrSource()} must return a string of thresholds and not a
     * {@link ThresholdsConfig.Source}.
     *
     * @param thresholds the thresholds configuration
     * @param units optional units for non-probability thresholds
     * @return a set of thresholds (possibly empty)
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if either input is null
     */

    public static Set<ThresholdOuter> getThresholdsFromThresholdsConfig( ThresholdsConfig thresholds,
                                                                         MeasurementUnit units )
    {
        Objects.requireNonNull( thresholds, "Specify non-null thresholds configuration." );

        Set<ThresholdOuter> returnMe = new HashSet<>();

        // Add an all data threshold by default
        returnMe.add( ThresholdOuter.ALL_DATA );

        wres.config.yaml.components.ThresholdOperator operator = wres.config.yaml.components.ThresholdOperator.GREATER;

        // Operator specified
        if ( Objects.nonNull( thresholds.getOperator() ) )
        {
            operator = ThresholdsGenerator.getThresholdOperator( thresholds );
        }

        ThresholdOrientation dataType = ThresholdOrientation.LEFT;

        // Operator specified
        if ( Objects.nonNull( thresholds.getApplyTo() ) )
        {
            dataType = ThresholdsGenerator.getThresholdDataType( thresholds.getApplyTo() );
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
                                                                                        type,
                                                                                        units ) );
        }

        return Collections.unmodifiableSet( returnMe );
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

    public static Set<ThresholdOuter> getAdjustedThresholds( MetricConstants metric,
                                                             Set<ThresholdOuter> thresholds,
                                                             wres.config.yaml.components.ThresholdType type )
    {
        Objects.requireNonNull( metric, "Specify a non-null metric." );

        Objects.requireNonNull( thresholds, "Specify non-null thresholds." );

        Objects.requireNonNull( type, "Specify a non-null threshold type." );

        if ( type == wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
        {
            return thresholds;
        }

        // All data only
        if ( !metric.isAThresholdMetric() )
        {
            return Set.of( ThresholdOuter.ALL_DATA );
        }

        // Otherwise, add the input thresholds
        Set<ThresholdOuter> returnMe = new HashSet<>( thresholds );

        // Add all data for appropriate types
        if ( metric.isInGroup( SampleDataGroup.ENSEMBLE ) || metric.isInGroup( SampleDataGroup.SINGLE_VALUED )
             || metric.isInGroup( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            returnMe.add( ThresholdOuter.ALL_DATA );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * <p>Maps between threshold operators in {@link ThresholdOperator} and those in {@link wres.config.yaml.components.ThresholdOperator}.
     *
     * <p>TODO: make these enumerations match on name to reduce brittleness.
     *
     * @param thresholdsConfig the threshold configuration
     * @return the mapped operator
     * @throws MetricConfigException if the operator is not mapped
     * @throws NullPointerException if the input is null or the {@link ThresholdsConfig#getOperator()} returns null
     */

    public static wres.config.yaml.components.ThresholdOperator getThresholdOperator( ThresholdsConfig thresholdsConfig )
    {
        Objects.requireNonNull( thresholdsConfig );
        Objects.requireNonNull( thresholdsConfig.getOperator() );

        return switch ( thresholdsConfig.getOperator() )
                {
                    case EQUAL_TO -> wres.config.yaml.components.ThresholdOperator.EQUAL;
                    case LESS_THAN -> wres.config.yaml.components.ThresholdOperator.LESS;
                    case GREATER_THAN -> wres.config.yaml.components.ThresholdOperator.GREATER;
                    case LESS_THAN_OR_EQUAL_TO -> wres.config.yaml.components.ThresholdOperator.LESS_EQUAL;
                    case GREATER_THAN_OR_EQUAL_TO -> wres.config.yaml.components.ThresholdOperator.GREATER_EQUAL;
                };
    }

    /**
     * Returns the {@link wres.config.yaml.components.ThresholdType} that corresponds to the {@link ThresholdType}
     * associated with the input configuration. Matches the enumerations by {@link Enum#name()}.
     *
     * @param thresholdType the threshold type
     * @return the mapped threshold group
     * @throws IllegalArgumentException if the threshold group is not mapped
     * @throws NullPointerException if the input is null
     */

    public static wres.config.yaml.components.ThresholdType getThresholdGroup( ThresholdType thresholdType )
    {
        Objects.requireNonNull( thresholdType );

        return wres.config.yaml.components.ThresholdType.valueOf( thresholdType.name() );
    }

    /**
     * Returns the {@link ThresholdOrientation} that corresponds to the {@link ThresholdDataType}
     * associated with the input configuration. Matches the enumerations by {@link Enum#name()}.
     *
     * @param thresholdDataType the threshold data type
     * @return the mapped threshold data type
     * @throws IllegalArgumentException if the data type is not mapped
     * @throws NullPointerException if the input is null
     */

    public static ThresholdOrientation getThresholdDataType( ThresholdDataType thresholdDataType )
    {
        Objects.requireNonNull( thresholdDataType );

        return ThresholdOrientation.valueOf( thresholdDataType.name() );
    }

    /**
     * Returns a list of {@link ThresholdOuter} from a comma-separated string. Specify the type of {@link ThresholdOuter}
     * required.
     *
     * @param inputString the comma-separated input string
     * @param oper the operator
     * @param dataType the data type to which the threshold applies
     * @param thresholdType the threshold type
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     * @throws MetricConfigException if the thresholds are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<ThresholdOuter> getThresholdsFromCommaSeparatedValues( String inputString,
                                                                              wres.config.yaml.components.ThresholdOperator oper,
                                                                              ThresholdOrientation dataType,
                                                                              ThresholdType thresholdType,
                                                                              MeasurementUnit units )
    {
        Objects.requireNonNull( inputString, "Specify a non-null input string." );
        Objects.requireNonNull( oper, "Specify a non-null operator." );
        Objects.requireNonNull( dataType, "Specify a non-null data type." );

        // Parse the double values
        List<Double> addMe =
                Arrays.stream( inputString.split( "," ) )
                      .map( Double::parseDouble )
                      .toList();

        // Between operator
        if ( oper == wres.config.yaml.components.ThresholdOperator.BETWEEN )
        {
            return ThresholdsGenerator.getBetweenThresholds( addMe, oper, dataType, thresholdType, units );
        }
        // Other operators
        else
        {
            return ThresholdsGenerator.getThresholds( addMe, oper, dataType, thresholdType, units );
        }
    }

    /**
     * Creates a set of thresholds with a between condition from the inputs.
     * @param values the threshold values
     * @param oper the operator
     * @param dataType the data type to which the threshold applies
     * @param thresholdType the threshold type
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     */

    private static Set<ThresholdOuter> getBetweenThresholds( List<Double> values,
                                                             wres.config.yaml.components.ThresholdOperator oper,
                                                             ThresholdOrientation dataType,
                                                             ThresholdType thresholdType,
                                                             MeasurementUnit units )
    {
        Set<ThresholdOuter> returnMe = new TreeSet<>();
        if ( values.size() < 2 )
        {
            throw new MetricConfigException( "At least two values are required to compose a "
                                             + "threshold that operates between a lower and an upper bound." );
        }
        for ( int i = 0; i < values.size() - 1; i++ )
        {
            ThresholdOuter.Builder builder = new ThresholdOuter.Builder()
                    .setOperator( oper )
                    .setOrientation( dataType )
                    .setUnits( units )
                    .setThresholdType( wres.config.yaml.components.ThresholdType.valueOf( thresholdType.name() ) );
            if ( thresholdType == ThresholdType.VALUE )
            {
                builder.setValues( OneOrTwoDoubles.of( values.get( i ),
                                                       values.get( i
                                                                   + 1 ) ) );
            }
            else
            {
                builder.setProbabilities( OneOrTwoDoubles.of( values.get( i ),
                                                              values.get( i
                                                                          + 1 ) ) );
            }
            returnMe.add( builder.build() );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Creates a set of thresholds from the inputs.
     * @param values the threshold values
     * @param oper the operator
     * @param dataType the data type to which the threshold applies
     * @param thresholdType the threshold type
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     */

    private static Set<ThresholdOuter> getThresholds( List<Double> values,
                                                      wres.config.yaml.components.ThresholdOperator oper,
                                                      ThresholdOrientation dataType,
                                                      ThresholdType thresholdType,
                                                      MeasurementUnit units )
    {
        Set<ThresholdOuter> returnMe = new TreeSet<>();
        for ( Double value : values )
        {
            ThresholdOuter.Builder builder = new ThresholdOuter.Builder()
                    .setOperator( oper )
                    .setOrientation( dataType )
                    .setUnits( units )
                    .setThresholdType( wres.config.yaml.components.ThresholdType.valueOf( thresholdType.name() ) );

            if ( thresholdType == ThresholdType.VALUE )
            {
                builder.setValues( OneOrTwoDoubles.of( value ) );
            }
            else
            {
                builder.setProbabilities( OneOrTwoDoubles.of( value ) );
            }
            returnMe.add( builder.build() );
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
