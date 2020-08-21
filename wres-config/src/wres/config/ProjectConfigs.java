package wres.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.ProjectConfig.Inputs;

/**
 * Provides static methods that help with ProjectConfig and its children.
 */

public class ProjectConfigs
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectConfigs.class );

    /**
     * Returns <code>true</code> if the input configuration has time-series metrics, otherwise <code>false</code>.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration has time-series metrics, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasTimeSeriesMetrics( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project declaration." );

        return projectConfig.getMetrics().stream().anyMatch( next -> !next.getTimeSeriesMetric().isEmpty() );
    }

    /**
     * Compares the input instances of {@link ProjectConfig}. Returns a negative, zero, or positive value when the first
     * input is less than, equal to, or greater than the second input, respectively. This is a minimal implementation
     * that is consistent with {@link Object#equals(Object)} and otherwise compares the inputs according to the value
     * of the {@link ProjectConfig#getName()} alone.
     * 
     * @param first the first input
     * @param second the second input
     * @return a negative, zero or positive integer when the first input is less than, equal to, or greater than
     *            the second input
     * @throws NullPointerException if either input is null
     */

    public static int compare( ProjectConfig first, ProjectConfig second )
    {
        Objects.requireNonNull( first, "The first input is null, which is not allowed." );

        Objects.requireNonNull( second, "The second input is null, which is not allowed." );

        if ( first.equals( second ) )
        {
            return 0;
        }

        // Null friendly natural order on project name
        return Objects.compare( first.getName(), second.getName(), Comparator.nullsFirst( Comparator.naturalOrder() ) );
    }

    /**
     * Get a duration of a period from a timescale config
     * 
     * @param timeScaleConfig the config
     * @return the duration
     * @throws NullPointerException if the input is null or expected contents is null
     */

    public static Duration getDurationFromTimeScale( TimeScaleConfig timeScaleConfig )
    {
        Objects.requireNonNull( timeScaleConfig, "Specify non-null input configuration " );

        Objects.requireNonNull( timeScaleConfig.getUnit(),
                                "The unit associated with the time scale declaration was null, which is not allowed." );

        Objects.requireNonNull( timeScaleConfig.getPeriod(),
                                "The period associated with the time scale declaration was null, which is not allowed." );

        ChronoUnit unit = ChronoUnit.valueOf( timeScaleConfig.getUnit()
                                                             .value()
                                                             .toUpperCase() );
        return Duration.of( timeScaleConfig.getPeriod(), unit );
    }

    /**
     * <p>Returns the variable identifier from the inputs configuration. The identifier is one of the following in
     * order of precedent:</p>
     *
     * <p>If the variable identifier is required for the left and right:</p>
     * <ol>
     * <li>The label associated with the variable in the left source.</li>
     * <li>The label associated with the variable in the right source.</li>
     * <li>The value associated with the left variable.</li>
     * </ol>
     *
     * <p>If the variable identifier is required for the baseline:</p>
     * <ol>
     * <li>The label associated with the variable in the baseline source.</li>
     * <li>The value associated with the baseline variable.</li>
     * </ol>
     *
     * <p>In both cases, the last declaration is always present.</p>
     *
     * @param inputs the inputs configuration
     * @param isBaseline is true if the variable name is required for the baseline
     * @return the variable identifier
     * @throws IllegalArgumentException if the baseline variable is requested and the input does not contain
     *            a baseline source
     * @throws NullPointerException if the input is null
     */

    public static String getVariableIdFromProjectConfig( Inputs inputs, boolean isBaseline )
    {
        Objects.requireNonNull( inputs );

        // Baseline required?
        if ( isBaseline )
        {
            // Has a baseline source
            if ( Objects.nonNull( inputs.getBaseline() ) )
            {
                // Has a baseline source with a label
                if ( Objects.nonNull( inputs.getBaseline().getVariable().getLabel() ) )
                {
                    return inputs.getBaseline().getVariable().getLabel();
                }
                // Only has a baseline source with a variable value
                return inputs.getBaseline().getVariable().getValue();
            }
            throw new IllegalArgumentException( "Cannot identify the variable for the baseline as the input project "
                                                + "does not contain a baseline source." );
        }
        // Has a left source with a label
        if ( Objects.nonNull( inputs.getLeft().getVariable().getLabel() ) )
        {
            return inputs.getLeft().getVariable().getLabel();
        }
        // Has a right source with a label
        else if ( Objects.nonNull( inputs.getRight().getVariable().getLabel() ) )
        {
            return inputs.getRight().getVariable().getLabel();
        }
        // Has a left source with a variable value
        return inputs.getLeft().getVariable().getValue();
    }

    /**
     * <p>Returns the variable name from an {@link DataSourceConfig}. The identifier is one of the following in
     * order of precedent:</p>
    
     * <ol>
     * <li>The label associated with the variable.</li>
     * <li>The value associated with the variable.</li>
     * </ol>
     *
     * @param dataSourceConfig the data source configuration
     * @return the variable identifier
     * @throws NullPointerException if the input is null or the variable is undefined
     */

    public static String getVariableIdFromDataSourceConfig( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );
        Objects.requireNonNull( dataSourceConfig.getVariable() );

        if ( Objects.nonNull( dataSourceConfig.getVariable().getLabel() ) )
        {
            return dataSourceConfig.getVariable().getLabel();
        }

        return dataSourceConfig.getVariable().getValue();
    }

    private ProjectConfigs()
    {
        // Prevent construction, this is a static helper class.
    }

    /**
     * Get all the destinations from a configuration for a particular type.
     * @param config the config to search through
     * @param types the types to look for
     * @return a list of destinations with the type specified
     * @throws NullPointerException when config or type is null
     */

    public static List<DestinationConfig> getDestinationsOfType( ProjectConfig config,
                                                                 DestinationType... types )
    {
        Objects.requireNonNull( config, "Config must not be null." );
        Objects.requireNonNull( types, "Type must not be null." );

        List<DestinationConfig> result = new ArrayList<>();

        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return java.util.Collections.unmodifiableList( result );
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            for ( DestinationType nextType : types )
            {
                if ( d.getType() == nextType )
                {
                    result.add( d );
                }
            }
        }

        return java.util.Collections.unmodifiableList( result );
    }

    /**
     * Get all the graphical destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of graphical destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getGraphicalDestinations( ProjectConfig config )
    {
        return ProjectConfigs.getDestinationsOfType( config,
                                                     DestinationType.GRAPHIC,
                                                     DestinationType.PNG,
                                                     DestinationType.SVG );
    }

    /**
     * @return Returns <code>true</code> if the input type is a graphical type, else <code>false</code>.
     */

    public static boolean isGraphicsType( DestinationType destinationType )
    {
        return destinationType == DestinationType.GRAPHIC || destinationType == DestinationType.PNG
               || destinationType == DestinationType.SVG;
    }

    /**
     * Get all the numerical destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of numerical destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getNumericalDestinations( ProjectConfig config )
    {
        return ProjectConfigs.getDestinationsOfType( config, DestinationType.NUMERIC );
    }

    /**
     * Returns the first instance of the named metric configuration or null if no such configuration exists.
     * 
     * @param projectConfig the project configuration
     * @param metricName the metric name
     * @return the named metric configuration or null
     * @throws NullPointerException if one or both of the inputs are null
     */

    public static MetricConfig getMetricConfigByName( ProjectConfig projectConfig, MetricConfigName metricName )
    {
        Objects.requireNonNull( projectConfig, "Specify a non-null metric configuration as input." );
        Objects.requireNonNull( metricName, "Specify a non-null metric name as input." );

        for ( MetricsConfig next : projectConfig.getMetrics() )
        {
            Optional<MetricConfig> nextConfig =
                    next.getMetric().stream().filter( metric -> metric.getName().equals( metricName ) ).findFirst();
            if ( nextConfig.isPresent() )
            {
                return nextConfig.get();
            }
        }

        return null;
    }

}

