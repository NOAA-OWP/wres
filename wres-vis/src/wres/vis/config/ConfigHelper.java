package wres.vis.config;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;
import wres.util.TimeHelper;

/**
 * For help with project declaration.
 */

public class ConfigHelper
{
    
    private static final String ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING =
            "Enter non-null metadata to establish a path for writing.";
    
    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigHelper.class );
    
    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link SampleMetadata}
     * associated with the results and a {@link TimeWindowOuter}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             TimeWindowOuter timeWindow,
                                             ChronoUnit leadUnits,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig, "Enter non-null time window to establish a path for writing." );

        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );

        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );

        return ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                  destinationConfig,
                                                  meta,
                                                  TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                                  leadUnits )
                                                        + "_"
                                                        + leadUnits.name().toUpperCase(),
                                                  metricName,
                                                  metricComponentName );
    }

    /**
     * Returns a path to write from a combination of the destination configuration, the input metadata and any 
     * additional string that should be appended to the path (e.g. lead time or threshold). 
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param append an optional string to append to the end of the path, may be null
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated 
     *            with the sample metadata
     * @throws IOException if the path cannot be produced
     * @throws ProjectConfigException when the destination configuration is invalid
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             String append,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig,
                                "Enter non-null destination configuration to establish "
                                                   + "a path for writing." );

        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( meta.getIdentifier(),
                                "Enter a non-null identifier for the metadata to establish "
                                                      + "a path for writing." );

        Objects.requireNonNull( metricName, "Specify a non-null metric name." );

        // Build the path 
        StringJoiner joinElements = new StringJoiner( "_" );
        DatasetIdentifier identifier = meta.getIdentifier();
        Evaluation evaluation = meta.getEvaluation();
        Pool pool = meta.getPool();

        // Work-around to figure out if this is gridded data and if so to use
        // something other than the feature name, use the description.
        // When you make gridded benchmarks congruent, remove this.
        if ( identifier.getFeatureTuple()
                       .getRight()
                       .getName()
                       .matches( "^-?[0-9]+\\.[0-9]+ -?[0-9]+\\.[0-9]+$" ) )
        {
            LOGGER.debug( "Using ugly workaround for ugly gridded benchmarks: {}",
                          identifier );
            joinElements.add( identifier.getFeatureTuple()
                                        .getRight()
                                        .getDescription() );
        }
        else
        {
            joinElements.add( identifier.getFeatureTuple()
                                        .getRight()
                                        .getName() );
        }

        joinElements.add( identifier.getVariableName() );


        // Baseline scenarioId
        String configuredScenarioId = null;
        String configuredBaselineScenarioId = null;
        if ( !evaluation.getRightDataName().isBlank() )
        {
            configuredScenarioId = evaluation.getRightDataName();
        }

        if ( !evaluation.getBaselineDataName().isBlank() )
        {
            configuredBaselineScenarioId = evaluation.getBaselineDataName();
        }

        // Add optional scenario identifier unless the configured identifiers cannot discriminate between 
        // RIGHT and BASELINE 
        if ( identifier.hasScenarioName() && !Objects.equals( configuredScenarioId, configuredBaselineScenarioId ) )
        {
            joinElements.add( identifier.getScenarioName() );
        }
        // If there are metrics for both the RIGHT and BASELINE, then additionally qualify the context
        else if ( identifier.hasLeftOrRightOrBaseline()
                  && pool.getIsBaselinePool() )
        {
            joinElements.add( identifier.getLeftOrRightOrBaseline().toString() );
        }

        // Add the metric name
        joinElements.add( metricName.name() );

        // Add a non-default component name
        if ( Objects.nonNull( metricComponentName ) && MetricConstants.MAIN != metricComponentName )
        {
            joinElements.add( metricComponentName.name() );
        }

        // Add optional append
        if ( Objects.nonNull( append ) )
        {
            joinElements.add( append );
        }

        // Add extension
        String extension;

        // Default graphic extension type
        if ( destinationConfig.getType() == DestinationType.GRAPHIC )
        {
            extension = ".png";
        }
        // Default numeric extension type
        else if ( destinationConfig.getType() == DestinationType.NUMERIC )
        {
            extension = ".csv";
        }
        // Specific type
        else
        {
            extension = destinationConfig.getType().name().toLowerCase();
        }

        // Derive a sanitized name
        String safeName = URLEncoder.encode( joinElements.toString().replace( " ", "_" ) + extension, "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }
    
    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link SampleMetadata}
     * associated with the results and a {@link OneOrTwoThresholds}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param threshold the threshold
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             OneOrTwoThresholds threshold,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( threshold, "Enter non-null threshold to establish a path for writing." );

        return getOutputPathToWrite( outputDirectory,
                                     destinationConfig,
                                     meta,
                                     threshold.toStringSafe(),
                                     metricName,
                                     metricComponentName );
    }
    
    /**
     * Returns a path to write from a combination of the {@link DestinationConfig} and the {@link SampleMetadata}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        return ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                  destinationConfig,
                                                  meta,
                                                  (String) null,
                                                  metricName,
                                                  metricComponentName );
    }
    
    /**Do not construct*/
    private ConfigHelper()
    {       
    }
    
}
