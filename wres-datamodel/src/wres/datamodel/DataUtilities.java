package wres.datamodel;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;

/**
 * Utilities for working with data/metadata objects.
 * 
 * @author James Brown
 */

public final class DataUtilities
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DataUtilities.class );

    private static final String ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING =
            "Enter non-null metadata to establish a path for writing.";

    /**
     * Returns a string representation of the {@link OneOrTwoThresholds} that contains only alphanumeric characters 
     * A-Z, a-z, and 0-9 and, additionally, the underscore character to separate between elements, and the period 
     * character as a decimal separator.
     * 
     * @param threshold the threshold, required
     * @return a safe string representation
     * @throws NullPointerException if the threshold is null
     */

    public static String toStringSafe( OneOrTwoThresholds threshold )
    {
        Objects.requireNonNull( threshold );

        if ( threshold.hasTwo() )
        {
            return DataUtilities.toStringSafe( threshold.first() )
                   + "_AND_"
                   + DataUtilities.toStringSafe( threshold.second() );
        }
        return DataUtilities.toStringSafe( threshold.first() );
    }

    /**
     * Returns a string representation of the {@link OneOrTwoThresholds} without any units. This is useful when forming 
     * string representations of a collection of {@link ThresholdOuter} and abstracting the common units to a higher 
     * level.
     * 
     * @param threshold the threshold, required
     * @return a string without any units
     * @throws NullPointerException if the threshold is null
     */

    public static String toStringWithoutUnits( OneOrTwoThresholds threshold )
    {
        Objects.requireNonNull( threshold );

        if ( threshold.hasTwo() )
        {
            return DataUtilities.toStringWithoutUnits( threshold.first() )
                   + " AND "
                   + DataUtilities.toStringWithoutUnits( threshold.second() );
        }
        return DataUtilities.toStringWithoutUnits( threshold.first() );
    }

    /**
     * Returns a string representation of the {@link ThresholdOuter} that contains only alphanumeric characters A-Z, a-z, 
     * and 0-9 and, additionally, the underscore character to separate between elements, and the period character as
     * a decimal separator.
     * 
     * @param threshold the threshold, required
     * @return a safe string representation
     * @throws NullPointerException if the threshold is null
     */

    public static String toStringSafe( ThresholdOuter threshold )
    {
        Objects.requireNonNull( threshold );

        String safe = threshold.toString();

        // Replace spaces and special characters: note the order of application matters
        safe = safe.replace( ">=", "GTE" );
        safe = safe.replace( "<=", "LTE" );
        safe = safe.replace( ">", "GT" );
        safe = safe.replace( "<", "LT" );
        safe = safe.replace( "=", "EQ" );
        safe = safe.replace( "Pr ", "Pr_" );
        safe = safe.replace( " ", "_" );
        safe = safe.replace( "[", "" );
        safe = safe.replace( "]", "" );
        safe = safe.replace( "(", "" );
        safe = safe.replace( ")", "" );

        // Any others, replace with empty
        safe = safe.replaceAll( "[^a-zA-Z0-9_.]", "" );

        return safe;
    }

    /**
     * Returns a string representation of the {@link ThresholdOuter} without any units. This is useful when forming 
     * string representations of a collection of {@link ThresholdOuter} and abstracting the common units to a higher 
     * level.
     * 
     * @param threshold the threshold, required
     * @return a string without any units
     * @throws NullPointerException if the threshold is null
     */

    public static String toStringWithoutUnits( ThresholdOuter threshold )
    {
        Objects.requireNonNull( threshold );

        if ( threshold.hasUnits() )
        {
            return threshold.toString()
                            .replace( " " + threshold.getUnits()
                                                     .toString(),
                                      "" );
        }

        return threshold.toString();
    }

    /**
     * Retrieves the specified number of fractional time units from the input duration. Accepted units include:
     * 
     * <ol>
     * <li>{@link ChronoUnit#DAYS}</li>
     * <li>{@link ChronoUnit#HOURS}</li>
     * <li>{@link ChronoUnit#MINUTES}</li>
     * <li>{@link ChronoUnit#SECONDS}</li>
     * <li>{@link ChronoUnit#MILLIS}</li>
     * </ol>
     *  
     * @param duration the duration
     * @param durationUnits the duration units required
     * @return the duration in the prescribed units
     * @throws IllegalArgumentException if the durationUnits is not one of the accepted units
     */
    public static Number durationToNumericUnits( Duration duration, ChronoUnit durationUnits )
    {
        // Get the duration in seconds plus nanos
        BigDecimal durationSeconds = BigDecimal.valueOf( duration.toSeconds() )
                                               .add( BigDecimal.valueOf( duration.get( ChronoUnit.NANOS ), 9 ) );

        // Divisor
        BigDecimal divisor = null;
        switch ( durationUnits )
        {
            case DAYS:
                divisor = BigDecimal.valueOf( 60.0 * 60.0 * 24.0 );
                break;
            case HOURS:
                divisor = BigDecimal.valueOf( 60.0 * 60.0 );
                break;
            case MINUTES:
                divisor = BigDecimal.valueOf( 60.0 );
                break;
            case SECONDS:
                divisor = BigDecimal.valueOf( 1.0 );
                break;
            case MILLIS:
                divisor = BigDecimal.valueOf( 1000.0 );
                break;
            default:
                throw new IllegalArgumentException( "The input time units '" + durationUnits
                                                    + "' are not supported "
                                                    + "in this context." );
        }

        double durationDouble = durationSeconds.divide( divisor, RoundingMode.HALF_UP )
                                               .doubleValue();

        // Use a long for a whole number
        if ( ( durationDouble == Math.floor( durationDouble ) ) && !Double.isInfinite( durationDouble ) )
        {
            return (long) durationDouble;
        }

        return durationDouble;
    }

    /**
     * Returns a path to write from the inputs.
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @param append an optional string to append
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getPathFromPoolMetadata( Path outputDirectory,
                                                PoolMetadata meta,
                                                TimeWindowOuter timeWindow,
                                                ChronoUnit leadUnits,
                                                MetricConstants metricName,
                                                MetricConstants metricComponentName,
                                                String append )
            throws IOException
    {
        Objects.requireNonNull( meta, DataUtilities.ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window to establish a path for writing." );

        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );

        String appendString = DataUtilities.durationToNumericUnits( timeWindow.getLatestLeadDuration(),
                                                                    leadUnits )
                              + "_"
                              + leadUnits.name().toUpperCase();

        if ( Objects.nonNull( append ) )
        {
            appendString = appendString + "_" + append;
        }

        return DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                      meta,
                                                      appendString,
                                                      metricName,
                                                      metricComponentName );
    }

    /**
     * Returns a path to write from the inputs.
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param threshold the threshold
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @param append an optional string to append
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getPathFromPoolMetadata( Path outputDirectory,
                                                PoolMetadata meta,
                                                OneOrTwoThresholds threshold,
                                                MetricConstants metricName,
                                                MetricConstants metricComponentName,
                                                String append )
            throws IOException
    {
        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );
        Objects.requireNonNull( threshold, "Enter non-null threshold to establish a path for writing." );

        String appendString = DataUtilities.toStringSafe( threshold );

        if ( Objects.nonNull( append ) )
        {
            appendString = appendString + "_" + append;
        }

        return DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                      meta,
                                                      appendString,
                                                      metricName,
                                                      metricComponentName );
    }

    /**
     * Returns a path to write from the inputs. 
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param append an optional string to append to the end of the path, may be null
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated 
     *            with the sample metadata
     * @throws IOException if the path cannot be produced
     * @throws ProjectConfigException when the destination configuration is invalid
     */

    public static Path getPathFromPoolMetadata( Path outputDirectory,
                                                PoolMetadata meta,
                                                String append,
                                                MetricConstants metricName,
                                                MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( metricName, "Specify a non-null metric name." );

        Pool pool = meta.getPool();

        // Build the path 
        StringJoiner joinElements = new StringJoiner( "_" );

        Evaluation evaluation = meta.getEvaluation();

        // Geographic name
        String geoName = DataUtilities.getGeographicName( pool );
        joinElements.add( geoName );

        // Dataset name
        String dataName = DataUtilities.getDatasetName( evaluation, pool );

        if ( !dataName.isBlank() )
        {
            joinElements.add( dataName );
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

        // Derive a sanitized name
        String safeName = URLEncoder.encode( joinElements.toString().replace( " ", "_" ), "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }

    /**
     * Returns a path to write from the inputs.
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getPathFromPoolMetadata( Path outputDirectory,
                                                PoolMetadata meta,
                                                MetricConstants metricName,
                                                MetricConstants metricComponentName )
            throws IOException
    {
        return DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                      meta,
                                                      (String) null,
                                                      metricName,
                                                      metricComponentName );
    }

    /**
     * Returns a geographic name for the pool.
     * @param pool the pool
     * @return a graphic name
     * @throws IllegalArgumentException if the pool has zero feature tuples or many tuples and no group name
     */

    private static String getGeographicName( Pool pool )
    {
        int geoCount = pool.getGeometryGroup()
                           .getGeometryTuplesCount();
        if ( geoCount == 0 )
        {
            throw new IllegalArgumentException( "Expected metadata with at least one feature tuple, but found none." );
        }

        if ( geoCount > 1 )
        {
            if ( "".equals( pool.getGeometryGroup()
                                .getRegionName() ) )
            {
                throw new IllegalArgumentException( "Discovered a pool with " + geoCount
                                                    + " features, but no region "
                                                    + "name that describes them, which is not allowed in this "
                                                    + "context." );
            }

            return pool.getGeometryGroup()
                       .getRegionName();
        }

        // Exactly one tuple
        GeometryTuple firstTuple = pool.getGeometryGroup()
                                       .getGeometryTuples( 0 );

        StringJoiner joiner = new StringJoiner( "_" );

        // Work-around to figure out if this is gridded data and if so to use
        // something other than the feature name, use the description.
        // When you make gridded benchmarks congruent, remove this.
        if ( firstTuple.getRight()
                       .getName()
                       .matches( "^-?[0-9]+\\.[0-9]+ -?[0-9]+\\.[0-9]+$" ) )
        {
            LOGGER.debug( "Using ugly workaround for ugly gridded benchmarks: {}",
                          firstTuple );
            joiner.add( firstTuple.getRight()
                                  .getDescription() );
        }
        else
        {
            LOGGER.debug( "Creating a geographic name from the single feature tuple, {}.", firstTuple );

            // Region name?
            if ( !"".equals( pool.getGeometryGroup()
                                 .getRegionName() ) )
            {
                joiner.add( pool.getGeometryGroup()
                                .getRegionName()
                                .replace( "-", "_" ) );
            }
            // No, use the first tuple instead
            else
            {
                joiner.add( firstTuple.getLeft()
                                      .getName() );
                joiner.add( firstTuple.getRight()
                                      .getName() );

                if ( firstTuple.hasBaseline() )
                {
                    joiner.add( firstTuple.getBaseline()
                                          .getName() );
                }
            }
        }

        return joiner.toString();
    }

    /**
     * Gets the name of a dataset from the evaluation and pool.
     * 
     * @param evaluation the evaluation
     * @param pool the pool
     * @return the dataset name
     */

    private static String getDatasetName( Evaluation evaluation, Pool pool )
    {
        String name = null;

        // Try to use the baseline data name if this pool is a baseline pool
        if ( pool.getIsBaselinePool() && !evaluation.getBaselineDataName().isBlank() )
        {
            name = evaluation.getBaselineDataName();

        }

        // Use the right name, which may be blank
        if ( Objects.isNull( name ) )
        {
            name = evaluation.getRightDataName();
        }

        // If both right and baseline have the same non-blank names, resolve this
        if ( evaluation.getBaselineDataName().equals( evaluation.getRightDataName() ) &&
             !evaluation.getRightDataName().isBlank() )
        {
            if ( pool.getIsBaselinePool() )
            {
                name = LeftOrRightOrBaseline.BASELINE.toString();
            }
            else
            {
                name = LeftOrRightOrBaseline.RIGHT.toString();
            }
        }

        return name;
    }

    /**
     * Prevent construction.
     */

    private DataUtilities()
    {
    }

}
