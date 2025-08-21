package wres.datamodel;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.components.DatasetOrientation;
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
    private static final String MAXDURATION = "MAXDURATION";
    private static final String MINDURATION = "MINDURATION";

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
        safe = safe.replace( "&", "AND" );

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
     * Returns a safe string representation of an {@link Instant} for use in a path to a web resource.
     * @param instant the instant
     * @return the safe string representation
     * @throws NullPointerException if the instant is null
     */

    public static String toStringSafe( Instant instant )
    {
        Objects.requireNonNull( instant );

        String baseString = instant.toString();

        return baseString.replace( Instant.MIN.toString(), "MINDATE" )
                         .replace( Instant.MAX.toString(), "MAXDATE" )
                         .replace( "+", "" )
                         .replace( "-", "" )
                         .replace( ":", "" );
    }

    /**
     * Returns a safe and user-friendly string representation of an {@link Duration} for use in a path to a web 
     * resource.
     * @param duration the duration
     * @param units the duration units
     * @return the safe string representation
     * @throws NullPointerException if either input is null
     */

    public static String toStringSafe( Duration duration, ChronoUnit units )
    {
        Objects.requireNonNull( duration );
        Objects.requireNonNull( units );

        if ( duration.equals( TimeWindowOuter.DURATION_MIN ) )
        {
            return MINDURATION;
        }
        else if ( duration.equals( TimeWindowOuter.DURATION_MAX ) )
        {
            return MAXDURATION;
        }

        return DataUtilities.durationToNumericUnits( duration, units )
                            .toString();
    }

    /**
     * Returns a safe and user-friendly string representation of a {@link TimeWindowOuter} for use in a path to a web 
     * resource.
     * @param timeWindow the timeWindow
     * @param units the lead duration units
     * @return the safe string representation
     * @throws NullPointerException if either input is null
     */

    public static String toStringSafe( TimeWindowOuter timeWindow, ChronoUnit units )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( units );

        return DataUtilities.toStringSafeDateTimesOnly( timeWindow )
               + "_"
               + DataUtilities.toStringSafeLeadDurationsOnly( timeWindow, units );
    }

    /**
     * Returns a safe and user-friendly string representation of the date-times in a {@link TimeWindowOuter} for use
     * in a path to a web resource.
     * @param timeWindow the timeWindow
     * @return the safe string representation
     * @throws NullPointerException if either input is null
     */

    public static String toStringSafeDateTimesOnly( TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( timeWindow );

        return DataUtilities.toStringSafe( timeWindow.getEarliestReferenceTime() )
               + "_TO_"
               + DataUtilities.toStringSafe( timeWindow.getLatestReferenceTime() )
               + "_"
               + DataUtilities.toStringSafe( timeWindow.getEarliestValidTime() )
               + "_TO_"
               + DataUtilities.toStringSafe( timeWindow.getLatestValidTime() );
    }

    /**
     * Returns a safe and user-friendly string representation of the lead durations within a {@link TimeWindowOuter}
     * for use in a path to a web resource.
     * @param timeWindow the timeWindow
     * @param units the lead duration units
     * @return the safe string representation
     * @throws NullPointerException if either input is null
     */

    public static String toStringSafeLeadDurationsOnly( TimeWindowOuter timeWindow, ChronoUnit units )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( units );
        String baseString;
        if ( Objects.equals( timeWindow.getEarliestLeadDuration(), timeWindow.getLatestLeadDuration() ) )
        {
            baseString = DataUtilities.toStringSafe( timeWindow.getEarliestLeadDuration(), units );
        }
        else
        {
            baseString = DataUtilities.toStringSafe( timeWindow.getEarliestLeadDuration(), units )
                         + "_TO_"
                         + DataUtilities.toStringSafe( timeWindow.getLatestLeadDuration(), units );
        }

        if ( !baseString.endsWith( MAXDURATION ) )
        {
            baseString += "_"
                          + units.name()
                                 .toUpperCase();
        }

        return baseString;
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
        BigDecimal divisor = switch ( durationUnits )
        {
            case DAYS -> BigDecimal.valueOf( 60.0 * 60.0 * 24.0 );
            case HOURS -> BigDecimal.valueOf( 60.0 * 60.0 );
            case MINUTES -> BigDecimal.valueOf( 60.0 );
            case SECONDS -> BigDecimal.valueOf( 1.0 );
            case MILLIS -> BigDecimal.valueOf( 1000.0 );
            default -> throw new IllegalArgumentException( "The input time units '" + durationUnits
                                                           + "' are not supported "
                                                           + "in this context." );
        };

        double durationDouble = durationSeconds.divide( divisor, RoundingMode.HALF_UP )
                                               .doubleValue();

        // Use a long for a whole number
        if ( ( durationDouble == Math.floor( durationDouble ) ) && !Double.isInfinite( durationDouble ) )
        {
            return ( long ) durationDouble;
        }

        return durationDouble;
    }

    /**
     * Takes a raw string and removes reserved characters, encodes, and replaces " " with "_".
     * Removes any non-alphanumeric symbols except for "-"
     *
     * @param rawString the potentially unsafe file string
     * @return sanitized string
     */
    public static String sanitizeFileName( String rawString )
    {
        String noSpaceString = rawString.replace( " ", "_" );
        String sanitized = noSpaceString.replaceAll( "[^a-zA-Z0-9\\-_.]", "" );
        return URLEncoder.encode( sanitized, StandardCharsets.UTF_8 );
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
     */

    public static Path getPathFromPoolMetadata( Path outputDirectory,
                                                PoolMetadata meta,
                                                TimeWindowOuter timeWindow,
                                                ChronoUnit leadUnits,
                                                MetricConstants metricName,
                                                MetricConstants metricComponentName,
                                                String append )
    {
        Objects.requireNonNull( meta, DataUtilities.ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window to establish a path for writing." );

        Objects.requireNonNull( leadUnits, "Enter a non-null time unit for the lead durations to establish a "
                                           + "path for writing." );

        // This is not fully qualified, but making it so will be a breaking change. It will need to be fully qualified
        // when arbitrary pools are supported: see #86646. At that time, use the time window safe string helpers in 
        // this class.
        String appendString = DataUtilities.toStringSafe( timeWindow.getLatestLeadDuration(), leadUnits );

        if ( !appendString.endsWith( MAXDURATION ) )
        {
            appendString += "_"
                            + leadUnits.name()
                                       .toUpperCase();
        }

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
     * <p>Returns a path to write from the inputs using a limited subset of metadata, notably:
     * <ol>
     *    <li>{@link DataUtilities#getGeographicName(Pool)};</li>
     *    <li>{@link DataUtilities#getDatasetName(Evaluation, Pool)};</li>
     *    <li>The {@link MetricConstants#name()} of the metric;</li>
     *    <li>The {@link MetricConstants#name()} of the metric component, unless it is {@link MetricConstants#MAIN}; and</li>
     *    <li>The supplied append string.</li>
     * </ol>
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param append an optional string to append to the end of the path, may be null
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated 
     *            with the sample metadata
     */

    public static Path getPathFromPoolMetadata( Path outputDirectory,
                                                PoolMetadata meta,
                                                String append,
                                                MetricConstants metricName,
                                                MetricConstants metricComponentName )
    {
        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );
        Objects.requireNonNull( metricName, "Specify a non-null metric name." );

        Pool pool = meta.getPoolDescription();

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
        if ( Objects.nonNull( metricComponentName )
             && MetricConstants.MAIN != metricComponentName )
        {
            joinElements.add( metricComponentName.name() );
        }

        // Add optional append
        if ( Objects.nonNull( append )
             && !append.isBlank() )
        {
            joinElements.add( append );
        }

        // Derive a sanitized name
        String rawString = joinElements.toString();
        return Paths.get( outputDirectory.toUri() )
                    .resolve( DataUtilities.sanitizeFileName( rawString ) );
    }

    /**
     * Returns a path to write from the inputs.
     *
     * @see #getPathFromPoolMetadata(Path, PoolMetadata, String, MetricConstants, MetricConstants)
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     */

    public static Path getPathFromPoolMetadata( Path outputDirectory,
                                                PoolMetadata meta,
                                                MetricConstants metricName,
                                                MetricConstants metricComponentName )
    {
        return DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                      meta,
                                                      null,
                                                      metricName,
                                                      metricComponentName );
    }

    /**
     * Return x,y from a wkt with a POINT in it.
     * As of 2020-06-30 only used for gridded evaluation.
     * @param wkt A well-known text geometry with POINT
     * @return point with x,y doubles parsed
     * @throws IllegalArgumentException when parsing fails
     */

    public static Coordinate getLonLatFromPointWkt( String wkt )
    {
        String wktUpperCase = wkt.strip()
                                 .toUpperCase();

        WKTReader reader = new WKTReader();

        try
        {
            Geometry geometry = reader.read( wktUpperCase );
            return geometry.getCoordinate();
        }
        catch ( ParseException e )
        {
            throw new IllegalArgumentException( "Failed to read a coordinate pair from this wkt: "
                                                + wkt
                                                + "." );
        }
    }

    /**
     * Return x,y from a wkt with a POINT in it.
     * A less strict version of the above method, returns null if anything goes
     * wrong. The above strict version is needed for gridded evaluation whereas
     * this one will return null when it cannot get a GeoPoitn out of the wkt.
     * @param wkt a well-known text geometry that may have POINT
     * @return point with x,y doubles parsed, null when anything goes wrong
     */

    public static Coordinate getLonLatOrNullFromWkt( String wkt )
    {
        try
        {
            return DataUtilities.getLonLatFromPointWkt( wkt );
        }
        catch ( IllegalArgumentException e )
        {
            LOGGER.debug( "Failed to read a coordinate pair from this wkt: {}.", wkt );
            return null;
        }
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
            if ( pool.getGeometryGroup()
                     .getRegionName()
                     .isEmpty() )
            {
                throw new IllegalArgumentException( "Discovered a pool with " + geoCount
                                                    + " features, but no region "
                                                    + "name that describes them, which is not allowed in this "
                                                    + "context." );
            }

            return pool.getGeometryGroup()
                       .getRegionName()
                       .replace( "-", "_" );
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
                       .matches( "^-?\\d+\\.\\d+ -?\\d+\\.\\d+$" ) )
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
            if ( !pool.getGeometryGroup()
                      .getRegionName()
                      .isEmpty() )
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

        // Try to use the baseline data name if this pool is a baseline pool. Always assign a name to a baseline pool
        if ( pool.getIsBaselinePool() )
        {
            if ( !evaluation.getBaselineDataName()
                            .isBlank() )
            {
                name = evaluation.getBaselineDataName();
            }
            else
            {
                name = DatasetOrientation.BASELINE.name();
            }
        }

        // Use the right name if required, which may be blank
        if ( Objects.isNull( name ) )
        {
            name = evaluation.getRightDataName();
        }

        // If both right and baseline have the same non-blank names, resolve this
        if ( evaluation.getBaselineDataName()
                       .equals( evaluation.getRightDataName() )
             && !evaluation.getRightDataName()
                           .isBlank() )
        {
            if ( pool.getIsBaselinePool() )
            {
                name = DatasetOrientation.BASELINE.name();
            }
            else
            {
                name = DatasetOrientation.RIGHT.name();
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
