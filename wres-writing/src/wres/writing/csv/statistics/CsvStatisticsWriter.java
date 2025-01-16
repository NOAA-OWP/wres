package wres.writing.csv.statistics;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.config.MetricConstants.SampleDataGroup;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.BoxplotStatistic.Box;
import wres.statistics.generated.Covariate;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Writes statistics to a file that contains comma separated values (CSV). There is one CSV file per evaluation and  
 * there should be one instance of this writer per evaluation.
 *
 * @author James Brown
 */

@ThreadSafe
public class CsvStatisticsWriter implements Function<Statistics, Set<Path>>, Closeable
{
    /** A default name for the pairs. */
    public static final String DEFAULT_FILE_NAME = "evaluation.csv.gz";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( CsvStatisticsWriter.class );

    /** The file header. */
    private static final String HEADER = "LEFT VARIABLE NAME,RIGHT VARIABLE NAME,BASELINE VARIABLE NAME,COVARIATE "
                                         + "FILTERS,POOL NUMBER,EVALUATION SUBJECT,FEATURE GROUP NAME,LEFT FEATURE "
                                         + "NAME,LEFT FEATURE WKT,LEFT FEATURE SRID,LEFT FEATURE DESCRIPTION,RIGHT "
                                         + "FEATURE NAME,RIGHT FEATURE WKT,RIGHT FEATURE SRID,RIGHT FEATURE "
                                         + "DESCRIPTION,BASELINE FEATURE NAME,BASELINE FEATURE WKT,BASELINE FEATURE "
                                         + "SRID,BASELINE FEATURE DESCRIPTION,EARLIEST ISSUED TIME EXCLUSIVE,LATEST "
                                         + "ISSUED TIME INCLUSIVE,EARLIEST VALID TIME EXCLUSIVE,LATEST VALID TIME "
                                         + "INCLUSIVE,EARLIEST LEAD DURATION EXCLUSIVE,LATEST LEAD DURATION INCLUSIVE,"
                                         + "TIME SCALE DURATION,TIME SCALE FUNCTION,TIME SCALE START MONTH-DAY "
                                         + "INCLUSIVE,TIME SCALE END MONTH-DAY INCLUSIVE,EVENT THRESHOLD NAME,EVENT "
                                         + "THRESHOLD LOWER VALUE,EVENT THRESHOLD UPPER VALUE,EVENT THRESHOLD UNITS,"
                                         + "EVENT THRESHOLD LOWER PROBABILITY,EVENT THRESHOLD UPPER PROBABILITY,EVENT "
                                         + "THRESHOLD SIDE,EVENT THRESHOLD OPERATOR,DECISION THRESHOLD NAME,DECISION "
                                         + "THRESHOLD LOWER VALUE,DECISION THRESHOLD UPPER VALUE,DECISION THRESHOLD "
                                         + "UNITS,DECISION THRESHOLD LOWER PROBABILITY,DECISION THRESHOLD UPPER "
                                         + "PROBABILITY,DECISION THRESHOLD SIDE,DECISION THRESHOLD OPERATOR,METRIC "
                                         + "NAME,METRIC COMPONENT NAME,METRIC COMPONENT QUALIFIER,METRIC COMPONENT "
                                         + "UNITS,METRIC COMPONENT MINIMUM,METRIC COMPONENT MAXIMUM,METRIC COMPONENT "
                                         + "OPTIMUM,STATISTIC GROUP NUMBER,SUMMARY STATISTIC NAME,SUMMARY STATISTIC "
                                         + "COMPONENT NAME,SUMMARY STATISTIC UNITS,SUMMARY STATISTIC DIMENSION,"
                                         + "SUMMARY STATISTIC QUANTILE,SAMPLE QUANTILE,STATISTIC";

    /** The CSV delimiter. */
    private static final String DELIMITER = ",";

    /** A delimiter for items within a list. */
    private static final String LIST_DELIMITER = ":";

    /** Platform-dependent line separator as a byte array. */
    private static final byte[] LINE_SEPARATOR = System.lineSeparator()
                                                       .getBytes( StandardCharsets.UTF_8 );

    /** Repeated string. */
    private static final String PROBABILITY = "PROBABILITY";

    /** Lock for writing csv to the {@link #path} for which this writer is built. */
    private final ReentrantLock writeLock;

    /** The evaluation description. */
    private final StringJoiner evaluationDescription;

    /** Buffered writer to share, must be closed on completion. */
    private final BufferedOutputStream bufferedWriter;

    /** Duration units. */
    private final ChronoUnit durationUnits;

    /** Decimal formatter. */
    private final DoubleFunction<String> decimalFormatter;

    /** Nanoseconds per {@link #durationUnits}. */
    private final BigDecimal nanosPerDuration;

    /** Path to write. */
    @GuardedBy( "writeLock" )
    private final Path path;

    /** Path to the supplementary CSVT file created for applications that use the GDAL geospatial library. */
    private final Path pathToCsvt;

    /** Whether the header has been written. */
    private final AtomicBoolean headerWritten = new AtomicBoolean();

    /**
     * Returns an instance, which writes to the prescribed path. Uses default value formatting, which means durations in
     * units of seconds and {@link String#valueOf(double)} for real values.
     *
     * @param evaluation the evaluation description
     * @param path the path to write
     * @param gzip is true to gzip the output
     * @return a writer instance
     * @throws NullPointerException if any input is null
     * @throws CommaSeparatedWriteException if the writer could not be created or the header written
     */

    public static CsvStatisticsWriter of( Evaluation evaluation,
                                          Path path,
                                          boolean gzip )
    {
        return CsvStatisticsWriter.of( evaluation, path, gzip, ChronoUnit.SECONDS, String::valueOf );
    }

    /**
     * Returns an instance, which writes to the prescribed path. Uses default value formatting, which means durations in
     * units of seconds and {@link String#valueOf(double)} for real values.
     *
     * @param evaluation the evaluation description
     * @param path the path to write
     * @param gzip is true to gzip the output
     * @param durationUnits the duration units
     * @param decimalFormatter the decimal formatter
     * @return a writer instance
     * @throws NullPointerException if any input is null
     * @throws CommaSeparatedWriteException if the writer could not be created or the header written
     */

    public static CsvStatisticsWriter of( Evaluation evaluation,
                                          Path path,
                                          boolean gzip,
                                          ChronoUnit durationUnits,
                                          DoubleFunction<String> decimalFormatter )
    {
        return new CsvStatisticsWriter( evaluation, path, gzip, durationUnits, decimalFormatter );
    }

    /**
     * Writes a blob of statistics to a CSV file.
     *
     * @throws CommaSeparatedWriteException if the statistics could not be written
     * @throws NullPointerException if the statistics are null
     */

    @Override
    public Set<Path> apply( Statistics statistics )
    {
        Objects.requireNonNull( statistics );

        LOGGER.debug( "Writer {} received a packet of statistics, which will be written to {}.", this, this.getPath() );

        // Lock here so that all statistics for one pool appear in the same place within the file. The pool numbering 
        // depends on this. If moving down the call chain to an individual write, then pool numbers would need to 
        // appear in the statistics themselves.
        ReentrantLock lock = this.getWriteLock();

        lock.lock();

        // There is only one thread per pool write, but it is convenient to increment in this form
        AtomicInteger groupNumber = new AtomicInteger( 1 );

        try
        {
            // Create the CSV file if not already created
            this.testCreateCsvFile( this.bufferedWriter );
            this.writeStatistics( statistics, this.bufferedWriter, groupNumber );
        }
        catch ( IOException e )
        {
            throw new CommaSeparatedWriteException( "Encountered an error while writing a blob of statistic to a CSV "
                                                    + "file.", e );
        }
        finally
        {
            lock.unlock();
        }

        return Set.of( this.path, this.pathToCsvt );
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing the CSV statistics writer." );

        if ( Objects.nonNull( this.bufferedWriter ) )
        {
            this.bufferedWriter.close();
        }
    }

    /**
     * Returns the path to the CSV file.
     *
     * @return the path to write.
     */

    private Path getPath()
    {
        return this.path;
    }

    /**
     * Returns the lock to use when writing CSV.
     *
     * @return the write lock
     */

    private ReentrantLock getWriteLock()
    {
        return this.writeLock;
    }

    /**
     * Returns a pool description from the pool definitions.
     *
     * @return a pool description
     * @throws IOException if the pool description cannot be generated
     */

    private StringJoiner getPoolDescription( Pool pool ) throws IOException
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        // Pool number
        joiner.add( String.valueOf( pool.getPoolId() ) );

        // Subject of the evaluation
        if ( pool.getIsBaselinePool() )
        {
            joiner.add( "BASELINE" );
        }
        else
        {
            joiner.add( "RIGHT" );
        }

        // Merge in geometry description
        GeometryGroup geoGroup = pool.getGeometryGroup();
        String featureGroupName = geoGroup.getRegionName();
        List<GeometryTuple> geometries = geoGroup.getGeometryTuplesList();
        StringJoiner geometryDescription = this.getGeometryTupleDescription( geometries, featureGroupName );
        joiner = joiner.merge( geometryDescription );

        // Merge in time window description
        StringJoiner timeWindowDescription = this.getTimeWindowDescription( pool.getTimeWindow() );
        joiner = joiner.merge( timeWindowDescription );

        // Merge in the timescale description
        if ( pool.hasTimeScale() )
        {
            StringJoiner timeScaleDescription = this.getTimeScaleDescription( pool.getTimeScale() );
            joiner = joiner.merge( timeScaleDescription );
        }
        // Add placeholders
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 4 );
        }

        // Merge in each threshold
        if ( pool.hasEventThreshold() )
        {
            StringJoiner threshold = this.getThresholdDescription( pool.getEventThreshold() );
            joiner = joiner.merge( threshold );
        }
        // Add placeholders
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 8 );
        }

        if ( pool.hasDecisionThreshold() )
        {
            StringJoiner threshold = this.getThresholdDescription( pool.getDecisionThreshold() );
            joiner = joiner.merge( threshold );
        }
        // Add placeholders
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 8 );
        }

        return joiner;
    }

    /**
     * Returns a geometry description from the input.
     *
     * @param geometries the geometries
     * @param featureGroupName the feature group name
     * @return the geometry description
     * @throws IOException if there are no geometries or more than one srid
     */

    private StringJoiner getGeometryTupleDescription( List<GeometryTuple> geometries,
                                                      String featureGroupName )
            throws IOException
    {
        if ( geometries.isEmpty() )
        {
            throw new IOException( "Expected at least one geometry but found none while processing "
                                   + "feature group "
                                   + featureGroupName
                                   + "." );
        }

        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        this.append( joiner, featureGroupName, true );

        // Left
        List<Geometry> left = geometries.stream()
                                        .map( GeometryTuple::getLeft )
                                        .toList();

        StringJoiner leftJoiner = this.getGeometryDescription( left, DatasetOrientation.LEFT, featureGroupName );

        joiner = joiner.merge( leftJoiner );

        // Right
        List<Geometry> right = geometries.stream()
                                         .map( GeometryTuple::getRight )
                                         .toList();

        StringJoiner rightJoiner = this.getGeometryDescription( right, DatasetOrientation.RIGHT, featureGroupName );

        joiner = joiner.merge( rightJoiner );


        // Baseline
        GeometryTuple first = geometries.get( 0 );
        if ( first.hasBaseline() )
        {
            // Right
            List<Geometry> baseline = geometries.stream()
                                                .map( GeometryTuple::getBaseline )
                                                .toList();

            StringJoiner baselineJoiner =
                    this.getGeometryDescription( baseline, DatasetOrientation.BASELINE, featureGroupName );

            joiner = joiner.merge( baselineJoiner );
        }
        // Add placeholders
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 4 );
        }

        return joiner;
    }

    /**
     * Returns a geometry description from the list of geometries.
     *
     * @param geometries the geometries
     * @param lrb the left or right or baseline context to help with messaging
     * @param groupName to help with messaging
     * @return the geometry description
     * @throws IllegalArgumentException if there are no geometries or more than one srid
     */

    private StringJoiner getGeometryDescription( List<Geometry> geometries,
                                                 DatasetOrientation lrb,
                                                 String groupName )
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        if ( geometries.isEmpty() )
        {
            throw new IllegalArgumentException( "Expected at least one geometry but found none while processing "
                                                + "feature group "
                                                + groupName
                                                + "." );
        }

        // Single srid expected
        Set<Integer> srids = geometries.stream()
                                       .mapToInt( Geometry::getSrid )
                                       .boxed()
                                       .collect( Collectors.toSet() );

        if ( srids.size() > 1 )
        {
            throw new IllegalArgumentException( "The csv2 format does not support a feature group whose features "
                                                + "contain more than one Spatial Reference Identifier (SRID). While "
                                                + "writing statistics for the "
                                                + lrb
                                                + " side of feature group "
                                                + groupName
                                                + ", discovered "
                                                + srids.size()
                                                + " SRIDs as follows: "
                                                + srids
                                                + "." );
        }

        // SRID of 0 means no SRID
        String srid = Integer.toString( srids.iterator().next() );

        List<String> wkts = geometries.stream()
                                      .map( Geometry::getWkt )
                                      .filter( next -> !next.isEmpty() )
                                      .toList();

        // Compose the names with a delimiter
        StringJoiner names = new StringJoiner( LIST_DELIMITER );
        geometries.stream()
                  .map( Geometry::getName )
                  .forEach( names::add );

        this.append( joiner, names.toString(), true );

        if ( !wkts.isEmpty() )
        {
            String multipartWkt = this.getMultiPartWktFromSinglePartWkts( wkts );
            this.append( joiner, multipartWkt, true );
            this.append( joiner, srid, false );
        }
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 2 );
        }

        // Compose any descriptions with a delimiter
        StringJoiner description = new StringJoiner( LIST_DELIMITER );
        geometries.stream()
                  .map( Geometry::getDescription )
                  .forEach( description::add );

        this.append( joiner, description.toString(), true );

        return joiner;
    }

    /**
     * Attempts to create a multi-part geometry from single-part geometries. TODO: consider using a geospatial library
     * for this. A limitation of this method as currently written is that it attempts to aggregate the geometries into
     * multi-part geometries, rather than simply collect them. Unfortunately, qgis and probably other tools do not 
     * currently support geometry collections, only multi-part geometries. If they ever do, consider upgrading this 
     * method as JTS and WKT both support a geometry collection and it is a much better modeling choice. Currently, 
     * if this method receives multiple polygons, for example, it will form the union of their point geometries.
     *
     * @param wkts the wkts
     * @return the multipart geometry
     * @throws IllegalArgumentException if the multipart geometry could not be constructed for whatever reason
     */

    private String getMultiPartWktFromSinglePartWkts( List<String> wkts )
    {
        if ( wkts.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot build a multi-part geometry from an empty collection." );
        }

        WKTReader reader = new WKTReader();
        org.locationtech.jts.geom.Geometry unionGeometry = null;

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Writing these WKT strings to the CSV2: {}.", wkts );
        }

        for ( String wkt : wkts )
        {
            try
            {
                org.locationtech.jts.geom.Geometry nextGeometry = reader.read( wkt );
                if ( Objects.isNull( unionGeometry ) )
                {
                    unionGeometry = nextGeometry;
                }
                else
                {
                    unionGeometry = unionGeometry.union( nextGeometry );
                }
            }
            catch ( ParseException e )
            {
                throw new IllegalArgumentException( "Failed to parse wkt " + wkt
                                                    + " into a geometry for aggregation." );
            }
        }

        WKTWriter writer = new WKTWriter();
        return writer.write( unionGeometry );
    }

    /**
     * Returns a time window description from the input.
     *
     * @param timeWindow the time window
     * @return the time window description
     */

    private StringJoiner getTimeWindowDescription( TimeWindow timeWindow )
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        // Use a more helpful api
        TimeWindowOuter outer = TimeWindowOuter.of( timeWindow );

        joiner.add( outer.getEarliestReferenceTime().toString() )
              .add( outer.getLatestReferenceTime().toString() )
              .add( outer.getEarliestValidTime().toString() )
              .add( outer.getLatestValidTime().toString() )
              .add( outer.getEarliestLeadDuration().toString() )
              .add( outer.getLatestLeadDuration().toString() );

        return joiner;
    }

    /**
     * Returns a time scale description from the input.
     *
     * @param timeScale the time scale
     * @return the time scale description
     */

    private StringJoiner getTimeScaleDescription( TimeScale timeScale )
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        // Use a more helpful api
        TimeScaleOuter outer = TimeScaleOuter.of( timeScale );

        if ( timeScale.hasPeriod() )
        {
            joiner.add( outer.getPeriod().toString() );
        }
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }

        joiner.add( outer.getFunction().toString() );

        if ( Objects.nonNull( outer.getStartMonthDay() ) )
        {
            joiner.add( outer.getStartMonthDay()
                             .toString()
                             .replace( "--", "" ) );
        }
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }

        if ( Objects.nonNull( outer.getEndMonthDay() ) )
        {
            joiner.add( outer.getEndMonthDay()
                             .toString()
                             .replace( "--", "" ) );
        }
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }

        return joiner;
    }

    /**
     * Returns a threshold description from the input.
     *
     * @param threshold the threshold
     * @return the threshold description
     */

    private StringJoiner getThresholdDescription( Threshold threshold )
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        // Use a more helpful api
        ThresholdOuter outer = ThresholdOuter.of( threshold );

        joiner.add( outer.getLabel() );

        // Real values?
        if ( outer.hasValues() )
        {
            OneOrTwoDoubles doubles = outer.getValues();
            String left = String.valueOf( doubles.first() );
            joiner.add( left );

            if ( doubles.hasTwo() )
            {
                String right = String.valueOf( doubles.second() );
                joiner.add( right );
            }
            // Placeholder
            else
            {
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );
            }

            // Units
            if ( outer.hasUnits() )
            {
                joiner.add( outer.getUnits().toString() );
            }
            // Placeholder
            else
            {
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );
            }

        }
        // Placeholder
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 3 );
        }

        // Probability values?
        if ( outer.hasProbabilities() )
        {
            OneOrTwoDoubles doubles = outer.getProbabilities();
            String left = String.valueOf( doubles.first() );
            joiner.add( left );

            if ( doubles.hasTwo() )
            {
                String right = String.valueOf( doubles.second() );
                joiner.add( right );
            }
            // Placeholder
            else
            {
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );
            }
        }
        // Placeholder
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 2 );
        }

        // Threshold side
        this.append( joiner, outer.getOrientation()
                                  .name()
                                  .replace( "_", " " ),
                     false );

        // Threshold operator
        this.append( joiner, outer.getOperator()
                                  .toString()
                                  .toUpperCase(),
                     false );

        return joiner;
    }

    /**
     * Writes a blob of statistics to the CSV file.
     *
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @param groupNumber the statistics group number
     * @throws IOException if the statistics could not be written
     */

    private void writeStatistics( Statistics statistics,
                                  BufferedOutputStream writer,
                                  AtomicInteger groupNumber )
            throws IOException
    {
        Objects.requireNonNull( statistics );

        if ( !statistics.hasPool()
             && !statistics.hasBaselinePool() )
        {
            throw new IOException( "Cannot write statistics to CSV without a pool definition." );
        }

        // Get the evaluation and pool information
        Pool pool = statistics.getPool();

        // Get the ensemble average type and use it to qualify metrics that consume single-valued pairs
        EnsembleAverageType ensembleAverageType = pool.getEnsembleAverageType();

        if ( !statistics.hasPool() )
        {
            pool = statistics.getBaselinePool();
        }

        StringJoiner poolDescription = this.getPoolDescription( pool );

        // Merge the evaluation and pool descriptions into an empty joiner
        StringJoiner merge = new StringJoiner( CsvStatisticsWriter.DELIMITER );
        merge.merge( this.evaluationDescription );
        StringJoiner mergeDescription = merge.merge( poolDescription );

        SummaryStatistic summaryStatistic = null;
        if ( statistics.hasSummaryStatistic() )
        {
            summaryStatistic = statistics.getSummaryStatistic();
        }

        // Write the double scores
        if ( !statistics.getScoresList()
                        .isEmpty() )
        {
            this.writeDoubleScores( mergeDescription,
                                    statistics.getScoresList(),
                                    writer,
                                    groupNumber,
                                    ensembleAverageType,
                                    summaryStatistic );
        }

        // Write the duration scores
        if ( !statistics.getDurationScoresList()
                        .isEmpty() )
        {
            this.writeDurationScores( mergeDescription,
                                      statistics.getDurationScoresList(),
                                      writer,
                                      this.getDurationUnits(),
                                      groupNumber,
                                      summaryStatistic );
        }

        // Write the diagrams
        if ( !statistics.getDiagramsList()
                        .isEmpty() )
        {
            this.writeDiagrams( mergeDescription,
                                statistics.getDiagramsList(),
                                writer,
                                groupNumber,
                                ensembleAverageType,
                                summaryStatistic );
        }

        // Write the box plots per pair (per pool)
        if ( !statistics.getOneBoxPerPairList()
                        .isEmpty() )
        {
            this.writeBoxPlots( mergeDescription,
                                statistics.getOneBoxPerPairList(),
                                summaryStatistic,
                                writer,
                                groupNumber,
                                ensembleAverageType );
        }

        // Write the box plots per pool
        if ( !statistics.getOneBoxPerPoolList()
                        .isEmpty() )
        {
            this.writeBoxPlots( mergeDescription,
                                statistics.getOneBoxPerPoolList(),
                                summaryStatistic,
                                writer,
                                groupNumber,
                                ensembleAverageType );
        }

        // Write the timing error diagrams
        if ( !statistics.getDurationDiagramsList()
                        .isEmpty() )
        {
            this.writeDurationDiagrams( mergeDescription,
                                        statistics.getDurationDiagramsList(),
                                        writer,
                                        this.getDurationUnits(),
                                        groupNumber,
                                        summaryStatistic );
        }
    }

    /**
     * Writes a list of double scores to the CSV file.
     *
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @param groupNumber the statistics group number
     * @param ensembleAverageType the ensemble average type, where applicable
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if any of the scores could not be written
     */

    private void writeDoubleScores( StringJoiner poolDescription,
                                    List<DoubleScoreStatistic> statistics,
                                    BufferedOutputStream writer,
                                    AtomicInteger groupNumber,
                                    EnsembleAverageType ensembleAverageType,
                                    SummaryStatistic summaryStatistic )
            throws IOException
    {
        // Sort in metric name order
        Comparator<DoubleScoreStatistic> comparator =
                Comparator.comparing( a -> a.getMetric().getName() );

        List<DoubleScoreStatistic> sorted = new ArrayList<>( statistics );
        sorted.sort( comparator );

        for ( DoubleScoreStatistic next : sorted )
        {
            this.writeDoubleScore( poolDescription, next, writer, groupNumber, ensembleAverageType, summaryStatistic );
        }
    }

    /**
     * Writes a list of duration scores to the CSV file.
     *
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @param durationUnits the duration units
     * @param groupNumber the statistics group number
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if any of the scores could not be written
     */

    private void writeDurationScores( StringJoiner poolDescription,
                                      List<DurationScoreStatistic> statistics,
                                      BufferedOutputStream writer,
                                      ChronoUnit durationUnits,
                                      AtomicInteger groupNumber,
                                      SummaryStatistic summaryStatistic )
            throws IOException
    {
        // Sort in metric name order
        Comparator<DurationScoreStatistic> comparator =
                Comparator.comparing( a -> a.getMetric().getName() );

        List<DurationScoreStatistic> sorted = new ArrayList<>( statistics );
        sorted.sort( comparator );

        for ( DurationScoreStatistic next : sorted )
        {
            this.writeDurationScore( poolDescription, next, writer, durationUnits, groupNumber, summaryStatistic );
        }
    }

    /**
     * Writes a list of diagrams to the CSV file.
     *
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @param groupNumber the statistics group number
     * @param ensembleAverageType the ensemble average type
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if any of the scores could not be written
     */

    private void writeDiagrams( StringJoiner poolDescription,
                                List<DiagramStatistic> statistics,
                                BufferedOutputStream writer,
                                AtomicInteger groupNumber,
                                EnsembleAverageType ensembleAverageType,
                                SummaryStatistic summaryStatistic )
            throws IOException
    {
        // Sort in metric name order
        Comparator<DiagramStatistic> comparator =
                Comparator.comparing( a -> a.getMetric()
                                            .getName() );

        List<DiagramStatistic> sorted = new ArrayList<>( statistics );
        sorted.sort( comparator );

        for ( DiagramStatistic next : sorted )
        {
            this.writeDiagram( poolDescription, next, writer, groupNumber, ensembleAverageType, summaryStatistic );
        }
    }

    /**
     * Writes a list of box plots scores to the CSV file.
     *
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param summaryStatistic the summary statistic, where applicable
     * @param writer a shared writer, not to be closed
     * @param groupNumber the statistics group number
     * @param ensembleAverageType the ensemble average type, where applicable
     * @throws IOException if any of the scores could not be written
     */

    private void writeBoxPlots( StringJoiner poolDescription,
                                List<BoxplotStatistic> statistics,
                                SummaryStatistic summaryStatistic,
                                BufferedOutputStream writer,
                                AtomicInteger groupNumber,
                                EnsembleAverageType ensembleAverageType )
            throws IOException
    {
        // Sort in metric name order
        Comparator<BoxplotStatistic> comparator =
                Comparator.comparing( a -> a.getMetric().getName() );

        List<BoxplotStatistic> sorted = new ArrayList<>( statistics );
        sorted.sort( comparator );

        for ( BoxplotStatistic next : sorted )
        {
            this.writeBoxplot( poolDescription, next, summaryStatistic, writer, groupNumber, ensembleAverageType );
        }
    }

    /**
     * Writes a list of duration diagrams to the CSV file.
     *
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @param durationUnits the duration units
     * @param groupNumber the statistics group number
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if any of the scores could not be written
     */

    private void writeDurationDiagrams( StringJoiner poolDescription,
                                        List<DurationDiagramStatistic> statistics,
                                        BufferedOutputStream writer,
                                        ChronoUnit durationUnits,
                                        AtomicInteger groupNumber,
                                        SummaryStatistic summaryStatistic )
            throws IOException
    {
        // Sort in metric name order
        Comparator<DurationDiagramStatistic> comparator =
                Comparator.comparing( a -> a.getMetric().getName() );

        List<DurationDiagramStatistic> sorted = new ArrayList<>( statistics );
        sorted.sort( comparator );

        for ( DurationDiagramStatistic next : sorted )
        {
            this.writeDurationDiagram( poolDescription, next, writer, durationUnits, groupNumber, summaryStatistic );
        }
    }

    /**
     * Writes a double score.
     *
     * @param poolDescription the pool description
     * @param score the score
     * @param writer a shared writer, not to be closed
     * @param groupNumber the statistics group number
     * @param ensembleAverageType the ensemble average type, where applicable
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if the score could not be written
     */

    private void writeDoubleScore( StringJoiner poolDescription,
                                   DoubleScoreStatistic score,
                                   BufferedOutputStream writer,
                                   AtomicInteger groupNumber,
                                   EnsembleAverageType ensembleAverageType,
                                   SummaryStatistic summaryStatistic )
            throws IOException
    {
        DoubleScoreMetric metric = score.getMetric();

        for ( DoubleScoreStatisticComponent next : score.getStatisticsList() )
        {
            // Add a line separator for the next row
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR );

            StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

            // Add the pool description
            joiner.merge( poolDescription );

            DoubleScoreMetricComponent metricComponent = next.getMetric();

            // Add the metric name, pretty printed
            String name = metric.getName()
                                .name();
            MetricConstants namedMetric = MetricConstants.valueOf( name );
            this.append( joiner, namedMetric.toString(), false );

            // Add the metric component name, pretty printed
            MetricConstants namedMetricComponent = MetricConstants.valueOf( metricComponent.getName()
                                                                                           .name() );
            this.append( joiner, namedMetricComponent.toString(), false );

            // Name qualifier
            String qualifier = this.getMetricComponentQualifier( namedMetric, ensembleAverageType );
            this.append( joiner, qualifier, false );

            // Add the metric component units
            this.append( joiner, metricComponent.getUnits(), false );

            // Add the minimum value
            this.append( joiner, CsvStatisticsWriter.getDoubleString( metricComponent.getMinimum() ), false );

            // Add the maximum value
            this.append( joiner, CsvStatisticsWriter.getDoubleString( metricComponent.getMaximum() ), false );

            // Add the optimum value
            this.append( joiner, CsvStatisticsWriter.getDoubleString( metricComponent.getOptimum() ), false );

            // Add the statistic group number
            this.append( joiner, String.valueOf( groupNumber.getAndIncrement() ), false );

            // Add the summary statistic, if available
            this.writeSummaryStatistic( summaryStatistic, metricComponent.getUnits(), joiner );

            // Add the statistic value
            String formattedValue = this.getDecimalFormatter()
                                        .apply( next.getValue() );
            this.append( joiner, formattedValue, false );

            // Join the row string and get the row bytes in utf8.
            byte[] row = joiner.toString()
                               .getBytes( StandardCharsets.UTF_8 );

            // Write the row
            writer.write( row );
        }
    }

    /**
     * Returns a string representation of a double
     *
     * @param value the value
     * @return a string representation of a double, else an empty value
     */

    private static String getDoubleString( double value )
    {
        if ( Double.isNaN( value ) )
        {
            return "";
        }

        return String.valueOf( value );
    }

    /**
     * Returns a metric component qualifier for single-valued metrics based on the type of ensemble average used.
     *
     * @param metricName the metric name to test whether it is a single-valued metric
     * @param ensembleAverageType the ensemble average type
     * @return a qualifier
     */

    private String getMetricComponentQualifier( MetricConstants metricName,
                                                EnsembleAverageType ensembleAverageType )
    {
        Objects.requireNonNull( metricName );

        String qualifier = "";

        if ( Objects.nonNull( ensembleAverageType )
             && metricName.isInGroup( SampleDataGroup.SINGLE_VALUED )
             && ensembleAverageType != EnsembleAverageType.NONE )
        {
            qualifier = "ENSEMBLE " + ensembleAverageType.name();
        }

        return qualifier;
    }

    /**
     * Writes a duration score.
     *
     * @param poolDescription the pool description
     * @param score the score
     * @param writer a shared writer, not to be closed
     * @param durationUnits the duration units
     * @param groupNumber the statistics group number
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if the score could not be written
     */

    private void writeDurationScore( StringJoiner poolDescription,
                                     DurationScoreStatistic score,
                                     BufferedOutputStream writer,
                                     ChronoUnit durationUnits,
                                     AtomicInteger groupNumber,
                                     SummaryStatistic summaryStatistic )
            throws IOException
    {
        DurationScoreMetric metric = score.getMetric();

        for ( DurationScoreStatisticComponent next : score.getStatisticsList() )
        {
            // Add a line separator for the next row
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR );

            StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

            // Add the pool description
            joiner.merge( poolDescription );

            DurationScoreMetricComponent metricComponent = next.getMetric();

            // Add the metric name, pretty printed
            MetricConstants namedMetric = MetricConstants.valueOf( metric.getName().name() );
            this.append( joiner, namedMetric.toString(), false );

            // Add the metric component name, pretty printed
            MetricConstants namedMetricComponent = MetricConstants.valueOf( metricComponent.getName().name() );
            this.append( joiner, namedMetricComponent.toString(), false );

            // Name qualifier
            this.append( joiner, "", false );

            // Relative timing errors
            if ( metric.getName() == MetricName.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC )
            {
                String units = durationUnits.toString()
                                            .toUpperCase() + " PER HOUR";
                this.append( joiner, units, false );

                // Add the metric limits
                this.addDurationMetricLimits( joiner,
                                              metricComponent.getMinimum(),
                                              metricComponent.getMaximum(),
                                              metricComponent.getOptimum() );

                // Add the statistic group number
                this.append( joiner, String.valueOf( groupNumber.getAndIncrement() ), false );

                // Add the summary statistic, if available
                this.writeSummaryStatistic( summaryStatistic, units, joiner );

                // Add the statistic value
                com.google.protobuf.Duration protoDuration = next.getValue();
                BigDecimal nanoAdd = BigDecimal.valueOf( protoDuration.getNanos(), 9 );
                BigDecimal durationInUserUnits = BigDecimal.valueOf( protoDuration.getSeconds() )
                                                           .add( nanoAdd )
                                                           .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

                String formattedValue = durationInUserUnits.toPlainString();
                this.append( joiner, formattedValue, false );
            }
            // Absolute timing errors
            else
            {
                String units = durationUnits.toString()
                                            .toUpperCase();
                this.append( joiner, units, false );

                // Add the metric limits
                this.addDurationMetricLimits( joiner,
                                              metricComponent.getMinimum(),
                                              metricComponent.getMaximum(),
                                              metricComponent.getOptimum() );

                // Add the statistic group number
                this.append( joiner, String.valueOf( groupNumber.getAndIncrement() ), false );

                // Add the summary statistic, if available
                this.writeSummaryStatistic( summaryStatistic, units, joiner );

                // Add the statistic value
                com.google.protobuf.Duration protoDuration = next.getValue();
                BigDecimal nanoAdd = BigDecimal.valueOf( protoDuration.getNanos(), 9 );
                BigDecimal durationInUserUnits = BigDecimal.valueOf( protoDuration.getSeconds() )
                                                           .add( nanoAdd )
                                                           .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

                String formattedValue = durationInUserUnits.toPlainString();
                this.append( joiner, formattedValue, false );
            }

            // Join the row string and get the row bytes in utf8.
            byte[] row = joiner.toString()
                               .getBytes( StandardCharsets.UTF_8 );

            // Write the row
            writer.write( row );
        }
    }

    /**
     * Adds the metric limits for a duration metric to a joiner.
     * @param joiner the joiner
     * @param minimum the minimum limit
     * @param maximum the maximum limit
     * @param optimum the optimum
     */

    private void addDurationMetricLimits( StringJoiner joiner,
                                          com.google.protobuf.Duration minimum,
                                          com.google.protobuf.Duration maximum,
                                          com.google.protobuf.Duration optimum )
    {
        // Add the minimum value
        BigDecimal minDurationAdd = BigDecimal.valueOf( minimum.getNanos(), 9 );
        BigDecimal minDurationInUserUnits = BigDecimal.valueOf( minimum.getSeconds() )
                                                      .add( minDurationAdd )
                                                      .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

        this.append( joiner, minDurationInUserUnits.toPlainString(), false );

        // Add the maximum value
        BigDecimal maxDurationAdd = BigDecimal.valueOf( maximum.getNanos(), 9 );
        BigDecimal maxDurationInUserUnits = BigDecimal.valueOf( maximum.getSeconds() )
                                                      .add( maxDurationAdd )
                                                      .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

        this.append( joiner, maxDurationInUserUnits.toPlainString(), false );

        // Add the optimum value
        BigDecimal optDurationAdd = BigDecimal.valueOf( optimum.getNanos(), 9 );
        BigDecimal optDurationInUserUnits = BigDecimal.valueOf( optimum.getSeconds() )
                                                      .add( optDurationAdd )
                                                      .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

        this.append( joiner, optDurationInUserUnits.toPlainString(), false );
    }

    /**
     * Writes a diagram.
     *
     * @param poolDescription the pool description
     * @param diagram the diagram
     * @param writer a shared writer, not to be closed
     * @param groupNumber the statistics group number
     * @param ensembleAverageType the ensemble average type
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if the score could not be written
     */

    private void writeDiagram( StringJoiner poolDescription,
                               DiagramStatistic diagram,
                               BufferedOutputStream writer,
                               AtomicInteger groupNumber,
                               EnsembleAverageType ensembleAverageType,
                               SummaryStatistic summaryStatistic )
            throws IOException
    {
        DiagramMetric metric = diagram.getMetric();

        for ( DiagramStatisticComponent next : diagram.getStatisticsList() )
        {
            int innerGroupNumber = groupNumber.get();
            for ( Double nextValue : next.getValuesList() )
            {
                // Add a line separator for the next row
                writer.write( CsvStatisticsWriter.LINE_SEPARATOR );

                StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

                // Add the pool description
                joiner.merge( poolDescription );

                // Add the metric information, depending on whether this is a summary statistic
                DiagramMetricComponent metricComponent = next.getMetric();
                MetricDetails details = this.getMetricDetails( metric, metricComponent, summaryStatistic );

                // Add the metric name
                this.append( joiner, details.metricName(), false );

                // Add the metric component name
                this.append( joiner, details.metricComponentName(), false );

                // Name qualifier: use an explicit name qualifier first, else the ensemble average type for
                // single-valued metrics. These two things do not overlap.
                String qualifier = next.getName();
                if ( qualifier.isBlank() )
                {
                    String enumString = DeclarationUtilities.toEnumName( details.metricName() );
                    MetricConstants metricName = MetricConstants.valueOf( enumString );
                    qualifier = this.getMetricComponentQualifier( metricName, ensembleAverageType );
                }
                this.append( joiner, qualifier, false );

                // Add the metric component units
                this.append( joiner, details.units(), false );

                this.append( joiner, details.minimum(), false );

                // Add the maximum value
                this.append( joiner, details.maximum(), false );

                // Optimum value
                this.append( joiner, details.optimum(), false );

                // Add the statistic group number
                this.append( joiner, String.valueOf( innerGroupNumber ), false );

                // Add the summary statistic, if available
                this.writeSummaryStatistic( summaryStatistic,
                                            details.summaryStatisticMetricName(),
                                            details.summaryStatisticMetricComponentName(),
                                            details.units(),
                                            joiner );

                // Add the statistic value
                String formattedValue = this.getDecimalFormatter()
                                            .apply( nextValue );

                this.append( joiner, formattedValue, false );

                // Join the row string and get the row bytes in utf8.
                byte[] row = joiner.toString()
                                   .getBytes( StandardCharsets.UTF_8 );

                // Write the row
                writer.write( row );

                // Increment the group number
                innerGroupNumber++;
            }
        }

        // Increment the group number by the number of elements in one diagram dimension because they are all equal in
        // size
        groupNumber.getAndAdd( diagram.getStatistics( 0 )
                                      .getValuesCount() );
    }

    /**
     * Generates the descriptive details of a metric.
     * @param metric the metric
     * @param metricComponent the metric component
     * @param summaryStatistic the summary statistic, if present
     * @return the metric details
     */
    private MetricDetails getMetricDetails( DiagramMetric metric,
                                            DiagramMetricComponent metricComponent,
                                            SummaryStatistic summaryStatistic )
    {
        String metricName;
        String metricComponentName;
        String units;
        String minimum;
        String maximum;
        String optimum;
        String summaryStatisticMetricName;
        String summaryStatisticMetricComponentName;

        // The diagram is itself a summary statistic for a named metric (e.g., histogram of mean errors)
        if ( Objects.nonNull( summaryStatistic )
             && metric.getStatisticName() != MetricName.UNDEFINED )
        {
            metricName = metric.getStatisticName()
                               .name()
                               .replace( "_", " " );
            metricComponentName = metric.getStatisticComponentName()
                                        .name()
                                        .replace( "_", " " );
            units = metricComponent.getUnits();
            minimum = String.valueOf( metric.getStatisticMinimum() );
            maximum = String.valueOf( metric.getStatisticMaximum() );
            optimum = String.valueOf( metric.getStatisticOptimum() );
            summaryStatisticMetricName = metric.getName()
                                               .name()
                                               .replace( "_", " " );
            summaryStatisticMetricComponentName = metricComponent.getName()
                                                                 .name()
                                                                 .replace( "_", " " );
        }
        // Diagram that contains summary statistic values, such as a quantile-quantile diagram whose quantiles
        // represent a mean value across geographic features
        else if ( Objects.nonNull( summaryStatistic ) )
        {
            metricName = metric.getName()
                               .name()
                               .replace( "_", " " );
            metricComponentName = metricComponent.getName()
                                                 .name()
                                                 .replace( "_", " " );
            units = metricComponent.getUnits();
            minimum = String.valueOf( metricComponent.getMinimum() );
            maximum = String.valueOf( metricComponent.getMaximum() );
            optimum = "";
            summaryStatisticMetricName = summaryStatistic.getStatistic()
                                                         .name()
                                                         .replace( "_", " " );
            summaryStatisticMetricComponentName = "";
        }
        // Diagram is a raw metric (e.g., quantile-quantile diagram for one geographic feature)
        else
        {
            metricName = metric.getName()
                               .name()
                               .replace( "_", " " );
            metricComponentName = metricComponent.getName()
                                                 .name()
                                                 .replace( "_", " " );
            units = metricComponent.getUnits();
            minimum = String.valueOf( metricComponent.getMinimum() );
            maximum = String.valueOf( metricComponent.getMaximum() );
            optimum = "";
            summaryStatisticMetricName = "";
            summaryStatisticMetricComponentName = "";
        }

        return new MetricDetails( metricName,
                                  metricComponentName,
                                  units,
                                  minimum,
                                  maximum,
                                  optimum,
                                  summaryStatisticMetricName,
                                  summaryStatisticMetricComponentName );
    }

    /**
     * Writes a duration diagram.
     *
     * @param poolDescription the pool description
     * @param diagram the diagram
     * @param writer a shared writer, not to be closed
     * @param durationUnits the duration units
     * @param groupNumber the statistics group number
     * @param summaryStatistic the summary statistic, if any
     * @throws IOException if the score could not be written
     */

    private void writeDurationDiagram( StringJoiner poolDescription,
                                       DurationDiagramStatistic diagram,
                                       BufferedOutputStream writer,
                                       ChronoUnit durationUnits,
                                       AtomicInteger groupNumber,
                                       SummaryStatistic summaryStatistic )
            throws IOException
    {
        DurationDiagramMetric metric = diagram.getMetric();

        Instant epoch = Instant.ofEpochMilli( 0 );
        String epochString = durationUnits.toString()
                                          .toUpperCase()
                             + " FROM "
                             + epoch;

        for ( PairOfInstantAndDuration next : diagram.getStatisticsList() )
        {
            // Write the instant
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR );
            StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );
            joiner.merge( poolDescription );
            MetricConstants namedMetric = MetricConstants.valueOf( metric.getName()
                                                                         .name() );
            this.append( joiner, namedMetric.toString(), false );
            ReferenceTimeType referenceTimeType = ReferenceTimeType.valueOf( diagram.getReferenceTimeType()
                                                                                    .name() );
            this.append( joiner, referenceTimeType.toString(), false );

            // Name qualifier
            this.append( joiner, "", false );

            // Units
            this.append( joiner, epochString, false );
            Instant time = Instant.ofEpochSecond( next.getTime()
                                                      .getSeconds(), next.getTime()
                                                                         .getNanos() );

            BigDecimal nanoAdd = BigDecimal.valueOf( time.getNano(), 9 );
            BigDecimal epochDurationInUserUnits = BigDecimal.valueOf( time.getEpochSecond() )
                                                            .add( nanoAdd )
                                                            .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

            // No limits for the reference time
            CsvStatisticsWriter.addEmptyValues( joiner, 3 );

            int gNumber = groupNumber.getAndIncrement();

            // Add the statistic group number
            this.append( joiner, String.valueOf( gNumber ), false );

            // Add the summary statistic, if available
            this.writeSummaryStatistic( summaryStatistic, epochString, joiner );

            String formattedValue = epochDurationInUserUnits.toPlainString();
            this.append( joiner, formattedValue, false );
            byte[] valueOne = joiner.toString()
                                    .getBytes( StandardCharsets.UTF_8 );
            writer.write( valueOne );

            // Write the duration
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR );
            StringJoiner joinerTwo = new StringJoiner( CsvStatisticsWriter.DELIMITER );
            joinerTwo.merge( poolDescription );
            this.append( joinerTwo, namedMetric.toString(), false );
            this.append( joinerTwo, "ERROR", false );

            // Name qualifier
            this.append( joinerTwo, "", false );

            // Units
            String units = durationUnits.toString()
                                        .toUpperCase();
            this.append( joinerTwo, units, false );

            com.google.protobuf.Duration protoDuration = next.getDuration();
            BigDecimal nanoDurationAdd = BigDecimal.valueOf( protoDuration.getNanos(), 9 );
            BigDecimal durationInUserUnits = BigDecimal.valueOf( protoDuration.getSeconds() )
                                                       .add( nanoDurationAdd )
                                                       .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

            // Add the metric limits
            this.addDurationMetricLimits( joinerTwo, metric.getMinimum(), metric.getMaximum(), metric.getOptimum() );

            // Add the statistic group number
            this.append( joinerTwo, String.valueOf( gNumber ), false );

            // Add the summary statistic, if available
            this.writeSummaryStatistic( summaryStatistic, units, joinerTwo );

            String formattedValueDuration = durationInUserUnits.toPlainString();
            this.append( joinerTwo, formattedValueDuration, false );
            byte[] valueTwo = joinerTwo.toString()
                                       .getBytes( StandardCharsets.UTF_8 );
            writer.write( valueTwo );
        }
    }

    /**
     * Writes a box plot.
     *
     * @param poolDescription the pool description
     * @param boxplot the box plot
     * @param summaryStatistic the summary statistic, where applicable
     * @param writer a shared writer, not to be closed
     * @param groupNumber the statistics group number
     * @param ensembleAverageType the ensemble average type, where applicable
     * @throws IOException if the score could not be written
     */

    private void writeBoxplot( StringJoiner poolDescription,
                               BoxplotStatistic boxplot,
                               SummaryStatistic summaryStatistic,
                               BufferedOutputStream writer,
                               AtomicInteger groupNumber,
                               EnsembleAverageType ensembleAverageType )
            throws IOException
    {
        BoxplotMetric metric = boxplot.getMetric();

        // Group number and amount by which to increment the group number after writing
        int gNumber = groupNumber.get();
        int addToGroupNumber = 0;

        MetricDimension quantileValueType = MetricDimension.valueOf( metric.getQuantileValueType()
                                                                           .name() );
        String quantileValueTypeString = quantileValueType.toString();

        for ( Box next : boxplot.getStatisticsList() )
        {
            LinkedValueType valueType = metric.getLinkedValueType();

            // Add the linked value if one exists
            if ( valueType != LinkedValueType.NONE )
            {
                String componentName = MetricDimension.valueOf( valueType.name() )
                                                      .toString();
                double statistic = next.getLinkedValue();
                BoxplotElement element = new BoxplotElement( poolDescription,
                                                             metric,
                                                             componentName,
                                                             summaryStatistic,
                                                             gNumber + addToGroupNumber,
                                                             statistic,
                                                             ensembleAverageType );
                this.writeBoxplotElement( element, writer );

                // Do not increment group number here: tie to the first probability/quantile of a box.
            }

            // Add the probabilities for the quantiles
            List<Double> probabilities = metric.getQuantilesList();

            for ( int i = 0; i < probabilities.size(); i++ )
            {
                BoxplotElement element = new BoxplotElement( poolDescription,
                                                             metric,
                                                             PROBABILITY,
                                                             summaryStatistic,
                                                             gNumber + addToGroupNumber + i,
                                                             probabilities.get( i ),
                                                             ensembleAverageType );
                this.writeBoxplotElement( element, writer );
            }

            // Add the quantiles
            List<Double> quantiles = next.getQuantilesList();

            for ( int i = 0; i < probabilities.size(); i++ )
            {
                BoxplotElement element = new BoxplotElement( poolDescription,
                                                             metric,
                                                             quantileValueTypeString,
                                                             summaryStatistic,
                                                             gNumber + addToGroupNumber + i,
                                                             quantiles.get( i ),
                                                             ensembleAverageType );
                this.writeBoxplotElement( element, writer );
            }

            // Increment the group number
            addToGroupNumber += probabilities.size();
        }

        // Increment the group number
        groupNumber.getAndAdd( addToGroupNumber );
    }

    /**
     * Writes a box plot statistic.
     *
     * @param element the boxplot element
     * @param writer thw writer
     * @throws IOException if the statistic could not be written
     */

    private void writeBoxplotElement( BoxplotElement element,
                                      BufferedOutputStream writer )
            throws IOException
    {
        StringJoiner poolDescription = element.poolDescription();
        SummaryStatistic summaryStatistic = element.summaryStatistic();
        int groupNumber = element.groupNumber();
        double statistic = element.statistic();
        EnsembleAverageType ensembleAverageType = element.ensembleAverageType();

        // Add a line separator for the next row
        writer.write( CsvStatisticsWriter.LINE_SEPARATOR );

        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        // Add the pool description
        joiner.merge( poolDescription );

        // Add the metric information, depending on whether this is a summary statistic
        MetricDetails details = this.getMetricDetails( element, summaryStatistic );

        // Add the metric name
        this.append( joiner, details.metricName(), false );

        this.append( joiner, details.metricComponentName(), false );

        // Add the qualifier
        String enumString = DeclarationUtilities.toEnumName( details.metricName() );
        MetricConstants metricName = MetricConstants.valueOf( enumString );
        String qualifier = this.getMetricComponentQualifier( metricName, ensembleAverageType );

        this.append( joiner, qualifier, false );

        // Add the metric component units
        this.append( joiner, details.units(), false );

        // Add the minimum value
        this.append( joiner, details.minimum(), false );

        // Add the maximum value
        this.append( joiner, details.maximum(), false );

        // Add the optimum value
        this.append( joiner, details.optimum(), false );

        // Add the statistics group number
        this.append( joiner, String.valueOf( groupNumber ), false );

        // Write the summary statistic
        this.writeSummaryStatistic( summaryStatistic,
                                    details.summaryStatisticMetricName(),
                                    details.summaryStatisticMetricComponentName(),
                                    details.units(),
                                    joiner );

        // Add the statistic value
        String formattedValue = this.getDecimalFormatter()
                                    .apply( statistic );
        this.append( joiner, formattedValue, false );

        // Join the row string and get the row bytes in utf8.
        byte[] row = joiner.toString()
                           .getBytes( StandardCharsets.UTF_8 );
        writer.write( row );
    }

    /**
     * Generates the descriptive details of a metric.
     * @param boxplotElement the boxplot element
     * @param summaryStatistic the summary statistic, if present
     * @return the metric details
     */
    private MetricDetails getMetricDetails( BoxplotElement boxplotElement,
                                            SummaryStatistic summaryStatistic )
    {
        BoxplotMetric metric = boxplotElement.metric();
        String metricName;
        String metricComponentName;
        String units = metric.getUnits();
        String minimum = String.valueOf( metric.getMinimum() );
        String maximum = String.valueOf( metric.getMaximum() );
        String optimum = String.valueOf( metric.getOptimum() );
        String summaryStatisticMetricName;
        String summaryStatisticMetricComponentName;

        // The diagram is itself a summary statistic for a named metric (e.g., histogram of mean errors)
        if ( Objects.nonNull( summaryStatistic )
             && metric.getStatisticName() != MetricName.UNDEFINED )
        {
            metricName = metric.getStatisticName()
                               .name()
                               .replace( "_", " " );
            metricComponentName = metric.getStatisticComponentName()
                                        .name()
                                        .replace( "_", " " );

            summaryStatisticMetricName = metric.getName()
                                               .name()
                                               .replace( "_", " " );
            summaryStatisticMetricComponentName = boxplotElement.metricComponentName()
                                                                .replace( "_", " " );
        }
        // Diagram that contains summary statistic values, such as a quantile-quantile diagram whose quantiles
        // represent a mean value across geographic features
        else if ( Objects.nonNull( summaryStatistic ) )
        {
            metricName = metric.getName()
                               .name()
                               .replace( "_", " " );
            metricComponentName = boxplotElement.metricComponentName()
                                                .replace( "_", " " );
            summaryStatisticMetricName = summaryStatistic.getStatistic()
                                                         .name()
                                                         .replace( "_", " " );
            summaryStatisticMetricComponentName = "";
        }
        // Diagram is a raw metric (e.g., quantile-quantile diagram for one geographic feature)
        else
        {
            metricName = metric.getName()
                               .name()
                               .replace( "_", " " );
            metricComponentName = boxplotElement.metricComponentName()
                                                .replace( "_", " " );
            summaryStatisticMetricName = "";
            summaryStatisticMetricComponentName = "";
        }

        if ( PROBABILITY.equals( metricComponentName )
             || PROBABILITY.equals( summaryStatisticMetricComponentName ) )
        {
            units = PROBABILITY;
        }

        return new MetricDetails( metricName,
                                  metricComponentName,
                                  units,
                                  minimum,
                                  maximum,
                                  optimum,
                                  summaryStatisticMetricName,
                                  summaryStatisticMetricComponentName );
    }

    /**
     * Writes the metadata for a summary statistic, which includes a resampled quantile.
     *
     * @param summaryStatistic the summary statistic
     * @param units the summary statistic units
     * @param joiner the string joiner to which the summary statistic will be appended
     */

    private void writeSummaryStatistic( SummaryStatistic summaryStatistic,
                                        String units,
                                        StringJoiner joiner )
    {
        this.writeSummaryStatistic( summaryStatistic,
                                    MetricName.UNDEFINED.toString(),
                                    MetricName.UNDEFINED.toString(),
                                    units,
                                    joiner );
    }

    /**
     * Writes the metadata for a summary statistic, which includes a resampled quantile.
     *
     * @param summaryStatistic the summary statistic
     * @param subject the subject metric of the summary statistic, where applicable
     * @param subjectComponent the component name of the subject metric of the summary statistic, where applicable
     * @param units the summary statistic units
     * @param joiner the string joiner to which the summary statistic will be appended
     */

    private void writeSummaryStatistic( SummaryStatistic summaryStatistic,
                                        String subject,
                                        String subjectComponent,
                                        String units,
                                        StringJoiner joiner )
    {
        if ( Objects.nonNull( summaryStatistic ) )
        {
            // Write the subject, where defined
            if ( !Objects.equals( subject, MetricName.UNDEFINED.toString() ) )
            {
                this.append( joiner, subject.replace( "_", " " ),
                             false );

                // Write the subject component, where defined
                if ( !Objects.equals( subjectComponent, MetricName.UNDEFINED.toString() ) )
                {
                    String component = subjectComponent.replace( "_", " " );
                    this.append( joiner, component, false );
                }
            }
            else
            {
                String statisticName = summaryStatistic.getStatistic()
                                                       .toString()
                                                       .replace( "_", " " );

                this.append( joiner, statisticName, false );
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );
            }

            this.append( joiner, units, false );

            String statisticDimension = summaryStatistic.getDimension()
                                                        .toString()
                                                        .replace( "_", " " );
            this.append( joiner, statisticDimension, false );

            // Quantile for the summary statistic?
            if ( this.hasQuantileSummaryStatistic( summaryStatistic ) )
            {
                double probability = summaryStatistic.getProbability();
                String stringProbability = String.valueOf( probability );
                this.append( joiner, stringProbability, false );
            }
            else
            {
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );
            }

            // Quantile for the sampling uncertainty?
            if ( this.hasQuantileSamplingUncertainty( summaryStatistic ) )
            {
                double probability = summaryStatistic.getProbability();
                String stringProbability = String.valueOf( probability );
                this.append( joiner, stringProbability, false );
            }
            else
            {
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );
            }
        }
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 6 );
        }
    }

    /**
     * Appends the string to the joiner and adds an empty value if the input string is null or blank. Optionally, quotes 
     * the input.
     *
     * @param append the string to add
     * @param quote is true to add quotations, false otherwise
     */

    private void append( StringJoiner joiner, String append, boolean quote )
    {
        if ( Objects.nonNull( append ) && !append.isBlank() )
        {
            if ( quote )
            {
                joiner.add( "\"" + append + "\"" );
            }
            else
            {
                joiner.add( append );
            }
        }
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }
    }

    /**
     * @return the duration units
     */

    private ChronoUnit getDurationUnits()
    {
        return this.durationUnits;
    }

    /**
     * @return the decimal formatter
     */

    private DoubleFunction<String> getDecimalFormatter()
    {
        return this.decimalFormatter;
    }

    /**
     * @param summaryStatistic the summary statistic to test
     * @return whether the summary statistic is a quantile
     */
    private boolean hasQuantileSummaryStatistic( SummaryStatistic summaryStatistic )
    {
        return Objects.nonNull( summaryStatistic )
               && summaryStatistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE
               && summaryStatistic.getDimension() != SummaryStatistic.StatisticDimension.RESAMPLED;
    }

    /**
     * @param summaryStatistic the summary statistic to test
     * @return whether the summary statistic contains a quantile for the sampling uncertainty
     */
    private boolean hasQuantileSamplingUncertainty( SummaryStatistic summaryStatistic )
    {
        return Objects.nonNull( summaryStatistic )
               && summaryStatistic.getDimension() == SummaryStatistic.StatisticDimension.RESAMPLED
               && summaryStatistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE;
    }

    /**
     * Hidden constructor.
     *
     * @param evaluation the evaluation description
     * @param path the path to the csv file to write
     * @param gzip is true to gzip the output
     * @param durationUnits the duration units
     * @param decimalFormatter the decimal formatter
     * @throws NullPointerException if any input is null
     * @throws CommaSeparatedWriteException if the writer could not be created or the header written
     */

    private CsvStatisticsWriter( Evaluation evaluation,
                                 Path path,
                                 boolean gzip,
                                 ChronoUnit durationUnits,
                                 DoubleFunction<String> decimalFormatter )
    {
        LOGGER.debug( "Creating a CSV format writer, which will write statistics to {}.", path );

        Objects.requireNonNull( path );
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( durationUnits );
        Objects.requireNonNull( decimalFormatter );

        this.path = path;
        this.writeLock = new ReentrantLock();
        this.durationUnits = durationUnits;
        this.decimalFormatter = decimalFormatter;

        Duration duration = durationUnits.getDuration();
        BigDecimal nanoAdd = BigDecimal.valueOf( duration.getNano(), 9 );
        this.nanosPerDuration = BigDecimal.valueOf( duration.getSeconds() )
                                          .add( nanoAdd );

        // Create the evaluation description from the evaluation
        this.evaluationDescription = this.createEvaluationDescription( evaluation );

        // Path to the CSVT file
        String name = path.getFileName()
                          .toString();
        name = name.replace( ".csv", ".csvt" );
        name = name.replace( ".gz", "" );

        // Resolve the path for the CSVT file
        this.pathToCsvt = path.getParent()
                              .resolve( name );

        try
        {
            // Gzip?
            if ( gzip )
            {
                this.bufferedWriter = new BufferedOutputStream( new GZIPOutputStream( Files.newOutputStream( this.path,
                                                                                                             StandardOpenOption.CREATE,
                                                                                                             StandardOpenOption.APPEND ) ) );
            }
            else
            {
                this.bufferedWriter = new BufferedOutputStream( Files.newOutputStream( this.path,
                                                                                       StandardOpenOption.CREATE,
                                                                                       StandardOpenOption.APPEND ) );
            }
        }
        catch ( IOException e )
        {
            throw new CommaSeparatedWriteException( "Encountered an exception while building a CSV writer.", e );
        }
    }

    /**
     * Attempts to create the CSV file with a standard header and the associated CSVT file.
     * @param writer the writer
     */
    private void testCreateCsvFile( BufferedOutputStream writer )
    {
        if ( !this.headerWritten.getAndSet( true ) )
        {
            try
            {
                this.writeHeader( writer );

                // Write the CSVT file that helps with import into GDAL-enabled off-the-shelf GIS tools.
                this.writeCsvtFileForGdalApplications( this.pathToCsvt );
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "Encountered an exception while building a CSV writer.", e );
            }
        }
    }

    /**
     * Writes a header to the CSV file.
     * @param writer the writer
     * @throws IOException if the header could not be written
     */

    private void writeHeader( BufferedOutputStream writer ) throws IOException
    {
        // Lock for writing
        ReentrantLock lock = this.getWriteLock();
        lock.lock();

        LOGGER.trace( "Acquired writing lock on {}", this.getPath() );

        try
        {
            byte[] headerBytes = CsvStatisticsWriter.HEADER.getBytes( StandardCharsets.UTF_8 );
            writer.write( headerBytes );

            LOGGER.trace( "Header for the CSV file composed of {} written to {}.",
                          CsvStatisticsWriter.HEADER,
                          this.getPath() );
        }
        // Complete writing
        finally
        {
            lock.unlock();

            LOGGER.trace( "Released writing lock on {}", this.getPath() );
        }
    }

    /**
     * Creates the evaluation description from the evaluation.
     *
     * @param evaluation the evaluation
     * @return the evaluation description
     */

    private StringJoiner createEvaluationDescription( Evaluation evaluation )
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        this.append( joiner, evaluation.getLeftVariableName(), false );
        this.append( joiner, evaluation.getRightVariableName(), false );

        if ( !evaluation.getBaselineVariableName()
                        .isBlank() )
        {
            this.append( joiner, evaluation.getBaselineVariableName(), false );
        }
        // Placeholder
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }

        // Covariates with an explicit purpose of filtering
        List<Covariate> covariates = MessageUtilities.getCovariateFilters( evaluation.getCovariatesList() );

        if ( !covariates.isEmpty() )
        {
            String joined = covariates.stream()
                                      .map( MessageUtilities::toString )
                                      .collect( Collectors.joining( LIST_DELIMITER ) );

            this.append( joiner, joined, true );
        }
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }

        return joiner;
    }

    /**
     * Adds a number of empty values inline.
     *
     * @param joiner the string joiner
     * @param count the number of empty values required
     */

    private static void addEmptyValues( StringJoiner joiner, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            joiner.add( "" );
        }
    }

    /**
     * Writes a small CSVT file that aids import into GDAL-enabled tools, such as QGIS.
     *
     * @param path the base path, which will be adjusted
     * @throws IOException if the file could not be written
     */

    private void writeCsvtFileForGdalApplications( Path path ) throws IOException
    {
        String columnClasses = "\"String\",\"String\",\"String\",\"String\",\"String\",\"Integer\",\"String\","
                               + "\"String\",\"WKT\",\"Integer\",\"String\",\"String\",\"WKT\",\"Integer\",\"String\","
                               + "\"String\",\"WKT\",\"Integer\",\"String\",\"String\",\"String\",\"String\","
                               + "\"String\",\"String\",\"String\",\"String\",\"String\",\"String\",\"String\","
                               + "\"String\",\"Real\",\"Real\",\"String\",\"Real\",\"Real\",\"String\",\"String\","
                               + "\"String\",\"Real\",\"Real\",\"String\",\"Real\",\"Real\",\"String\",\"String\","
                               + "\"String\",\"String\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"Integer\","
                               + "\"String\",\"String\",\"String\",\"String\",\"Real\",\"Real\",\"Real\"";

        // Sanity check that the number of column classes equals the number of columns
        int classCount = columnClasses.split( "," ).length;
        int colCount = CsvStatisticsWriter.HEADER.split( "," ).length;

        // Equal?
        if ( classCount != colCount )
        {
            throw new IOException( "Encountered an error while writing a CSVT file. The number of column classes was "
                                   + colCount
                                   + " and the number of columns was "
                                   + classCount
                                   + ". They must be equal." );
        }

        Files.writeString( path,
                           columnClasses,
                           StandardCharsets.UTF_8,
                           StandardOpenOption.CREATE,
                           StandardOpenOption.TRUNCATE_EXISTING );
    }

    /**
     * A boxplot element.
     * @param poolDescription the pool descriptions
     * @param metric the metric
     * @param summaryStatistic the summary statistic, where applicable
     * @param metricComponentName the metric component name
     * @param groupNumber the statistics group number
     * @param statistic the statistic
     * @param ensembleAverageType the ensemble average type, where applicable
     */
    private record BoxplotElement( StringJoiner poolDescription,
                                   BoxplotMetric metric,
                                   String metricComponentName,
                                   SummaryStatistic summaryStatistic,
                                   int groupNumber,
                                   double statistic,
                                   EnsembleAverageType ensembleAverageType )
    {
    }

    /**
     * Descriptive details of a metric.
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     * @param units the unit of the metric component
     * @param minimum the minimum value of the metric component
     * @param maximum the maximum value of the metric component
     * @param optimum the optimal value of the metric component
     * @param summaryStatisticMetricName the summary statistic metric name
     * @param summaryStatisticMetricComponentName the component name of the summary statistic metric
     */
    private record MetricDetails( String metricName,
                                  String metricComponentName,
                                  String units,
                                  String minimum,
                                  String maximum,
                                  String optimum,
                                  String summaryStatisticMetricName,
                                  String summaryStatisticMetricComponentName )
    {
    }

}
