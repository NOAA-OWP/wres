package wres.io.writing.commaseparated.statistics;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.Format;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.BoxplotStatistic.Box;
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
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

/**
 * Writes statistics to a file that contains comma separated values (CSV). There is one CSV file per evaluation and  
 * there should be one instance of this writer per evaluation.
 * 
 *  and hence
 * one instance of this writer per evaluation is anticipated, although multiple .
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
public class CsvStatisticsWriter implements Function<Statistics, Path>, Closeable
{

    /**
     * A default name for the pairs.
     */

    public static final String DEFAULT_FILE_NAME = "evaluation.csv.gz";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( CsvStatisticsWriter.class );

    /**
     * The file header.
     */

    private static final String HEADER = "LEFT VARIABLE NAME,RIGHT VARIABLE NAME,BASELINE VARIABLE NAME,POOL NUMBER,"
                                         + "EVALUATION SUBJECT,LEFT FEATURE NAME,LEFT FEATURE WKT,LEFT FEATURE SRID,"
                                         + "LEFT FEATURE DESCRIPTION,RIGHT FEATURE NAME,RIGHT FEATURE WKT,RIGHT "
                                         + "FEATURE SRID,RIGHT FEATURE DESCRIPTION,BASELINE FEATURE NAME,BASELINE "
                                         + "FEATURE WKT,BASELINE FEATURE SRID,BASELINE FEATURE DESCRIPTION,EARLIEST "
                                         + "ISSUED TIME EXCLUSIVE,LATEST ISSUED TIME INCLUSIVE,EARLIEST VALID TIME "
                                         + "EXCLUSIVE,LATEST VALID TIME INCLUSIVE,EARLIEST LEAD DURATION EXCLUSIVE,"
                                         + "LATEST LEAD DURATION INCLUSIVE,TIME SCALE DURATION,TIME SCALE FUNCTION,"
                                         + "EVENT THRESHOLD NAME,EVENT THRESHOLD LOWER VALUE,EVENT THRESHOLD UPPER "
                                         + "VALUE,EVENT THRESHOLD UNITS,EVENT THRESHOLD LOWER PROBABILITY,EVENT "
                                         + "THRESHOLD UPPER PROBABILITY,EVENT THRESHOLD SIDE,EVENT THRESHOLD OPERATOR,"
                                         + "DECISION THRESHOLD NAME,DECISION THRESHOLD LOWER VALUE,DECISION THRESHOLD "
                                         + "UPPER VALUE,DECISION THRESHOLD UNITS,DECISION THRESHOLD LOWER PROBABILITY,"
                                         + "DECISION THRESHOLD UPPER PROBABILITY,DECISION THRESHOLD SIDE,DECISION "
                                         + "THRESHOLD OPERATOR,METRIC NAME,METRIC COMPONENT NAME,METRIC COMPONENT "
                                         + "UNITS,METRIC COMPONENT MINIMUM,METRIC COMPONENT MAXIMUM,METRIC COMPONENT "
                                         + "OPTIMUM,STATISTIC GROUP NUMBER,STATISTIC";

    /**
     * The CSV delimiter.
     */

    private static final String DELIMITER = ",";

    /**
     * Platform-dependent line separator.
     */

    private static final String LINE_SEPARATOR = System.getProperty( "line.separator" );

    /**
     * Lock for writing pairs to the {@link #path} for which this writer is built.
     */

    private final ReentrantLock writeLock;

    /**
     * The evaluation description.
     */

    private final StringJoiner evaluationDescription;

    /**
     * Buffered writer to share, must be closed on completion.
     */

    private final BufferedOutputStream bufferedWriter;

    /**
     * Number of the current pool being written.
     */

    private int poolNumber = 1;

    /**
     * Group number. Identifies statistics that should be considered within the same group.
     */

    private int groupNumber = 1;

    /**
     * Duration units.
     */

    private final ChronoUnit durationUnits;

    /**
     * Decimal formatter.
     */

    private final Function<Double, String> decimalFormatter;

    /**
     * Nanoseconds per {@link #durationUnits}.
     */

    private final BigDecimal nanosPerDuration;

    /**
     * Path to write.
     */

    @GuardedBy( "writeLock" )
    private final Path path;

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
        return CsvStatisticsWriter.of( evaluation, path, gzip, ChronoUnit.SECONDS, null );
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
                                          Format decimalFormatter )
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
    public Path apply( Statistics statistics )
    {
        Objects.requireNonNull( statistics );

        // Lock here so that all statistics for one pool appear in the same place within the file. The pool numbering 
        // depends on this. If moving down the call chain to an individual write, then pool numbers would need to 
        // appear in the statistics themselves.
        ReentrantLock lock = this.getWriteLock();

        lock.lock();

        try
        {
            this.writeStatistics( statistics, this.bufferedWriter );

            // Increment the pool number
            this.poolNumber++;
        }
        catch ( IOException e )
        {
            throw new CommaSeparatedWriteException( "Encountered an error while writing a blob of statistic to file.",
                                                    e );
        }
        finally
        {
            lock.unlock();
        }

        return this.path;
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
     * @return the pool number for the current pool.
     */

    private int getPoolNumber()
    {
        return poolNumber;
    }

    /**
     * Returns a pool description from the pool definitions.
     * 
     * @return a pool description
     */

    private StringJoiner getPoolDescription( Pool pool )
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        // Pool number
        joiner.add( String.valueOf( this.getPoolNumber() ) );

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
        StringJoiner geometryDescription = this.getGeometryDescription( pool.getGeometryTuplesList() );
        joiner = joiner.merge( geometryDescription );

        // Merge in time window description
        StringJoiner timeWindowDescription = this.getTimeWindowDescription( pool.getTimeWindow() );
        joiner = joiner.merge( timeWindowDescription );

        // Merge in the time scale description
        StringJoiner timeScaleDescription = this.getTimeScaleDescription( pool.getTimeScale() );
        joiner = joiner.merge( timeScaleDescription );

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
     * Returns a geometry description from the input. Currently accepts one geometry only.
     * 
     * @param geometries the geometries
     * @return the geometry description
     * @throws IllegalArgumentException if there is more than one geometry
     */

    private StringJoiner getGeometryDescription( List<GeometryTuple> geometries )
    {
        if ( geometries.size() != 1 )
        {
            throw new IllegalArgumentException( "Expected one geometry tuple per pool but discovered "
                                                + geometries.size()
                                                + ", which is not supported." );
        }

        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        GeometryTuple first = geometries.get( 0 );

        // Left
        Geometry left = first.getLeft();
        // SRID of 0 means no SRID
        String leftSrid = Integer.toString( left.getSrid() );
        joiner.add( left.getName() )
              .add( left.getWkt() );

        if ( !left.getWkt().isBlank() )
        {
            joiner.add( leftSrid );
        }
        // Placeholder
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }

        // Description
        this.append( joiner, left.getDescription(), true );

        // Right
        Geometry right = first.getRight();
        String rightSrid = Integer.toString( right.getSrid() );
        joiner.add( right.getName() )
              .add( right.getWkt() );
        if ( !right.getWkt().isBlank() )
        {
            joiner.add( rightSrid );
        }
        // Placeholder
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }
        // Description
        this.append( joiner, right.getDescription(), true );

        // Baseline
        if ( first.hasBaseline() )
        {
            Geometry baseline = first.getBaseline();
            String baselineSrid = Integer.toString( baseline.getSrid() );
            joiner.add( baseline.getName() )
                  .add( baseline.getWkt() );

            if ( !baseline.getWkt().isBlank() )
            {
                joiner.add( baselineSrid );
            }
            // Placeholder
            else
            {
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );
            }

            // Description
            this.append( joiner, baseline.getDescription(), true );
        }
        // Add placeholders
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 4 );
        }

        return joiner;
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

        joiner.add( outer.getPeriod().toString() )
              .add( outer.getFunction().toString() );

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
        ThresholdOuter outer = new ThresholdOuter.Builder( threshold ).build();

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
        this.append( joiner, outer.getDataType().toString(), false );

        // Threshold operator
        this.append( joiner, outer.getOperator().toString(), false );

        return joiner;
    }

    /**
     * Writes a blob of statistics to the CSV file.
     * 
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @throws IOException if the statistics could not be written
     */

    private void writeStatistics( Statistics statistics, BufferedOutputStream writer ) throws IOException
    {
        Objects.requireNonNull( statistics );

        // Get the evaluation and pool information
        StringJoiner poolDescription = this.getPoolDescription( statistics.getPool() );

        // Merge the evaluation and pool descriptions into an empty joiner
        StringJoiner merge = new StringJoiner( CsvStatisticsWriter.DELIMITER );
        merge.merge( this.evaluationDescription );
        StringJoiner mergeDescription = merge.merge( poolDescription );

        // Write the double scores
        if ( !statistics.getScoresList().isEmpty() )
        {
            this.writeDoubleScores( mergeDescription, statistics.getScoresList(), writer );
        }

        // Write the duration scores
        if ( !statistics.getDurationScoresList().isEmpty() )
        {
            this.writeDurationScores( mergeDescription,
                                      statistics.getDurationScoresList(),
                                      writer,
                                      this.getDurationUnits() );
        }

        // Write the diagrams
        if ( !statistics.getDiagramsList().isEmpty() )
        {
            this.writeDiagrams( mergeDescription,
                                statistics.getDiagramsList(),
                                writer );
        }

        // Write the box plots per pair (per pool)
        if ( !statistics.getOneBoxPerPairList().isEmpty() )
        {
            this.writeBoxPlots( mergeDescription,
                                statistics.getOneBoxPerPairList(),
                                writer );
        }

        // Write the box plots per pool
        if ( !statistics.getOneBoxPerPairList().isEmpty() )
        {
            this.writeBoxPlots( mergeDescription,
                                statistics.getOneBoxPerPoolList(),
                                writer );
        }

        // Write the timing error diagrams
        if ( !statistics.getDurationDiagramsList().isEmpty() )
        {
            this.writeDurationDiagrams( mergeDescription,
                                        statistics.getDurationDiagramsList(),
                                        writer,
                                        this.getDurationUnits() );
        }
    }

    /**
     * Writes a list of double scores to the CSV file.
     * 
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @throws IOException if the any of the scores could not be written
     */

    private void writeDoubleScores( StringJoiner poolDescription,
                                    List<DoubleScoreStatistic> statistics,
                                    BufferedOutputStream writer )
            throws IOException
    {
        for ( DoubleScoreStatistic next : statistics )
        {
            this.writeDoubleScore( poolDescription, next, writer );
        }
    }

    /**
     * Writes a list of duration scores to the CSV file.
     * 
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @param durationUnits the duration units
     * @throws IOException if the any of the scores could not be written
     */

    private void writeDurationScores( StringJoiner poolDescription,
                                      List<DurationScoreStatistic> statistics,
                                      BufferedOutputStream writer,
                                      ChronoUnit durationUnits )
            throws IOException
    {
        for ( DurationScoreStatistic next : statistics )
        {
            this.writeDurationScore( poolDescription, next, writer, durationUnits );
        }
    }

    /**
     * Writes a list of diagrams to the CSV file.
     * 
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @throws IOException if the any of the scores could not be written
     */

    private void writeDiagrams( StringJoiner poolDescription,
                                List<DiagramStatistic> statistics,
                                BufferedOutputStream writer )
            throws IOException
    {
        for ( DiagramStatistic next : statistics )
        {
            this.writeDiagram( poolDescription, next, writer );
        }
    }

    /**
     * Writes a list of box plots scores to the CSV file.
     * 
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @throws IOException if the any of the scores could not be written
     */

    private void writeBoxPlots( StringJoiner poolDescription,
                                List<BoxplotStatistic> statistics,
                                BufferedOutputStream writer )
            throws IOException
    {
        for ( BoxplotStatistic next : statistics )
        {
            this.writeBoxplot( poolDescription, next, writer );
        }
    }

    /**
     * Writes a list of duration diagrams to the CSV file.
     * 
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @param durationUnits the duration units
     * @throws IOException if the any of the scores could not be written
     */

    private void writeDurationDiagrams( StringJoiner poolDescription,
                                        List<DurationDiagramStatistic> statistics,
                                        BufferedOutputStream writer,
                                        ChronoUnit durationUnits )
            throws IOException
    {
        for ( DurationDiagramStatistic next : statistics )
        {
            this.writeDurationDiagram( poolDescription, next, writer, durationUnits );
        }
    }

    /**
     * Writes a double score.
     * 
     * @param poolDescription the pool description
     * @param score the score
     * @param writer a shared writer, not to be closed
     * @param formatter the formatter
     * @throws IOException if the score could not be written
     */

    private void writeDoubleScore( StringJoiner poolDescription,
                                   DoubleScoreStatistic score,
                                   BufferedOutputStream writer )
            throws IOException
    {
        DoubleScoreMetric metric = score.getMetric();

        for ( DoubleScoreStatisticComponent next : score.getStatisticsList() )
        {
            // Add a line separator for the next row
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR.getBytes() );

            StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

            // Add the pool description
            joiner.merge( poolDescription );

            DoubleScoreMetricComponent metricComponent = next.getMetric();

            // Add the metric name, pretty printed
            MetricConstants namedMetric = MetricConstants.valueOf( metric.getName().name() );
            this.append( joiner, namedMetric.toString(), false );

            // Add the metric component name, pretty printed
            MetricConstants namedMetricComponent = MetricConstants.valueOf( metricComponent.getName().name() );
            this.append( joiner, namedMetricComponent.toString(), false );

            // Add the metric component units
            this.append( joiner, metricComponent.getUnits(), false );

            // Add the minimum value
            this.append( joiner, CsvStatisticsWriter.getDoubleString( metricComponent.getMinimum(), true ), false );

            // Add the maximum value
            this.append( joiner, CsvStatisticsWriter.getDoubleString( metricComponent.getMaximum(), true ), false );

            // Add the optimum value
            this.append( joiner, CsvStatisticsWriter.getDoubleString( metricComponent.getOptimum(), true ), false );

            // Add the statistic group number
            this.append( joiner, String.valueOf( this.groupNumber ), false );

            // Add the statistic value
            String formattedValue = this.getDecimalFormatter()
                                        .apply( next.getValue() );
            this.append( joiner, formattedValue, false );

            // Write the row
            writer.write( joiner.toString().getBytes() );

            // Increment the group number
            this.groupNumber++;
        }
    }

    /**
     * Returns a string representation of a double
     * 
     * @param value the value
     * @param nanAsEmpty is true to treat {@link Double#NaN} as empty.
     * @return a string representation of a double, else an empty value
     */

    private static String getDoubleString( double value, boolean nanAsEmpty )
    {
        if ( nanAsEmpty && Double.isNaN( value ) )
        {
            return "";
        }

        return String.valueOf( value );
    }

    /**
     * Writes a duration score.
     * 
     * @param poolDescription the pool description
     * @param score the score
     * @param writer a shared writer, not to be closed
     * @param durationUnits the duration units
     * @throws IOException if the score could not be written
     */

    private void writeDurationScore( StringJoiner poolDescription,
                                     DurationScoreStatistic score,
                                     BufferedOutputStream writer,
                                     ChronoUnit durationUnits )
            throws IOException
    {
        DurationScoreMetric metric = score.getMetric();

        for ( DurationScoreStatisticComponent next : score.getStatisticsList() )
        {
            // Add a line separator for the next row
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR.getBytes() );

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

            // Relative timing errors
            if ( metric.getName() == MetricName.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC )
            {
                this.append( joiner, durationUnits.toString().toUpperCase() + " PER HOUR", false );

                // Add the metric limits
                this.addDurationMetricLimits( joiner,
                                              metricComponent.getMinimum(),
                                              metricComponent.getMaximum(),
                                              metricComponent.getOptimum() );

                // Add the statistic group number
                this.append( joiner, String.valueOf( this.groupNumber ), false );

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
                this.append( joiner, durationUnits.toString().toUpperCase(), false );

                // Add the metric limits
                this.addDurationMetricLimits( joiner,
                                              metricComponent.getMinimum(),
                                              metricComponent.getMaximum(),
                                              metricComponent.getOptimum() );

                // Add the statistic group number
                this.append( joiner, String.valueOf( this.groupNumber ), false );

                // Add the statistic value
                com.google.protobuf.Duration protoDuration = next.getValue();
                BigDecimal nanoAdd = BigDecimal.valueOf( protoDuration.getNanos(), 9 );
                BigDecimal durationInUserUnits = BigDecimal.valueOf( protoDuration.getSeconds() )
                                                           .add( nanoAdd )
                                                           .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

                String formattedValue = durationInUserUnits.toPlainString();
                this.append( joiner, formattedValue, false );
            }

            // Write the row
            writer.write( joiner.toString().getBytes() );

            // Increment the group number
            this.groupNumber++;
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
     * @param formatter the formatter
     * @throws IOException if the score could not be written
     */

    private void writeDiagram( StringJoiner poolDescription,
                               DiagramStatistic diagram,
                               BufferedOutputStream writer )
            throws IOException
    {
        DiagramMetric metric = diagram.getMetric();

        for ( DiagramStatisticComponent next : diagram.getStatisticsList() )
        {
            int innerGroupNumber = this.groupNumber;
            for ( Double nextValue : next.getValuesList() )
            {
                // Add a line separator for the next row
                writer.write( CsvStatisticsWriter.LINE_SEPARATOR.getBytes() );

                StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

                // Add the pool description
                joiner.merge( poolDescription );

                DiagramMetricComponent metricComponent = next.getMetric();

                // Add the metric name, pretty printed
                MetricConstants namedMetric = MetricConstants.valueOf( metric.getName().name() );
                this.append( joiner, namedMetric.toString(), false );

                // Add the metric component name, pretty printed
                MetricDimension namedMetricComponent = MetricDimension.valueOf( metricComponent.getName().name() );
                this.append( joiner, namedMetricComponent.toString(), false );

                // Add the metric component units
                this.append( joiner, metricComponent.getUnits(), false );

                // Add the minimum value
                this.append( joiner, String.valueOf( metricComponent.getMinimum() ), false );

                // Add the maximum value
                this.append( joiner, String.valueOf( metricComponent.getMaximum() ), false );

                // No optimum value
                CsvStatisticsWriter.addEmptyValues( joiner, 1 );

                // Add the statistic group number
                this.append( joiner, String.valueOf( innerGroupNumber ), false );

                // Add the statistic value
                String formattedValue = this.getDecimalFormatter()
                                            .apply( nextValue );

                this.append( joiner, formattedValue, false );

                // Write the row
                writer.write( joiner.toString().getBytes() );

                // Increment the group number
                innerGroupNumber++;
            }
        }

        // Increment the group number by the number of elements in one diagram dimension
        this.groupNumber += diagram.getStatistics( 0 )
                                   .getValuesCount();
    }

    /**
     * Writes a duration diagram.
     * 
     * @param poolDescription the pool description
     * @param diagram the diagram
     * @param writer a shared writer, not to be closed
     * @param formatter the formatter
     * @param durationUnits the duration units
     * @throws IOException if the score could not be written
     */

    private void writeDurationDiagram( StringJoiner poolDescription,
                                       DurationDiagramStatistic diagram,
                                       BufferedOutputStream writer,
                                       ChronoUnit durationUnits )
            throws IOException
    {
        DurationDiagramMetric metric = diagram.getMetric();

        Instant epoch = Instant.ofEpochMilli( 0 );
        String epochString = durationUnits.toString().toUpperCase() + " FROM " + epoch;

        for ( PairOfInstantAndDuration next : diagram.getStatisticsList() )
        {
            // Write the instant
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR.getBytes() );
            StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );
            joiner.merge( poolDescription );
            MetricConstants namedMetric = MetricConstants.valueOf( metric.getName().name() );
            this.append( joiner, namedMetric.toString(), false );
            ReferenceTimeType referenceTimeType = ReferenceTimeType.valueOf( next.getReferenceTimeType().name() );
            this.append( joiner, referenceTimeType.toString(), false );
            this.append( joiner, epochString, false );
            Instant time = Instant.ofEpochSecond( next.getTime().getSeconds(), next.getTime().getNanos() );

            BigDecimal nanoAdd = BigDecimal.valueOf( time.getNano(), 9 );
            BigDecimal epochDurationInUserUnits = BigDecimal.valueOf( time.getEpochSecond() )
                                                            .add( nanoAdd )
                                                            .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

            // No limits for the reference time
            CsvStatisticsWriter.addEmptyValues( joiner, 3 );

            // Add the statistic group number
            this.append( joiner, String.valueOf( this.groupNumber ), false );

            String formattedValue = epochDurationInUserUnits.toPlainString();
            this.append( joiner, formattedValue, false );
            writer.write( joiner.toString().getBytes() );

            // Write the duration
            writer.write( CsvStatisticsWriter.LINE_SEPARATOR.getBytes() );
            StringJoiner joinerTwo = new StringJoiner( CsvStatisticsWriter.DELIMITER );
            joinerTwo.merge( poolDescription );
            this.append( joinerTwo, namedMetric.toString(), false );
            this.append( joinerTwo, "ERROR", false );
            this.append( joinerTwo, durationUnits.toString().toUpperCase(), false );

            com.google.protobuf.Duration protoDuration = next.getDuration();
            BigDecimal nanoDurationAdd = BigDecimal.valueOf( protoDuration.getNanos(), 9 );
            BigDecimal durationInUserUnits = BigDecimal.valueOf( protoDuration.getSeconds() )
                                                       .add( nanoDurationAdd )
                                                       .divide( this.nanosPerDuration, RoundingMode.HALF_UP );

            // Add the metric limits
            this.addDurationMetricLimits( joinerTwo, metric.getMinimum(), metric.getMaximum(), metric.getOptimum() );

            // Add the statistic group number
            this.append( joinerTwo, String.valueOf( this.groupNumber ), false );

            String formattedValueDuration = durationInUserUnits.toPlainString();
            this.append( joinerTwo, formattedValueDuration, false );
            writer.write( joinerTwo.toString().getBytes() );

            // Increment the group number
            this.groupNumber++;
        }
    }

    /**
     * Writes a box plot.
     * 
     * @param poolDescription the pool description
     * @param boxplot the box plot
     * @param writer a shared writer, not to be closed
     * @param formatter the formatter
     * @throws IOException if the score could not be written
     */

    private void writeBoxplot( StringJoiner poolDescription,
                               BoxplotStatistic boxplot,
                               BufferedOutputStream writer )
            throws IOException
    {
        BoxplotMetric metric = boxplot.getMetric();

        // Amount by which to increment the group number after writing
        int addToGroupNumber = 0;

        MetricDimension quantileValueType = MetricDimension.valueOf( metric.getQuantileValueType().name() );
        String quantileValueTypeString = quantileValueType.toString();

        for ( Box next : boxplot.getStatisticsList() )
        {
            LinkedValueType valueType = metric.getLinkedValueType();
            String units = metric.getUnits();

            // Add the linked value if one exists
            if ( valueType != LinkedValueType.NONE )
            {
                String componentName = MetricDimension.valueOf( valueType.name() )
                                                      .toString();
                double statistic = next.getLinkedValue();
                this.writeBoxplotElement( poolDescription,
                                          metric,
                                          componentName,
                                          units,
                                          this.groupNumber + addToGroupNumber,
                                          statistic,
                                          writer );

                // Do not increment group number here: tie to the first probability/quantile of a box.
            }

            // Add the probabilities for the quantiles
            List<Double> probabilities = metric.getQuantilesList();

            for ( int i = 0; i < probabilities.size(); i++ )
            {
                this.writeBoxplotElement( poolDescription,
                                          metric,
                                          "PROBABILITY",
                                          "PROBABILITY",
                                          this.groupNumber + addToGroupNumber + i,
                                          probabilities.get( i ),
                                          writer );
            }

            // Add the quantiles
            List<Double> quantiles = next.getQuantilesList();

            for ( int i = 0; i < probabilities.size(); i++ )
            {
                this.writeBoxplotElement( poolDescription,
                                          metric,
                                          quantileValueTypeString,
                                          units,
                                          this.groupNumber + addToGroupNumber + i,
                                          quantiles.get( i ),
                                          writer );
            }

            // Increment the group number
            addToGroupNumber += probabilities.size();
        }

        // Increment the group number
        this.groupNumber += addToGroupNumber;
    }

    /**
     * Writes a box plot statistic.
     * 
     * @param poolDescription the pool descriptions
     * @param metric the metric
     * @param metricComponentName the metric component name
     * @param units the metric units
     * @param groupNumber the statistics group number
     * @param statistic the statistic
     * @param writer the writer
     * @throws IOException if the statistic could not be written
     */

    private void writeBoxplotElement( StringJoiner poolDescription,
                                      BoxplotMetric metric,
                                      String metricComponentName,
                                      String units,
                                      int groupNumber,
                                      double statistic,
                                      BufferedOutputStream writer )
            throws IOException
    {
        // Add a line separator for the next row
        writer.write( CsvStatisticsWriter.LINE_SEPARATOR.getBytes() );

        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

        // Add the pool description
        joiner.merge( poolDescription );

        // Add the metric name, pretty printed
        MetricConstants metricName = MetricConstants.valueOf( metric.getName().name() );
        this.append( joiner, metricName.toString(), false );

        // Add the component name            
        this.append( joiner, metricComponentName, false );

        // Add the metric component units
        this.append( joiner, units, false );

        // Add the minimum value
        this.append( joiner, String.valueOf( metric.getMinimum() ), false );

        // Add the maximum value
        this.append( joiner, String.valueOf( metric.getMaximum() ), false );

        // Add the optimum value
        this.append( joiner, String.valueOf( metric.getOptimum() ), false );

        // Add the statistics group number
        this.append( joiner, String.valueOf( groupNumber ), false );

        // Add the statistic value
        String formattedValue = this.getDecimalFormatter()
                                    .apply( statistic );
        this.append( joiner, formattedValue, false );

        // Write the row
        writer.write( joiner.toString().getBytes() );
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

    private Function<Double, String> getDecimalFormatter()
    {
        return this.decimalFormatter;
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
                                 Format decimalFormatter )
    {
        LOGGER.debug( "Creating a CSV format writer, which will write statistics to {}.", path );

        Objects.requireNonNull( path );
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( durationUnits );

        this.path = path;
        this.writeLock = new ReentrantLock();
        this.durationUnits = durationUnits;

        if ( Objects.nonNull( decimalFormatter ) )
        {
            this.decimalFormatter = decimalFormatter::format;
        }
        else
        {
            this.decimalFormatter = String::valueOf;
        }

        Duration duration = durationUnits.getDuration();
        BigDecimal nanoAdd = BigDecimal.valueOf( duration.getNano(), 9 );
        BigDecimal totalNanos = BigDecimal.valueOf( duration.getSeconds() )
                                          .add( nanoAdd );

        this.nanosPerDuration = totalNanos;

        // Create the evaluation description from the evaluation
        this.evaluationDescription = this.createEvaluationDescription( evaluation );

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
            this.writeHeader( this.bufferedWriter );

            // Write the CSVT file that helps with import into GDAL-enabled off-the-shelf GIS tools.
            this.writeCsvtFileForGdalApplications( path );
        }
        catch ( IOException e )
        {
            throw new CommaSeparatedWriteException( "Encountered an exception while building a CSV writer.", e );
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
            writer.write( CsvStatisticsWriter.HEADER.getBytes() );

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

        joiner.add( evaluation.getLeftVariableName() );
        joiner.add( evaluation.getRightVariableName() );

        if ( !evaluation.getBaselineVariableName().isBlank() )
        {
            joiner.add( evaluation.getBaselineVariableName() );
        }
        // Placeholder
        else
        {
            CsvStatisticsWriter.addEmptyValues( joiner, 1 );
        }

        return joiner;
    }

    /**
     * Adds a number of empty values inline.
     * 
     * @param StringJoiner joiner
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
        String name = path.getFileName().toString();
        name = name.replace( ".csv", ".csvt" );
        name = name.replace( ".gz", "" );

        // Resolve the path for the CSVT file
        Path pathToCsvt = path.getParent().resolve( name );

        String columnClasses = "\"String\",\"String\",\"String\",\"Integer\",\"String\",\"String\",\"WKT\",\"Integer\","
                               + "\"String\",\"String\",\"WKT\",\"Integer\",\"String\",\"String\",\"WKT\",\"Integer\","
                               + "\"String\",\"String\",\"String\",\"String\",\"String\",\"String\",\"String\","
                               + "\"String\",\"String\",\"String\",\"Real\",\"Real\",\"String\",\"Real\",\"Real\","
                               + "\"String\",\"String\",\"String\",\"Real\",\"Real\",\"String\",\"Real\",\"Real\","
                               + "\"String\",\"String\",\"String\",\"String\",\"String\",\"Real\",\"Real\",\"Real\","
                               + "\"Integer\",\"Real\"";

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

        Files.writeString( pathToCsvt,
                           columnClasses,
                           StandardCharsets.UTF_8,
                           StandardOpenOption.CREATE,
                           StandardOpenOption.TRUNCATE_EXISTING );
    }

}
