package wres.io.writing.commaseparated.statistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.Format;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

/**
 * Writes statistics to a file that contains comma separated values (CSV). There is one file per evaluation and hence
 * one instance of this writer per evaluation.
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
public class CsvStatisticsWriter implements Function<Statistics, Path>
{

    /**
     * A default name for the pairs.
     */

    public static final String DEFAULT_FILE_NAME = "evaluation.csv";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( CsvStatisticsWriter.class );

    /**
     * The file header.
     */

    private static final String HEADER = "LEFT VARIABLE NAME,RIGHT VARIABLE NAME,BASELINE VARIABLE NAME,EVALUATION "
                                         + "SUBJECT,LEFT FEATURE NAME,LEFT FEATURE WKT,LEFT FEATURE SRID,LEFT FEATURE "
                                         + "DESCRIPTION,RIGHT FEATURE NAME,RIGHT FEATURE WKT,RIGHT FEATURE SRID,RIGHT "
                                         + "FEATURE DESCRIPTION,BASELINE FEATURE NAME,BASELINE FEATURE WKT,BASELINE "
                                         + "FEATURE SRID,BASELINE FEATURE DESCRIPTION,EARLIEST ISSUED TIME EXCLUSIVE,"
                                         + "LATEST ISSUED TIME INCLUSIVE,EARLIEST VALID TIME EXCLUSIVE,LATEST VALID "
                                         + "TIME INCLUSIVE,EARLIEST LEAD DURATION EXCLUSIVE,LATEST LEAD DURATION "
                                         + "INCLUSIVE,TIME SCALE DURATION,TIME SCALE FUNCTION,EVENT THRESHOLD NAME,"
                                         + "EVENT THRESHOLD LOWER VALUE,EVENT THRESHOLD UPPER VALUE,EVENT THRESHOLD "
                                         + "UNITS,EVENT THRESHOLD LOWER PROBABILITY,EVENT THRESHOLD UPPER PROBABILITY,"
                                         + "EVENT THRESHOLD SIDE,EVENT THRESHOLD OPERATOR,DECISION THRESHOLD NAME,"
                                         + "DECISION THRESHOLD LOWER VALUE,DECISION THRESHOLD UPPER VALUE,DECISION "
                                         + "THRESHOLD UNITS,DECISION THRESHOLD LOWER PROBABILITY,DECISION THRESHOLD "
                                         + "UPPER PROBABILITY,DECISION THRESHOLD SIDE,DECISION THRESHOLD OPERATOR,"
                                         + "METRIC NAME,METRIC COMPONENT NAME,METRIC COMPONENT UNITS,STATISTIC";

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
     * Formatter for double values.
     */

    private final Format formatter;

    /**
     * Path to write.
     */

    @GuardedBy( "writeLock" )
    private final Path path;

    /**
     * Returns an instance, which writes to the prescribed path.
     * 
     * @param evaluation the evaluation description
     * @param path the path to write
     * @param formatter a formatter for double values
     * @return a writer instance
     * @throws NullPointerException if any input is null
     * @throws CommaSeparatedWriteException if the writer could not be created or the header written
     */

    public static CsvStatisticsWriter of( Evaluation evaluation, Path path, Format formatter )
    {
        return new CsvStatisticsWriter( evaluation, path, formatter );
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

        // Lock here so that all statistics appear in the same place within the file. 
        // Inevitably, this increases contention slightly, but is preferred to distributing the statistics within the 
        // file.
        ReentrantLock lock = this.getWriteLock();

        lock.lock();

        // One writer instance per blob of statistics
        // Could create one per file, but then this class becomes a resource to close
        try ( BufferedWriter writer = Files.newBufferedWriter( this.path,
                                                               StandardCharsets.UTF_8,
                                                               StandardOpenOption.APPEND ) )
        {
            this.writeStatistics( statistics, writer );
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
     */

    private StringJoiner getPoolDescription( Pool pool )
    {
        StringJoiner joiner = new StringJoiner( CsvStatisticsWriter.DELIMITER );

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

    private void writeStatistics( Statistics statistics, BufferedWriter writer ) throws IOException
    {
        Objects.requireNonNull( statistics );

        if ( !statistics.getScoresList().isEmpty() )
        {
            StringJoiner poolDescription = this.getPoolDescription( statistics.getPool() );

            // Merge the evaluation and pool descriptions into an empty joiner
            StringJoiner merge = new StringJoiner( CsvStatisticsWriter.DELIMITER );
            merge.merge( this.evaluationDescription );
            StringJoiner mergeDescription = merge.merge( poolDescription );

            this.writeScores( mergeDescription, statistics.getScoresList(), writer );
        }
    }

    /**
     * Writes a list of scores to the CSV file.
     * 
     * @param poolDescription the pool description
     * @param statistics the statistics
     * @param writer a shared writer, not to be closed
     * @throws IOException if the any of the scores could not be written
     */

    private void writeScores( StringJoiner poolDescription, List<DoubleScoreStatistic> scores, BufferedWriter writer )
            throws IOException
    {
        for ( DoubleScoreStatistic next : scores )
        {
            this.writeScore( poolDescription, next, writer );
        }
    }

    /**
     * Writes a list of scores to the CSV file.
     * 
     * @param poolDescription the pool description
     * @param score the score
     * @param writer a shared writer, not to be closed
     * @throws IOException if the score could not be written
     */

    private void writeScore( StringJoiner poolDescription, DoubleScoreStatistic score, BufferedWriter writer )
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
            MetricConstants namedMetric = MetricConstants.valueOf( metric.getName().name() );
            this.append( joiner, namedMetric.toString(), false );

            // Add the metric component name, pretty printed
            MetricConstants namedMetricComponent = MetricConstants.valueOf( metricComponent.getName().name() );
            this.append( joiner, namedMetricComponent.toString(), false );

            // Add the metric component units
            this.append( joiner, metricComponent.getUnits(), false );

            // Add the statistic value
            String formattedValue = formatter.format( next.getValue() );
            this.append( joiner, formattedValue, false );

            // Write the row
            writer.write( joiner.toString() );
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
     * Hidden constructor.
     * 
     * @param evaluation the evaluation description
     * @param path the path to the csv file to write
     * @param formatter a formatter for double values
     * @throws NullPointerException if any input is null
     * @throws CommaSeparatedWriteException if the writer could not be created or the header written
     */

    private CsvStatisticsWriter( Evaluation evaluation, Path path, Format formatter )
    {
        Objects.requireNonNull( path );
        Objects.requireNonNull( formatter );
        Objects.requireNonNull( evaluation );

        this.path = path;
        this.formatter = formatter;
        this.writeLock = new ReentrantLock();

        // Create the evaluation description from the evaluation
        this.evaluationDescription = this.createEvaluationDescription( evaluation );

        try ( BufferedWriter writer = Files.newBufferedWriter( this.path,
                                                               StandardCharsets.UTF_8,
                                                               StandardOpenOption.CREATE,
                                                               StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            // Write the header
            this.writeHeader( writer );
        }
        catch ( IOException e )
        {
            throw new CommaSeparatedWriteException( "Encountered an exceptiion while building a CSV writer.", e );
        }
    }

    /**
     * Writes a header to the CSV file.
     * @param writer the writer
     * @throws IOException if the header could not be written
     */

    private void writeHeader( BufferedWriter writer ) throws IOException
    {
        // Lock for writing
        ReentrantLock lock = this.getWriteLock();
        lock.lock();

        LOGGER.trace( "Acquired writing lock on {}", this.getPath() );

        try
        {
            writer.write( CsvStatisticsWriter.HEADER );

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

}
