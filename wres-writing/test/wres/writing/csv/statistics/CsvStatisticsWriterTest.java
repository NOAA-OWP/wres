package wres.writing.csv.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.writing.WriterTestHelper;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.SummaryStatistic;

/**
 * Tests the writing of statistics formatted as Comma Separated Values (CSV) Version 2 (CSV2).
 */

class CsvStatisticsWriterTest
{
    /** Fixed header to expect. */
    private static final String LINE_ZERO_EXPECTED =
            "LEFT VARIABLE NAME,RIGHT VARIABLE NAME,BASELINE VARIABLE NAME,COVARIATE "
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

    @Test
    void testWriteDoubleScores() throws IOException
    {
        // Get some raw scores and an imaginary evaluation
        Statistics statistics = this.getDoubleScoreStatistics( false, 1, false );
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 4, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            String lineOneExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,"
                                     + "PT1H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ABSOLUTE ERROR,"
                                     + "MAIN,,,0.0,Infinity,0.0,1,,,,,,,3.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineTwoExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ERROR,MAIN,,,-Infinity,"
                                     + "Infinity,0.0,2,,,,,,,2.0";

            assertEquals( lineTwoExpected, actual.get( 2 ) );

            String lineThreeExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                       + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN SQUARE ERROR,MAIN,,,0.0,"
                                       + "Infinity,0.0,3,,,,,,,1.0";

            assertEquals( lineThreeExpected, actual.get( 3 ) );
        }
    }

    @Test
    void testWriteDoubleScoresForSeparateBaseline() throws IOException
    {
        // Get some raw scores and an imaginary evaluation
        Statistics statistics = this.getDoubleScoreStatistics( true, 1, false );
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 4, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            String lineOneExpected = "QINE,SQIN,,,1,BASELINE,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ABSOLUTE ERROR,MAIN,,,"
                                     + "0.0,Infinity,0.0,1,,,,,,,3.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineTwoExpected = "QINE,SQIN,,,1,BASELINE,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ERROR,MAIN,,,-Infinity,"
                                     + "Infinity,0.0,2,,,,,,,2.0";

            assertEquals( lineTwoExpected, actual.get( 2 ) );

            String lineThreeExpected = "QINE,SQIN,,,1,BASELINE,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                       + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN SQUARE ERROR,MAIN,,,0.0,"
                                       + "Infinity,0.0,3,,,,,,,1.0";

            assertEquals( lineThreeExpected, actual.get( 3 ) );
        }
    }

    @Test
    void testWriteDurationScores() throws IOException
    {
        // Get some duration score statistics
        Statistics statistics = this.getDurationScoreStatistics();
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 4, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            String lineOneExpected = "QINE,SQIN,,,1,RIGHT,\"DOLC2-DOLC2\",\"DOLC2\",,,,\"DOLC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,"
                                     + "MEAN,ENSEMBLE MEDIAN,SECONDS,0.000000000,0.000000000,0.000000000,1,,,,,,,"
                                     + "3600.000000000";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineTwoExpected = "QINE,SQIN,,,1,RIGHT,\"DOLC2-DOLC2\",\"DOLC2\",,,,\"DOLC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,"
                                     + "MEDIAN,ENSEMBLE MEDIAN,SECONDS,0.000000000,0.000000000,0.000000000,2,,,,,,,"
                                     + "7200.000000000";

            assertEquals( lineTwoExpected, actual.get( 2 ) );

            String lineThreeExpected = "QINE,SQIN,,,1,RIGHT,\"DOLC2-DOLC2\",\"DOLC2\",,,,\"DOLC2\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,"
                                       + "PT18H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR "
                                       + "STATISTIC,MAXIMUM,ENSEMBLE MEDIAN,SECONDS,0.000000000,0.000000000,"
                                       + "0.000000000,3,,,,,,,10800.000000000";

            assertEquals( lineThreeExpected, actual.get( 3 ) );
        }
    }

    @Test
    void testWriteBoxPlot() throws IOException
    {
        // Get some box plot score statistics for both pairs and pools
        Statistics statistics = this.getBoxplotStatistics();
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 54, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            // Make selected assertions        
            String lineOneExpected = "QINE,SQIN,,,1,RIGHT,\"JUNP1-JUNP1\",\"JUNP1\",,,,\"JUNP1\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,BOX PLOT OF ERRORS BY "
                                     + "OBSERVED VALUE,OBSERVED VALUE,,,-Infinity,Infinity,0.0,1,,,,,,,1.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineEightExpected = "QINE,SQIN,,,1,RIGHT,\"JUNP1-JUNP1\",\"JUNP1\",,,,\"JUNP1\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,"
                                       + "PT24H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,BOX PLOT OF ERRORS "
                                       + "BY OBSERVED VALUE,FORECAST ERROR,,,-Infinity,Infinity,0.0,2,,,,,,,3.0";

            assertEquals( lineEightExpected, actual.get( 8 ) );

            String lineNineteenExpected = "QINE,SQIN,,,1,RIGHT,\"JUNP1-JUNP1\",\"JUNP1\",,,,\"JUNP1\",,,,,,,,"
                                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,"
                                          + "PT24H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,BOX PLOT OF "
                                          + "ERRORS BY OBSERVED VALUE,FORECAST ERROR,,,-Infinity,Infinity,0.0,7,,,,,,,"
                                          + "9.0";

            assertEquals( lineNineteenExpected, actual.get( 19 ) );

            String lineThirtyOneExpected = "QINE,SQIN,,,1,RIGHT,\"JUNP1-JUNP1\",\"JUNP1\",,,,\"JUNP1\",,,,,,,,"
                                           + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                           + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,"
                                           + "PT24H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,BOX PLOT OF "
                                           + "ERRORS BY OBSERVED VALUE,FORECAST ERROR,,,-Infinity,Infinity,0.0,13,,,,,,"
                                           + ",27.0";

            assertEquals( lineThirtyOneExpected, actual.get( 31 ) );

            String lineFiftyTwoExpected = "QINE,SQIN,,,1,RIGHT,\"JUNP1-JUNP1\",\"JUNP1\",,,,\"JUNP1\",,,,,,,,"
                                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                          + "PT24H,PT24H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,"
                                          + "BOX PLOT OF ERRORS,FORECAST ERROR,,,-Infinity,Infinity,0.0,24,,,,,,,77.0";

            assertEquals( lineFiftyTwoExpected, actual.get( 52 ) );
        }
    }

    @Test
    void testWriteDiagram() throws IOException
    {
        // Get some statistics with a reliability diagram
        Statistics statistics = this.getDiagramStatistics();
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 16, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            // Make selected assertions        
            String lineThreeExpected = "QINE,SQIN,,,1,RIGHT,\"CREC1-CREC1\",\"CREC1\",,,,\"CREC1\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,"
                                       + "PT24H,,,,,,11.94128,,,0.9,,LEFT,GREATER EQUAL,,,,,,,,,RELIABILITY "
                                       + "DIAGRAM,FORECAST PROBABILITY,,,0.0,0.0,,3,,,,,,,0.50723";

            assertEquals( lineThreeExpected, actual.get( 3 ) );

            String lineEightExpected = "QINE,SQIN,,,1,RIGHT,\"CREC1-CREC1\",\"CREC1\",,,,\"CREC1\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,"
                                       + "PT24H,,,,,,11.94128,,,0.9,,LEFT,GREATER EQUAL,,,,,,,,,"
                                       + "RELIABILITY DIAGRAM,OBSERVED RELATIVE FREQUENCY,,,0.0,0.0,,3,,,,,,,0.5";

            assertEquals( lineEightExpected, actual.get( 8 ) );

            String lineThirteenExpected = "QINE,SQIN,,,1,RIGHT,\"CREC1-CREC1\",\"CREC1\",,,,\"CREC1\",,,,,,,,"
                                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,"
                                          + "PT24H,,,,,,11.94128,,,0.9,,LEFT,GREATER EQUAL,,,,,,,,,"
                                          + "RELIABILITY DIAGRAM,SAMPLE SIZE,,,0.0,"
                                          + "0.0,,3,,,,,,,540.0";

            assertEquals( lineThirteenExpected, actual.get( 13 ) );
        }
    }

    @Test
    void testWriteDurationDiagram() throws IOException
    {
        // Get some statistics with a timing error diagram
        Statistics statistics = this.getDurationDiagramStatistics();
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 7, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            // Make selected assertions        
            String lineThreeExpected = "QINE,SQIN,,,1,RIGHT,\"FTSC1-FTSC1\",\"FTSC1\",,,,\"FTSC1\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,"
                                       + "PT18H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR,"
                                       + "UNKNOWN,,SECONDS FROM 1970-01-01T00:00:00Z,,,,2,,,,,,,473472000.000000000";

            assertEquals( lineThreeExpected, actual.get( 3 ) );

            String lineFourExpected = "QINE,SQIN,,,1,RIGHT,\"FTSC1-FTSC1\",\"FTSC1\",,,,\"FTSC1\",,,,,,,,"
                                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,"
                                      + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR,ERROR,ENSEMBLE MEAN,"
                                      + "SECONDS,-9223372036854775808.000000000,0.000000000,0.000000000,2,,,,,,,"
                                      + "7200.000000000";

            assertEquals( lineFourExpected, actual.get( 4 ) );
        }
    }

    /**
     * See #97539.
     * @throws IOException if a write error occurs
     * @throws InterruptedException if a thread is interrupted
     * @throws ExecutionException if a write fails for any other reason
     */

    @RepeatedTest( 100 )
    void testMultithreadedWriteIsDeterministic() throws IOException, InterruptedException, ExecutionException
    {
        // Get some raw scores for two different pools and an imaginary evaluation
        Statistics statisticsOne = this.getDoubleScoreStatistics( false, 1, false );
        Statistics statisticsTwo = this.getDoubleScoreStatistics( true, 2, false );

        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";

        ExecutorService executor = Executors.newFixedThreadPool( 2 );

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );
            CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation, csvPath, false );

            // Create the writer and submit two separate writes to the two-threaded executor
            Runnable runOne = () -> writer.apply( statisticsOne );
            Runnable runTwo = () -> writer.apply( statisticsTwo );

            Future<?> one = executor.submit( runOne );
            Future<?> two = executor.submit( runTwo );

            // Complete the writes
            one.get();
            two.get();

            // Flush the writes by closing
            writer.close();

            // Read the lines written, sort them in natural order, and assert against them
            List<String> actual = Files.readAllLines( pathToStore );
            Collections.sort( actual );

            assertEquals( 7, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            String lineOneExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,"
                                     + "PT1H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ABSOLUTE ERROR,"
                                     + "MAIN,,,0.0,Infinity,0.0,1,,,,,,,3.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );
        }
    }

    @Test
    void testWriteDoubleScoresWithSampleQuantile() throws IOException
    {
        // Get some raw scores and an imaginary evaluation
        Statistics statistics = this.getDoubleScoreStatistics( false, 1, true );
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 4, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            String lineOneExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,"
                                     + "PT1H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ABSOLUTE ERROR,"
                                     + "MAIN,,,0.0,Infinity,0.0,1,QUANTILE,,,RESAMPLED,,0.95,3.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineTwoExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ERROR,MAIN,,,-Infinity,"
                                     + "Infinity,0.0,2,QUANTILE,,,RESAMPLED,,0.95,2.0";

            assertEquals( lineTwoExpected, actual.get( 2 ) );

            String lineThreeExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                       + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN SQUARE ERROR,MAIN,,,0.0,"
                                       + "Infinity,0.0,3,QUANTILE,,,RESAMPLED,,0.95,1.0";

            assertEquals( lineThreeExpected, actual.get( 3 ) );
        }
    }

    @Test
    void testWriteDoubleScoresWithSummaryStatistic() throws IOException
    {
        // Get some raw scores and an imaginary evaluation
        Statistics statistics = this.getDoubleScoreStatistics( false, 1, false );

        // Add a summary statistic over geographic features
        SummaryStatistic summaryStatistic =
                SummaryStatistic.newBuilder()
                                .setStatistic( SummaryStatistic.StatisticName.MAXIMUM )
                                .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                .build();
        statistics = statistics.toBuilder()
                               .setSummaryStatistic( summaryStatistic )
                               .build();

        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", fileName );
            Path csvPath = Files.createFile( pathToStore );

            // Create the writer and write
            try ( CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                       csvPath,
                                                                       false ) )
            {
                writer.apply( statistics );
            }

            // Assert content
            List<String> actual = Files.readAllLines( pathToStore );

            assertEquals( 4, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            String lineOneExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,"
                                     + "PT1H,,,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ABSOLUTE ERROR,"
                                     + "MAIN,,,0.0,Infinity,0.0,1,MAXIMUM,,,FEATURES,,,3.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineTwoExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                     + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                     + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN ERROR,MAIN,,,-Infinity,"
                                     + "Infinity,0.0,2,MAXIMUM,,,FEATURES,,,2.0";

            assertEquals( lineTwoExpected, actual.get( 2 ) );

            String lineThreeExpected = "QINE,SQIN,,,1,RIGHT,\"DRRC2-DRRC2\",\"DRRC2\",,,,\"DRRC2\",,,,,,,,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                       + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,"
                                       + ",,,,,-Infinity,,,,,LEFT,GREATER,,,,,,,,,MEAN SQUARE ERROR,MAIN,,,0.0,"
                                       + "Infinity,0.0,3,MAXIMUM,,,FEATURES,,,1.0";

            assertEquals( lineThreeExpected, actual.get( 3 ) );
        }
    }

    /**
     * @param isBaselinePool is true for a baseline pool, false for a regular pool
     * @param poolNumber the pool number
     * @param addSampleQuantile is true to add a sample quantile, false otherwise
     * @return statistics that include double scores.
     */

    private Statistics getDoubleScoreStatistics( boolean isBaselinePool,
                                                 int poolNumber,
                                                 boolean addSampleQuantile )
    {
        // Get some raw scores
        List<DoubleScoreStatisticOuter> scores = WriterTestHelper.getScoreStatisticsForOnePool();

        Pool pool = scores.get( 0 )
                          .getPoolMetadata()
                          .getPool();

        Statistics.Builder builder = Statistics.newBuilder()
                                               .addAllScores( scores.stream()
                                                                    .map( DoubleScoreStatisticOuter::getStatistic )
                                                                    .toList() );
        if ( addSampleQuantile )
        {
            SummaryStatistic quantile = SummaryStatistic.newBuilder()
                                                        .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                        .setDimension( SummaryStatistic.StatisticDimension.RESAMPLED )
                                                        .setProbability( 0.95 )
                                                        .build();

            builder.setSummaryStatistic( quantile );
        }

        if ( isBaselinePool )
        {
            builder.setBaselinePool( pool.toBuilder()
                                         .setIsBaselinePool( true )
                                         .setPoolId( poolNumber ) );
        }
        else
        {
            builder.setPool( pool.toBuilder()
                                 .setPoolId( poolNumber ) );
        }

        return builder.build();
    }

    /**
     * @return statistics that include duration scores.
     */

    private Statistics getDurationScoreStatistics()
    {
        // Get some raw scores
        List<DurationScoreStatisticOuter> scores = WriterTestHelper.getDurationScoreStatisticsForOnePool();

        Pool pool = scores.get( 0 )
                          .getPoolMetadata()
                          .getPool()
                          .toBuilder()
                          .setEnsembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                          .build();

        return Statistics.newBuilder()
                         .addAllDurationScores( scores.stream()
                                                      .map( DurationScoreStatisticOuter::getStatistic )
                                                      .toList() )
                         .setPool( pool )
                         .build();
    }

    /**
     * @return statistics that include a duration diagram.
     */

    private Statistics getDurationDiagramStatistics()
    {
        // Get some a timing error diagram
        List<DurationDiagramStatisticOuter> scores = WriterTestHelper.getTimeToPeakErrorsForOnePool();

        Pool pool = scores.get( 0 )
                          .getPoolMetadata()
                          .getPool().toBuilder()
                          .setEnsembleAverageType( Pool.EnsembleAverageType.MEAN )
                          .build();

        return Statistics.newBuilder()
                         .addAllDurationDiagrams( scores.stream()
                                                        .map( DurationDiagramStatisticOuter::getStatistic )
                                                        .toList() )
                         .setPool( pool )
                         .build();
    }

    /**
     * @return statistics that include boxplots.
     */

    private Statistics getBoxplotStatistics()
    {
        // Get some box plots per pair
        List<BoxplotStatisticOuter> boxesPaired = WriterTestHelper.getBoxPlotPerPairForOnePool();

        // Get some box plots per pool: #104065
        List<BoxplotStatisticOuter> boxesPooled = WriterTestHelper.getBoxPlotPerPoolForTwoPools();

        Pool pool = boxesPaired.get( 0 )
                               .getPoolMetadata()
                               .getPool();

        return Statistics.newBuilder()
                         .addAllOneBoxPerPair( boxesPaired.stream()
                                                          .map( BoxplotStatisticOuter::getStatistic )
                                                          .toList() )
                         .addAllOneBoxPerPool( boxesPooled.stream()
                                                          .map( BoxplotStatisticOuter::getStatistic )
                                                          .toList() )
                         .setPool( pool )
                         .build();
    }

    /**
     * @return statistics that include a reliability diagram.
     */

    private Statistics getDiagramStatistics()
    {
        // Get a diagram
        List<DiagramStatisticOuter> boxes = WriterTestHelper.getReliabilityDiagramForOnePool();

        Pool pool = boxes.get( 0 )
                         .getPoolMetadata()
                         .getPool();

        return Statistics.newBuilder()
                         .addAllDiagrams( boxes.stream()
                                               .map( DiagramStatisticOuter::getStatistic )
                                               .toList() )
                         .setPool( pool )
                         .build();
    }

    /**
     * @return an imaginary evaluation for testing.
     */

    private Evaluation getEvaluation()
    {
        return Evaluation.newBuilder()
                         .setRightDataName( "HEFS" )
                         .setBaselineDataName( "ESP" )
                         .setLeftVariableName( "QINE" )
                         .setRightVariableName( "SQIN" )
                         .setMeasurementUnit( "CMS" )
                         .build();
    }

}
