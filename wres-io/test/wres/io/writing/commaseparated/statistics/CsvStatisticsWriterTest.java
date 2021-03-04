package wres.io.writing.commaseparated.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.io.writing.WriterTestHelper;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;

/**
 * Tests the writing of score outputs to a file of Comma Separated Values (CSV).
 */

class CsvStatisticsWriterTest
{

    /**
     * In-memory file system for testing.
     */

    private FileSystem fileSystem;

    /**
     * Fixed header to expect.
     */

    private static final String LINE_ZERO_EXPECTED = "LEFT VARIABLE NAME,RIGHT VARIABLE NAME,BASELINE VARIABLE NAME,"
                                                     + "POOL NUMBER,EVALUATION SUBJECT,LEFT FEATURE NAME,LEFT FEATURE "
                                                     + "WKT,LEFT FEATURE SRID,LEFT FEATURE DESCRIPTION,RIGHT FEATURE "
                                                     + "NAME,RIGHT FEATURE WKT,RIGHT FEATURE SRID,RIGHT FEATURE "
                                                     + "DESCRIPTION,BASELINE FEATURE NAME,BASELINE FEATURE WKT,"
                                                     + "BASELINE FEATURE SRID,BASELINE FEATURE DESCRIPTION,EARLIEST "
                                                     + "ISSUED TIME EXCLUSIVE,LATEST ISSUED TIME INCLUSIVE,EARLIEST "
                                                     + "VALID TIME EXCLUSIVE,LATEST VALID TIME INCLUSIVE,EARLIEST LEAD "
                                                     + "DURATION EXCLUSIVE,LATEST LEAD DURATION INCLUSIVE,TIME SCALE "
                                                     + "DURATION,TIME SCALE FUNCTION,EVENT THRESHOLD NAME,EVENT "
                                                     + "THRESHOLD LOWER VALUE,EVENT THRESHOLD UPPER VALUE,EVENT "
                                                     + "THRESHOLD UNITS,EVENT THRESHOLD LOWER PROBABILITY,EVENT "
                                                     + "THRESHOLD UPPER PROBABILITY,EVENT THRESHOLD SIDE,EVENT "
                                                     + "THRESHOLD OPERATOR,DECISION THRESHOLD NAME,DECISION THRESHOLD "
                                                     + "LOWER VALUE,DECISION THRESHOLD UPPER VALUE,DECISION THRESHOLD "
                                                     + "UNITS,DECISION THRESHOLD LOWER PROBABILITY,DECISION THRESHOLD "
                                                     + "UPPER PROBABILITY,DECISION THRESHOLD SIDE,DECISION THRESHOLD "
                                                     + "OPERATOR,METRIC NAME,METRIC COMPONENT NAME,METRIC COMPONENT "
                                                     + "UNITS,STATISTIC GROUP NUMBER,STATISTIC";

    @BeforeEach
    void runBeforeEachTest()
    {
        this.fileSystem = Jimfs.newFileSystem( Configuration.unix() );
    }

    @Test
    void testWriteDoubleScores() throws IOException
    {
        // Get some raw scores and an imaginary evaluation
        Statistics statistics = this.getDoubleScoreStatistics();
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";
        Path directory = this.fileSystem.getPath( "test" );
        Files.createDirectory( directory );
        Path pathToStore = this.fileSystem.getPath( "test", fileName );
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

        String lineOneExpected = "QINE,SQIN,,1,RIGHT,DRRC2,,,,DRRC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,PT0S,UNKNOWN,,-Infinity,,,,,LEFT,"
                                 + "GREATER,,,,,,,,,MEAN SQUARE ERROR,MAIN,,1,1.0";

        assertEquals( lineOneExpected, actual.get( 1 ) );

        String lineTwoExpected = "QINE,SQIN,,1,RIGHT,DRRC2,,,,DRRC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,PT0S,UNKNOWN,,-Infinity,,,,,LEFT,"
                                 + "GREATER,,,,,,,,,MEAN ERROR,MAIN,,2,2.0";

        assertEquals( lineTwoExpected, actual.get( 2 ) );

        String lineThreeExpected = "QINE,SQIN,,1,RIGHT,DRRC2,,,,DRRC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                   + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                   + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                   + "LEFT,GREATER,,,,,,,,,MEAN ABSOLUTE ERROR,MAIN,,3,3.0";

        assertEquals( lineThreeExpected, actual.get( 3 ) );
    }

    @Test
    void testWriteDurationScores() throws IOException
    {
        // Get some duration score statistics
        Statistics statistics = this.getDurationScoreStatistics();
        Evaluation evaluation = this.getEvaluation();

        // Create a path on an in-memory file system
        String fileName = "evaluation.csv";
        Path directory = this.fileSystem.getPath( "test" );
        Files.createDirectory( directory );
        Path pathToStore = this.fileSystem.getPath( "test", fileName );
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

        String lineOneExpected = "QINE,SQIN,,1,RIGHT,DOLC2,,,,DOLC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,LEFT,"
                                 + "GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,MEAN,SECONDS,1,3600.000000000";

        assertEquals( lineOneExpected, actual.get( 1 ) );

        String lineTwoExpected = "QINE,SQIN,,1,RIGHT,DOLC2,,,,DOLC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                 + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,LEFT,"
                                 + "GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,MEDIAN,SECONDS,2,7200.000000000";

        assertEquals( lineTwoExpected, actual.get( 2 ) );

        String lineThreeExpected = "QINE,SQIN,,1,RIGHT,DOLC2,,,,DOLC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                   + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                   + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                   + "LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,MAXIMUM,SECONDS,3,"
                                   + "10800.000000000";

        assertEquals( lineThreeExpected, actual.get( 3 ) );
    }

    /**
     * @return statistics that include double scores.
     * @throws IOException
     */

    private Statistics getDoubleScoreStatistics() throws IOException
    {
        // Get some raw scores
        List<DoubleScoreStatisticOuter> scores = WriterTestHelper.getScoreStatisticsForOnePool();

        Pool pool = scores.get( 0 )
                          .getMetadata()
                          .getPool();

        return Statistics.newBuilder()
                         .addAllScores( scores.stream()
                                              .map( DoubleScoreStatisticOuter::getData )
                                              .collect( Collectors.toList() ) )
                         .setPool( pool )
                         .build();
    }

    /**
     * @return statistics that include duration scores.
     * @throws IOException
     */

    private Statistics getDurationScoreStatistics() throws IOException
    {
        // Get some raw scores
        List<DurationScoreStatisticOuter> scores = WriterTestHelper.getDurationScoreStatisticsForOnePool();

        Pool pool = scores.get( 0 )
                          .getMetadata()
                          .getPool();

        return Statistics.newBuilder()
                         .addAllDurationScores( scores.stream()
                                                      .map( DurationScoreStatisticOuter::getData )
                                                      .collect( Collectors.toList() ) )
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
