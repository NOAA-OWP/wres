package wres.io.writing.commaseparated.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.io.writing.WriterTestHelper;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;

/**
 * Tests the writing of statistics formatted as Comma Separated Values (CSV) Version 2 (CSV2).
 */

class CsvStatisticsWriterTest
{

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
                                                     + "UNITS,METRIC COMPONENT MINIMUM,METRIC COMPONENT MAXIMUM,METRIC "
                                                     + "COMPONENT OPTIMUM,STATISTIC GROUP NUMBER,STATISTIC";

    @Test
    void testWriteDoubleScores() throws IOException
    {
        // Get some raw scores and an imaginary evaluation
        Statistics statistics = this.getDoubleScoreStatistics();
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

            String lineOneExpected = "QINE,SQIN,,1,RIGHT,DRRC2,,,,DRRC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                     + "LEFT,GREATER,,,,,,,,,MEAN SQUARE ERROR,MAIN,,0.0,Infinity,0.0,1,1.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineTwoExpected = "QINE,SQIN,,1,RIGHT,DRRC2,,,,DRRC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                     + "LEFT,GREATER,,,,,,,,,MEAN ERROR,MAIN,,-Infinity,Infinity,0.0,2,2.0";

            assertEquals( lineTwoExpected, actual.get( 2 ) );

            String lineThreeExpected = "QINE,SQIN,,1,RIGHT,DRRC2,,,,DRRC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT1H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                       + "LEFT,GREATER,,,,,,,,,MEAN ABSOLUTE ERROR,MAIN,,0.0,Infinity,0.0,3,3.0";

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

            String lineOneExpected = "QINE,SQIN,,1,RIGHT,DOLC2,,,,DOLC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                     + "LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,MEAN,SECONDS,0.000000000,"
                                     + "0.000000000,0.000000000,1,3600.000000000";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineTwoExpected = "QINE,SQIN,,1,RIGHT,DOLC2,,,,DOLC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                     + "LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,MEDIAN,SECONDS,0.000000000,"
                                     + "0.000000000,0.000000000,2,7200.000000000";

            assertEquals( lineTwoExpected, actual.get( 2 ) );

            String lineThreeExpected = "QINE,SQIN,,1,RIGHT,DOLC2,,,,DOLC2,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                       + "LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR STATISTIC,MAXIMUM,SECONDS,"
                                       + "0.000000000,0.000000000,0.000000000,3,10800.000000000";

            assertEquals( lineThreeExpected, actual.get( 3 ) );
        }
    }

    @Test
    void testWriteBoxPlot() throws IOException
    {
        // Get some box plot score statistics
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

            assertEquals( 34, actual.size() );

            assertEquals( CsvStatisticsWriterTest.LINE_ZERO_EXPECTED, actual.get( 0 ) );

            // Make selected assertions        
            String lineOneExpected = "QINE,SQIN,,1,RIGHT,JUNP1,,,,JUNP1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                     + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                     + "LEFT,GREATER,,,,,,,,,BOX PLOT OF ERRORS BY OBSERVED VALUE,OBSERVED VALUE,,"
                                     + "-Infinity,Infinity,0.0,1,1.0";

            assertEquals( lineOneExpected, actual.get( 1 ) );

            String lineEightExpected = "QINE,SQIN,,1,RIGHT,JUNP1,,,,JUNP1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,PT0S,UNKNOWN,,-Infinity,"
                                       + ",,,,LEFT,GREATER,,,,,,,,,BOX PLOT OF ERRORS BY OBSERVED VALUE,FORECAST ERROR,"
                                       + ",-Infinity,Infinity,0.0,2,3.0";

            assertEquals( lineEightExpected, actual.get( 8 ) );

            String lineNineteenExpected = "QINE,SQIN,,1,RIGHT,JUNP1,,,,JUNP1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                          + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                          + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,PT0S,UNKNOWN,,-Infinity,"
                                          + ",,,,LEFT,GREATER,,,,,,,,,BOX PLOT OF ERRORS BY OBSERVED VALUE,"
                                          + "FORECAST ERROR,,-Infinity,Infinity,0.0,7,9.0";

            assertEquals( lineNineteenExpected, actual.get( 19 ) );

            String lineThirtyOneExpected = "QINE,SQIN,,1,RIGHT,JUNP1,,,,JUNP1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                           + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                           + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,PT0S,UNKNOWN,,"
                                           + "-Infinity,,,,,LEFT,GREATER,,,,,,,,,BOX PLOT OF ERRORS BY OBSERVED VALUE,"
                                           + "FORECAST ERROR,,-Infinity,Infinity,0.0,13,27.0";

            assertEquals( lineThirtyOneExpected, actual.get( 31 ) );
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
            String lineThreeExpected = "QINE,SQIN,,1,RIGHT,CREC1,,,,CREC1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,PT0S,UNKNOWN,,11.94128,,,"
                                       + "0.9,,LEFT,GREATER EQUAL,,,,,,,,,RELIABILITY DIAGRAM,FORECAST PROBABILITY,,"
                                       + "0.0,0.0,,3,0.50723";

            assertEquals( lineThreeExpected, actual.get( 3 ) );

            String lineEightExpected = "QINE,SQIN,,1,RIGHT,CREC1,,,,CREC1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,PT0S,UNKNOWN,,11.94128,,,"
                                       + "0.9,,LEFT,GREATER EQUAL,,,,,,,,,RELIABILITY DIAGRAM,"
                                       + "OBSERVED RELATIVE FREQUENCY,,0.0,0.0,,3,0.5";

            assertEquals( lineEightExpected, actual.get( 8 ) );

            String lineThirteenExpected = "QINE,SQIN,,1,RIGHT,CREC1,,,,CREC1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                          + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                          + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H,PT0S,UNKNOWN,,11.94128,"
                                          + ",,0.9,,LEFT,GREATER EQUAL,,,,,,,,,RELIABILITY DIAGRAM,SAMPLE SIZE,,0.0,"
                                          + "0.0,,3,540.0";

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
            String lineThreeExpected = "QINE,SQIN,,1,RIGHT,FTSC1,,,,FTSC1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                       + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                       + "LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR,UNKNOWN,"
                                       + "SECONDS FROM 1970-01-01T00:00:00Z,,,,2,473472000.000000000";

            assertEquals( lineThreeExpected, actual.get( 3 ) );

            String lineFourExpected = "QINE,SQIN,,1,RIGHT,FTSC1,,,,FTSC1,,,,,,,,-1000000000-01-01T00:00:00Z,"
                                      + "+1000000000-12-31T23:59:59.999999999Z,-1000000000-01-01T00:00:00Z,"
                                      + "+1000000000-12-31T23:59:59.999999999Z,PT1H,PT18H,PT0S,UNKNOWN,,-Infinity,,,,,"
                                      + "LEFT,GREATER,,,,,,,,,TIME TO PEAK ERROR,ERROR,SECONDS,"
                                      + "-9223372036854775808.000000000,0.000000000,0.000000000,2,7200.000000000";

            assertEquals( lineFourExpected, actual.get( 4 ) );
        }
    }

    /**
     * @return statistics that include double scores.
     */

    private Statistics getDoubleScoreStatistics()
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
     */

    private Statistics getDurationScoreStatistics()
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
     * @return statistics that include a duration diagram.
     */

    private Statistics getDurationDiagramStatistics()
    {
        // Get some a timing error diagram
        List<DurationDiagramStatisticOuter> scores = WriterTestHelper.getTimeToPeakErrorsForOnePool();

        Pool pool = scores.get( 0 )
                          .getMetadata()
                          .getPool();

        return Statistics.newBuilder()
                         .addAllDurationDiagrams( scores.stream()
                                                        .map( DurationDiagramStatisticOuter::getData )
                                                        .collect( Collectors.toList() ) )
                         .setPool( pool )
                         .build();
    }

    /**
     * @return statistics that include boxplots.
     */

    private Statistics getBoxplotStatistics()
    {
        // Get some box plots
        List<BoxplotStatisticOuter> boxes = WriterTestHelper.getBoxPlotPerPairForOnePool();

        Pool pool = boxes.get( 0 )
                         .getMetadata()
                         .getPool();

        return Statistics.newBuilder()
                         .addAllOneBoxPerPair( boxes.stream()
                                                    .map( BoxplotStatisticOuter::getData )
                                                    .collect( Collectors.toList() ) )
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
                         .getMetadata()
                         .getPool();

        return Statistics.newBuilder()
                         .addAllDiagrams( boxes.stream()
                                               .map( DiagramStatisticOuter::getData )
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
