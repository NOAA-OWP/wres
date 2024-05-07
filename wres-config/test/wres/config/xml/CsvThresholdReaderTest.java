package wres.config.xml;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.xml.generated.ThresholdFormat;
import wres.config.xml.generated.ThresholdOperator;
import wres.config.xml.generated.ThresholdType;
import wres.config.xml.generated.ThresholdsConfig;
import wres.statistics.generated.Threshold;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Tests the {@link CsvThresholdReader}.
 *
 * @author James Brown
 */
@Deprecated( since = "6.14", forRemoval = true )
class CsvThresholdReaderTest
{
    private static final String TEST_CSV = "test.csv";
    private static final String TEST = "test";
    private static final String UNIT_STRING = "CMS";

    @Test
    void testProbabilityThresholdsWithLabels() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "locationId, A, B, C\n" )
                      .append( "DRRC2, 0.4, 0.6, 0.8\n" )
                      .append( "DOLC2, 0.2, 0.3, 0.7\n" );
            }

            URI uri = csvPath.toUri();
            ThresholdsConfig.Source source = new ThresholdsConfig.Source( uri,
                                                                          ThresholdFormat.CSV,
                                                                          "CMS",
                                                                          "-999",
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null );
            ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                                     wres.config.xml.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            Map<String, Set<Threshold>> actual =
                    CsvThresholdReader.readThresholds( thresholdConfig,
                                                       UNIT_STRING );

            // Compare to expected
            Map<String, Set<Threshold>> expected = new TreeMap<>();

            Threshold one = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.4 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "A" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.6 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "B" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setLeftThresholdProbability( 0.8 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT )
                                       .setName( "C" )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();

            Set<Threshold> first = new HashSet<>();
            first.add( one );
            first.add( two );
            first.add( three );

            expected.put( "DRRC2", first );

            Threshold four = Threshold.newBuilder()
                                      .setLeftThresholdProbability( 0.2 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setName( "A" )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setLeftThresholdProbability( 0.3 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setName( "B" )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "C" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();

            Set<Threshold> second = new HashSet<>();
            second.add( four );
            second.add( five );
            second.add( six );

            expected.put( "DOLC2", second );

            // Compare
            Assertions.assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testValueThresholdsWithLabels() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "locationId, E, F, G\n" )
                      .append( "DRRC2, 3.0, 7.0, 15.0\n" )
                      .append( "DOLC2, 23.0, 12.0, 99.7\n" );
            }

            URI uri = csvPath.toUri();
            ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                    uri,
                    ThresholdFormat.CSV,
                    "CMS",
                    "-999",
                    null,
                    null,
                    null,
                    null );
            ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.VALUE,
                                                                     wres.config.xml.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            Map<String, Set<Threshold>> actual =
                    CsvThresholdReader.readThresholds( thresholdConfig,
                                                       UNIT_STRING );

            // Compare to expected
            Map<String, Set<Threshold>> expected = new TreeMap<>();

            Threshold one = Threshold.newBuilder()
                                     .setLeftThresholdValue( 3.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "E" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setLeftThresholdValue( 7.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "F" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setLeftThresholdValue( 15.0 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT )
                                       .setName( "G" )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();

            Set<Threshold> first = new HashSet<>();
            first.add( one );
            first.add( two );
            first.add( three );

            expected.put( "DRRC2", first );

            Threshold four = Threshold.newBuilder()
                                      .setLeftThresholdValue( 23.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setName( "E" )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setLeftThresholdValue( 12.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setName( "F" )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setLeftThresholdValue( 99.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "G" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();

            Set<Threshold> second = new HashSet<>();
            second.add( four );
            second.add( five );
            second.add( six );

            expected.put( "DOLC2", second );

            // Compare
            Assertions.assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testProbabilityThresholdsWithoutLabels() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "DRRC2, 0.4, 0.6, 0.8\n" )
                      .append( "DOLC2, 0.2, 0.3, 0.7\n" );
            }

            URI uri = csvPath.toUri();
            ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                    uri,
                    ThresholdFormat.CSV,
                    "CMS",
                    "-999",
                    null,
                    null,
                    null,
                    null );
            ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                                     wres.config.xml.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );
            Map<String, Set<Threshold>> actual =
                    CsvThresholdReader.readThresholds( thresholdConfig,
                                                       UNIT_STRING );

            // Compare to expected
            Map<String, Set<Threshold>> expected = new TreeMap<>();

            Threshold one = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.4 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.6 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setLeftThresholdProbability( 0.8 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();

            Set<Threshold> first = new HashSet<>();
            first.add( one );
            first.add( two );
            first.add( three );

            expected.put( "DRRC2", first );

            Threshold four = Threshold.newBuilder()
                                      .setLeftThresholdProbability( 0.2 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setLeftThresholdProbability( 0.3 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();

            Set<Threshold> second = new HashSet<>();
            second.add( four );
            second.add( five );
            second.add( six );

            expected.put( "DOLC2", second );

            // Compare
            Assertions.assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testValueThresholdsWithoutLabels() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "DRRC2, 3.0, 7.0, 15.0\n" )
                      .append( "DOLC2, 23.0, 12.0, 99.7\n" );
            }

            URI uri = csvPath.toUri();

            ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                    uri,
                    ThresholdFormat.CSV,
                    "CMS",
                    "-999",
                    null,
                    null,
                    null,
                    null );
            ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.VALUE,
                                                                     wres.config.xml.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );
            Map<String, Set<Threshold>> actual =
                    CsvThresholdReader.readThresholds( thresholdConfig,
                                                       UNIT_STRING );

            // Compare to expected
            Map<String, Set<Threshold>> expected = new TreeMap<>();

            Threshold one = Threshold.newBuilder()
                                     .setLeftThresholdValue( 3.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setLeftThresholdValue( 7.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setLeftThresholdValue( 15.0 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();

            Set<Threshold> first = new HashSet<>();
            first.add( one );
            first.add( two );
            first.add( three );

            expected.put( "DRRC2", first );

            Threshold four = Threshold.newBuilder()
                                      .setLeftThresholdValue( 23.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setLeftThresholdValue( 12.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setLeftThresholdValue( 99.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();

            Set<Threshold> second = new HashSet<>();
            second.add( four );
            second.add( five );
            second.add( six );

            expected.put( "DOLC2", second );

            // Compare
            Assertions.assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testValueThresholdsWithoutLabelsWithMissings() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "DRRC2, 3.0, 7.0, -999.0\n" )
                      .append( "DOLC2, 23.0, -999.0, 99.7\n" );
            }

            URI uri = csvPath.toUri();

            ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                    uri,
                    ThresholdFormat.CSV,
                    "CMS",
                    "-999",
                    null,
                    null,
                    null,
                    null );
            ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.VALUE,
                                                                     wres.config.xml.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );
            Map<String, Set<Threshold>> actual =
                    CsvThresholdReader.readThresholds( thresholdConfig,
                                                       UNIT_STRING );

            // Compare to expected
            Map<String, Set<Threshold>> expected = new TreeMap<>();

            Threshold one = Threshold.newBuilder()
                                     .setLeftThresholdValue( 3.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setLeftThresholdValue( 7.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();

            Set<Threshold> first = new HashSet<>();
            first.add( one );
            first.add( two );

            expected.put( "DRRC2", first );

            Threshold three = Threshold.newBuilder()
                                       .setLeftThresholdValue( 23.0 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();
            Threshold four = Threshold.newBuilder()
                                      .setLeftThresholdValue( 99.7 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.LEFT )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();

            Set<Threshold> second = new HashSet<>();
            second.add( three );
            second.add( four );

            expected.put( "DOLC2", second );

            // Compare
            Assertions.assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }

    }

    @Test
    void testProbabilityThresholdsWithLabelsThrowsExpectedExceptions() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "locationId, A, B, C\n" )
                      .append( "LOCWITHWRONGCOUNT_A, 0.4, 0.6\n" )
                      .append( "LOCWITHWRONGCOUNT_B, 0.3, 0.2\n" )
                      .append( "LOCWITHALLMISSING_A, -999, -999, -999\n" )
                      .append( "LOCWITHALLMISSING_B, -999, -999, -999\n" )
                      .append( "LOCWITHALLMISSING_C, -999, -999, -999\n" )
                      .append( "LOCWITHNONNUMERIC_A, X, Y, Z\n" )
                      .append( "LOCWITHWRONGPROBS_A, 1.1, 0.7, 0.9\n" )
                      .append( "LOCWITHNOPROBLEMS_A, 0.9, 0.95, 0.99\n" );
            }

            URI uri = csvPath.toUri();

            ThresholdsConfig.Source source = new ThresholdsConfig.Source( uri,
                                                                          ThresholdFormat.CSV,
                                                                          "CMS",
                                                                          "-999",
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null );
            ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                                     wres.config.xml.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            IllegalArgumentException actualException = Assertions.assertThrows( IllegalArgumentException.class,
                                                                                () -> CsvThresholdReader.readThresholds(
                                                                                        thresholdConfig,
                                                                                        UNIT_STRING ) );

            String nL = System.lineSeparator();

            String expectedPath = uri.getPath();
            String expectedMessage = "When processing thresholds by feature, 7 of 8 features contained in '"
                                     + expectedPath
                                     + "' failed with exceptions, as follows. "
                                     + nL
                                     + "     These features failed with an inconsistency between the number of labels and the number of thresholds: [LOCWITHWRONGCOUNT_A, LOCWITHWRONGCOUNT_B]. "
                                     + nL
                                     + "     These features failed because all thresholds matched the missing value: [LOCWITHALLMISSING_A, LOCWITHALLMISSING_B, LOCWITHALLMISSING_C]. "
                                     + nL
                                     + "     These features failed with non-numeric input: [LOCWITHNONNUMERIC_A]. "
                                     + nL
                                     + "     These features failed with invalid input for the threshold type: [LOCWITHWRONGPROBS_A].";

            Assertions.assertEquals( expectedMessage, actualException.getMessage() );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testProbabilityThresholdsWithLabelsForIssue75812() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "\"locationId\", \"A\", \"B\", \"C\"\n" )
                      .append( "DRRC2, 0.4, 0.6, 0.8\n" );
            }

            URI uri = csvPath.toUri();
            ThresholdsConfig.Source source = new ThresholdsConfig.Source( uri,
                                                                          ThresholdFormat.CSV,
                                                                          "CMS",
                                                                          "-999",
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null );
            ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                                     wres.config.xml.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            Map<String, Set<Threshold>> actual =
                    CsvThresholdReader.readThresholds( thresholdConfig,
                                                       UNIT_STRING );

            // Compare to expected
            Map<String, Set<Threshold>> expected = new TreeMap<>();

            Threshold one = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.4 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "A" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setLeftThresholdProbability( 0.6 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.LEFT )
                                     .setName( "B" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setLeftThresholdProbability( 0.8 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT )
                                       .setName( "C" )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();

            Set<Threshold> first = new HashSet<>();
            first.add( one );
            first.add( two );
            first.add( three );

            expected.put( "DRRC2", first );

            // Compare
            Assertions.assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

}