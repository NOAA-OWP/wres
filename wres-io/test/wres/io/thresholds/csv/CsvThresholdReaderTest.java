package wres.io.thresholds.csv;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.units.UnitMapper;
import wres.datamodel.thresholds.ThresholdException;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.system.SystemSettings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Tests the {@link CsvThresholdReader}.
 * 
 * @author James Brown
 */

class CsvThresholdReaderTest
{
    private static final String TEST_CSV = "test.csv";
    private static final String TEST = "test";
    private UnitMapper unitMapper;
    private MeasurementUnit units = MeasurementUnit.of( "CMS" );
    private SystemSettings systemSettings;

    @BeforeEach
    void runBeforeEachTest()
    {
        this.unitMapper = Mockito.mock( UnitMapper.class );
        System.setProperty( "user.timezone", "UTC" );
        this.systemSettings = SystemSettings.withDefaults();
        Mockito.when( this.unitMapper.getUnitMapper( "CMS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( this.units.toString() );
    }

    @Test
    void testProbabilityThresholdsWithLabels() throws IOException, URISyntaxException
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
                                                                     wres.config.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            Map<String, Set<ThresholdOuter>> actual =
                    CsvThresholdReader.readThresholds( this.systemSettings,
                                                       thresholdConfig,
                                                       this.units,
                                                       this.unitMapper );

            // Compare to expected
            Map<String, Set<ThresholdOuter>> expected = new TreeMap<>();

            Set<ThresholdOuter> first = new TreeSet<>();
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              "A",
                                                              this.units ) );
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.6 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              "B",
                                                              this.units ) );
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.8 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              "C",
                                                              this.units ) );

            String firstFeature = "DRRC2";
            expected.put( firstFeature, first );

            Set<ThresholdOuter> second = new TreeSet<>();
            second.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2 ),
                                                               Operator.GREATER,
                                                               ThresholdDataType.LEFT,
                                                               "A",
                                                               this.units ) );
            second.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                               Operator.GREATER,
                                                               ThresholdDataType.LEFT,
                                                               "B",
                                                               this.units ) );
            second.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                               Operator.GREATER,
                                                               ThresholdDataType.LEFT,
                                                               "C",
                                                               this.units ) );

            String secondFeature = "DOLC2";
            expected.put( secondFeature, second );

            // Compare
            assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testValueThresholdsWithLabels() throws IOException, URISyntaxException
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
                                                                     wres.config.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            Map<String, Set<ThresholdOuter>> actual =
                    CsvThresholdReader.readThresholds( this.systemSettings,
                                                       thresholdConfig,
                                                       this.units,
                                                       this.unitMapper );

            // Compare to expected
            Map<String, Set<ThresholdOuter>> expected = new TreeMap<>();

            Set<ThresholdOuter> first = new TreeSet<>();
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          "E",
                                          this.units ) );
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 7.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          "F",
                                          this.units ) );
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 15.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          "G",
                                          this.units ) );

            String firstFeature = "DRRC2";
            expected.put( firstFeature, first );

            Set<ThresholdOuter> second = new TreeSet<>();
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 23.0 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           "E",
                                           this.units ) );
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 12.0 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           "F",
                                           this.units ) );
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 99.7 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           "G",
                                           this.units ) );

            String secondFeature = "DOLC2";
            expected.put( secondFeature, second );

            // Compare
            assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testProbabilityThresholdsWithoutLabels() throws IOException, URISyntaxException
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
                                                                     wres.config.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );
            Map<String, Set<ThresholdOuter>> actual =
                    CsvThresholdReader.readThresholds( this.systemSettings,
                                                       thresholdConfig,
                                                       this.units,
                                                       this.unitMapper );

            // Compare to expected
            Map<String, Set<ThresholdOuter>> expected = new TreeMap<>();

            Set<ThresholdOuter> first = new TreeSet<>();
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              this.units ) );
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.6 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              this.units ) );
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.8 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              this.units ) );

            String firstFeature = "DRRC2";
            expected.put( firstFeature, first );

            Set<ThresholdOuter> second = new TreeSet<>();
            second.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2 ),
                                                               Operator.GREATER,
                                                               ThresholdDataType.LEFT,
                                                               this.units ) );
            second.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                               Operator.GREATER,
                                                               ThresholdDataType.LEFT,
                                                               this.units ) );
            second.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                               Operator.GREATER,
                                                               ThresholdDataType.LEFT,
                                                               this.units ) );

            String secondFeature = "DOLC2";
            expected.put( secondFeature, second );

            // Compare
            assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testValueThresholdsWithoutLabels() throws IOException, URISyntaxException
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
                                                                     wres.config.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );
            Map<String, Set<ThresholdOuter>> actual =
                    CsvThresholdReader.readThresholds( this.systemSettings,
                                                       thresholdConfig,
                                                       this.units,
                                                       this.unitMapper );

            // Compare to expected
            Map<String, Set<ThresholdOuter>> expected = new TreeMap<>();

            Set<ThresholdOuter> first = new TreeSet<>();
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          this.units ) );
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 7.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          this.units ) );
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 15.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          this.units ) );

            String firstFeature = "DRRC2";
            expected.put( firstFeature, first );

            Set<ThresholdOuter> second = new TreeSet<>();
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 23.0 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           this.units ) );
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 12.0 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           this.units ) );
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 99.7 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           this.units ) );

            String secondFeature = "DOLC2";
            expected.put( secondFeature, second );

            // Compare
            assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testValueThresholdsWithoutLabelsWithMissings() throws IOException, URISyntaxException
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
                                                                     wres.config.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );
            Map<String, Set<ThresholdOuter>> actual =
                    CsvThresholdReader.readThresholds( this.systemSettings,
                                                       thresholdConfig,
                                                       this.units,
                                                       this.unitMapper );

            // Compare to expected
            Map<String, Set<ThresholdOuter>> expected = new TreeMap<>();

            Set<ThresholdOuter> first = new TreeSet<>();
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          this.units ) );
            first.add( ThresholdOuter.of( OneOrTwoDoubles.of( 7.0 ),
                                          Operator.GREATER,
                                          ThresholdDataType.LEFT,
                                          this.units ) );

            String firstFeature = "DRRC2";
            expected.put( firstFeature, first );

            Set<ThresholdOuter> second = new TreeSet<>();
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 23.0 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           this.units ) );
            second.add( ThresholdOuter.of( OneOrTwoDoubles.of( 99.7 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT,
                                           this.units ) );

            String secondFeature = "DOLC2";
            expected.put( secondFeature, second );

            // Compare
            assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }

    }

    @Test
    void testProbabilityThresholdsWithLabelsThrowsExpectedExceptions() throws IOException, URISyntaxException
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
                                                                     wres.config.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            ThresholdException actualException = assertThrows( ThresholdException.class,
                                                               () -> CsvThresholdReader.readThresholds( this.systemSettings,
                                                                                                        thresholdConfig,
                                                                                                        this.units,
                                                                                                        this.unitMapper ) );

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

            assertEquals( expectedMessage, actualException.getMessage() );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testProbabilityThresholdsWithLabelsForIssue75812() throws IOException, URISyntaxException
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
                                                                     wres.config.generated.ThresholdDataType.LEFT,
                                                                     source,
                                                                     ThresholdOperator.GREATER_THAN );

            Map<String, Set<ThresholdOuter>> actual =
                    CsvThresholdReader.readThresholds( this.systemSettings,
                                                       thresholdConfig,
                                                       this.units,
                                                       this.unitMapper );

            // Compare to expected
            Map<String, Set<ThresholdOuter>> expected = new TreeMap<>();

            Set<ThresholdOuter> first = new TreeSet<>();
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              "A",
                                                              this.units ) );
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.6 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              "B",
                                                              this.units ) );
            first.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.8 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT,
                                                              "C",
                                                              this.units ) );

            String firstFeature = "DRRC2";
            expected.put( firstFeature, first );

            // Compare
            assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

}