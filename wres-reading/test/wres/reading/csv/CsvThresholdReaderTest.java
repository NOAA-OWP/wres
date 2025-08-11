package wres.reading.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.reading.ThresholdReadingException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link CsvThresholdReader}.
 * @author James Brown
 */
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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();
            Set<wres.config.yaml.components.Threshold> actual = reader.read( source,
                                                                             Set.of( "DRRC2", "DOLC2" ),
                                                                             null );

            // Build the expectation
            Threshold one = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.4 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "A" )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.6 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "B" )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setObservedThresholdProbability( 0.8 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                       .setName( "C" )
                                       .build();
            Threshold four = Threshold.newBuilder()
                                      .setObservedThresholdProbability( 0.2 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setName( "A" )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setObservedThresholdProbability( 0.3 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setName( "B" )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "C" )
                                     .build();

            Geometry expectedFeatureOne = Geometry.newBuilder()
                                                  .setName( "DRRC2" )
                                                  .build();
            Geometry expectedFeatureTwo = Geometry.newBuilder()
                                                  .setName( "DOLC2" )
                                                  .build();

            wres.config.yaml.components.Threshold oneWrapped =
                    ThresholdBuilder.builder()
                                    .threshold( one )
                                    .featureNameFrom( DatasetOrientation.LEFT )
                                    .feature( expectedFeatureOne )
                                    .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                    .build();
            wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                               .threshold( two )
                                                                               .build();
            wres.config.yaml.components.Threshold threeWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                 .threshold( three )
                                                                                 .build();
            wres.config.yaml.components.Threshold fourWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                .feature( expectedFeatureTwo )
                                                                                .threshold( four )
                                                                                .build();
            wres.config.yaml.components.Threshold fiveWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                                .threshold( five )
                                                                                .build();
            wres.config.yaml.components.Threshold sixWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                               .threshold( six )
                                                                               .build();

            Set<wres.config.yaml.components.Threshold> expected = Set.of( oneWrapped,
                                                                          twoWrapped,
                                                                          threeWrapped,
                                                                          fourWrapped,
                                                                          fiveWrapped,
                                                                          sixWrapped );

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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .unit( UNIT_STRING )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();
            Set<wres.config.yaml.components.Threshold> actual = reader.read( source,
                                                                             Set.of( "DRRC2", "DOLC2" ),
                                                                             null );

            // Build the expectation
            Threshold one = Threshold.newBuilder()
                                     .setObservedThresholdValue( 3.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "E" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setObservedThresholdValue( 7.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "F" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setObservedThresholdValue( 15.0 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                       .setName( "G" )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();
            Threshold four = Threshold.newBuilder()
                                      .setObservedThresholdValue( 23.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setName( "E" )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setObservedThresholdValue( 12.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setName( "F" )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setObservedThresholdValue( 99.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "G" )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();

            Geometry expectedFeatureOne = Geometry.newBuilder()
                                                  .setName( "DRRC2" )
                                                  .build();
            Geometry expectedFeatureTwo = Geometry.newBuilder()
                                                  .setName( "DOLC2" )
                                                  .build();

            wres.config.yaml.components.Threshold oneWrapped =
                    ThresholdBuilder.builder()
                                    .threshold( one )
                                    .featureNameFrom( DatasetOrientation.LEFT )
                                    .feature( expectedFeatureOne )
                                    .type( wres.config.yaml.components.ThresholdType.VALUE )
                                    .build();
            wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                               .threshold( two )
                                                                               .build();
            wres.config.yaml.components.Threshold threeWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                 .threshold( three )
                                                                                 .build();
            wres.config.yaml.components.Threshold fourWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                .feature( expectedFeatureTwo )
                                                                                .threshold( four )
                                                                                .build();
            wres.config.yaml.components.Threshold fiveWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                                .threshold( five )
                                                                                .build();
            wres.config.yaml.components.Threshold sixWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                               .threshold( six )
                                                                               .build();

            Set<wres.config.yaml.components.Threshold> expected = Set.of( oneWrapped,
                                                                          twoWrapped,
                                                                          threeWrapped,
                                                                          fourWrapped,
                                                                          fiveWrapped,
                                                                          sixWrapped );
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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();
            Set<wres.config.yaml.components.Threshold> actual = reader.read( source,
                                                                             Set.of( "DRRC2", "DOLC2" ),
                                                                             null );

            // Build the expectation
            Threshold one = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.4 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.6 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setObservedThresholdProbability( 0.8 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                       .build();
            Threshold four = Threshold.newBuilder()
                                      .setObservedThresholdProbability( 0.2 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setObservedThresholdProbability( 0.3 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .build();

            Geometry expectedFeatureOne = Geometry.newBuilder()
                                                  .setName( "DRRC2" )
                                                  .build();
            Geometry expectedFeatureTwo = Geometry.newBuilder()
                                                  .setName( "DOLC2" )
                                                  .build();

            wres.config.yaml.components.Threshold oneWrapped =
                    ThresholdBuilder.builder()
                                    .threshold( one )
                                    .featureNameFrom( DatasetOrientation.LEFT )
                                    .feature( expectedFeatureOne )
                                    .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                    .build();
            wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                               .threshold( two )
                                                                               .build();
            wres.config.yaml.components.Threshold threeWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                 .threshold( three )
                                                                                 .build();
            wres.config.yaml.components.Threshold fourWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                .feature( expectedFeatureTwo )
                                                                                .threshold( four )
                                                                                .build();
            wres.config.yaml.components.Threshold fiveWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                                .threshold( five )
                                                                                .build();
            wres.config.yaml.components.Threshold sixWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                               .threshold( six )
                                                                               .build();

            Set<wres.config.yaml.components.Threshold> expected = Set.of( oneWrapped,
                                                                          twoWrapped,
                                                                          threeWrapped,
                                                                          fourWrapped,
                                                                          fiveWrapped,
                                                                          sixWrapped );
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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .unit( UNIT_STRING )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();
            Set<wres.config.yaml.components.Threshold> actual = reader.read( source,
                                                                             Set.of( "DRRC2", "DOLC2" ),
                                                                             null );

            // Build the expectation
            Threshold one = Threshold.newBuilder()
                                     .setObservedThresholdValue( 3.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setObservedThresholdValue( 7.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setObservedThresholdValue( 15.0 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();
            Threshold four = Threshold.newBuilder()
                                      .setObservedThresholdValue( 23.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold five = Threshold.newBuilder()
                                      .setObservedThresholdValue( 12.0 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();
            Threshold six = Threshold.newBuilder()
                                     .setObservedThresholdValue( 99.7 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();

            Geometry expectedFeatureOne = Geometry.newBuilder()
                                                  .setName( "DRRC2" )
                                                  .build();
            Geometry expectedFeatureTwo = Geometry.newBuilder()
                                                  .setName( "DOLC2" )
                                                  .build();

            wres.config.yaml.components.Threshold oneWrapped =
                    ThresholdBuilder.builder()
                                    .threshold( one )
                                    .featureNameFrom( DatasetOrientation.LEFT )
                                    .feature( expectedFeatureOne )
                                    .type( wres.config.yaml.components.ThresholdType.VALUE )
                                    .build();
            wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                               .threshold( two )
                                                                               .build();
            wres.config.yaml.components.Threshold threeWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                 .threshold( three )
                                                                                 .build();
            wres.config.yaml.components.Threshold fourWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                .feature( expectedFeatureTwo )
                                                                                .threshold( four )
                                                                                .build();
            wres.config.yaml.components.Threshold fiveWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                                .threshold( five )
                                                                                .build();
            wres.config.yaml.components.Threshold sixWrapped = ThresholdBuilder.builder( fourWrapped )
                                                                               .threshold( six )
                                                                               .build();

            Set<wres.config.yaml.components.Threshold> expected = Set.of( oneWrapped,
                                                                          twoWrapped,
                                                                          threeWrapped,
                                                                          fourWrapped,
                                                                          fiveWrapped,
                                                                          sixWrapped );

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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .unit( UNIT_STRING )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();
            Set<wres.config.yaml.components.Threshold> actual = reader.read( source,
                                                                             Set.of( "DRRC2", "DOLC2" ),
                                                                             null );

            // Build the expectation
            Threshold one = Threshold.newBuilder()
                                     .setObservedThresholdValue( 3.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setObservedThresholdValue( 7.0 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setObservedThresholdValue( 23.0 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();
            Threshold four = Threshold.newBuilder()
                                      .setObservedThresholdValue( 99.7 )
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();

            Geometry expectedFeatureOne = Geometry.newBuilder()
                                                  .setName( "DRRC2" )
                                                  .build();
            Geometry expectedFeatureTwo = Geometry.newBuilder()
                                                  .setName( "DOLC2" )
                                                  .build();

            wres.config.yaml.components.Threshold oneWrapped =
                    ThresholdBuilder.builder()
                                    .threshold( one )
                                    .featureNameFrom( DatasetOrientation.LEFT )
                                    .feature( expectedFeatureOne )
                                    .type( wres.config.yaml.components.ThresholdType.VALUE )
                                    .build();
            wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                               .threshold( two )
                                                                               .build();
            wres.config.yaml.components.Threshold threeWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                 .feature( expectedFeatureTwo )
                                                                                 .threshold( three )
                                                                                 .build();
            wres.config.yaml.components.Threshold fourWrapped = ThresholdBuilder.builder( threeWrapped )
                                                                                .threshold( four )
                                                                                .build();

            Set<wres.config.yaml.components.Threshold> expected = Set.of( oneWrapped,
                                                                          twoWrapped,
                                                                          threeWrapped,
                                                                          fourWrapped );

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
    void testValueThresholdsWithBetweenOperator() throws IOException
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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .unit( UNIT_STRING )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( ThresholdOperator.BETWEEN )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();
            Set<wres.config.yaml.components.Threshold> actual = reader.read( source,
                                                                             Set.of( "DRRC2", "DOLC2" ),
                                                                             null );

            // Build the expectation
            Threshold one = Threshold.newBuilder()
                                     .setObservedThresholdValue( 3.0 )
                                     .setPredictedThresholdValue( 7.0 )
                                     .setOperator( Threshold.ThresholdOperator.BETWEEN )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setObservedThresholdValue( 7.0 )
                                     .setPredictedThresholdValue( 15.0 )
                                     .setOperator( Threshold.ThresholdOperator.BETWEEN )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setThresholdValueUnits( UNIT_STRING )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setObservedThresholdValue( 12.0 )
                                       .setPredictedThresholdValue( 23.0 )
                                       .setOperator( Threshold.ThresholdOperator.BETWEEN )
                                       .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                       .setThresholdValueUnits( UNIT_STRING )
                                       .build();
            Threshold four = Threshold.newBuilder()
                                      .setObservedThresholdValue( 23.0 )
                                      .setPredictedThresholdValue( 99.7 )
                                      .setOperator( Threshold.ThresholdOperator.BETWEEN )
                                      .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                      .setThresholdValueUnits( UNIT_STRING )
                                      .build();

            Geometry expectedFeatureOne = Geometry.newBuilder()
                                                  .setName( "DRRC2" )
                                                  .build();
            Geometry expectedFeatureTwo = Geometry.newBuilder()
                                                  .setName( "DOLC2" )
                                                  .build();

            wres.config.yaml.components.Threshold oneWrapped =
                    ThresholdBuilder.builder()
                                    .threshold( one )
                                    .featureNameFrom( DatasetOrientation.LEFT )
                                    .feature( expectedFeatureOne )
                                    .type( wres.config.yaml.components.ThresholdType.VALUE )
                                    .build();
            wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                               .threshold( two )
                                                                               .build();
            wres.config.yaml.components.Threshold threeWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                 .feature( expectedFeatureTwo )
                                                                                 .threshold( three )
                                                                                 .build();
            wres.config.yaml.components.Threshold fourWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                .feature( expectedFeatureTwo )
                                                                                .threshold( four )
                                                                                .build();

            Set<wres.config.yaml.components.Threshold> expected = Set.of( oneWrapped,
                                                                          twoWrapped,
                                                                          threeWrapped,
                                                                          fourWrapped );

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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .unit( UNIT_STRING )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();

            Set<String> features = Set.of( "LOCWITHWRONGCOUNT_A",
                                           "LOCWITHWRONGCOUNT_B",
                                           "LOCWITHALLMISSING_A",
                                           "LOCWITHALLMISSING_B",
                                           "LOCWITHALLMISSING_C",
                                           "LOCWITHNONNUMERIC_A",
                                           "LOCWITHWRONGPROBS_A",
                                           "LOCWITHNOPROBLEMS_A" );

            ThresholdReadingException actualException =
                    Assertions.assertThrows( ThresholdReadingException.class,
                                             () -> reader.read( source, features, null ) );

            String nL = System.lineSeparator();

            String expectedPath = uri.getPath();
            String expectedMessage = "When processing thresholds by feature, 7 of 8 features contained in '"
                                     + expectedPath
                                     + "' failed with exceptions, as follows."
                                     + nL
                                     + "- These features failed with an inconsistency between the number of labels and the number of thresholds: [LOCWITHWRONGCOUNT_A, LOCWITHWRONGCOUNT_B]."
                                     + nL
                                     + "- These features failed because all thresholds matched the missing value: [LOCWITHALLMISSING_A, LOCWITHALLMISSING_B, LOCWITHALLMISSING_C]."
                                     + nL
                                     + "- These features failed with non-numeric input: [LOCWITHNONNUMERIC_A]."
                                     + nL
                                     + "- These features failed with invalid input for the threshold type: [LOCWITHWRONGPROBS_A].";

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

            ThresholdSource source = ThresholdSourceBuilder.builder()
                                                           .uri( uri )
                                                           .missingValue( -999.0 )
                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                           .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                                           .applyTo( ThresholdOrientation.OBSERVED )
                                                           .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                                           .build();

            CsvThresholdReader reader = CsvThresholdReader.of();
            Set<wres.config.yaml.components.Threshold> actual = reader.read( source,
                                                                             Set.of( "DRRC2" ),
                                                                             null );

            // Build the expectation
            Threshold one = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.4 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "A" )
                                     .build();
            Threshold two = Threshold.newBuilder()
                                     .setObservedThresholdProbability( 0.6 )
                                     .setOperator( Threshold.ThresholdOperator.GREATER )
                                     .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                     .setName( "B" )
                                     .build();
            Threshold three = Threshold.newBuilder()
                                       .setObservedThresholdProbability( 0.8 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.OBSERVED )
                                       .setName( "C" )
                                       .build();

            Geometry expectedFeatureOne = Geometry.newBuilder()
                                                  .setName( "DRRC2" )
                                                  .build();

            wres.config.yaml.components.Threshold oneWrapped =
                    ThresholdBuilder.builder()
                                    .threshold( one )
                                    .featureNameFrom( DatasetOrientation.LEFT )
                                    .feature( expectedFeatureOne )
                                    .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                    .build();
            wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                               .threshold( two )
                                                                               .build();
            wres.config.yaml.components.Threshold threeWrapped = ThresholdBuilder.builder( oneWrapped )
                                                                                 .threshold( three )
                                                                                 .build();

            Set<wres.config.yaml.components.Threshold> expected = Set.of( oneWrapped,
                                                                          twoWrapped,
                                                                          threeWrapped );

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
