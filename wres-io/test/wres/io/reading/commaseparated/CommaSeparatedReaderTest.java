package wres.io.reading.commaseparated;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.metadata.MetadataFactory;

/**
 * Tests the {@link CommaSeparatedReader}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class CommaSeparatedReaderTest
{

    /**
     * Data factory.
     */

    private static final DataFactory FACTORY = DefaultDataFactory.getInstance();

    /**
     * Tests the {@link CommaSeparatedReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testProbabiiltyThresholdsWithLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     */

    @Test
    public void testProbabilityThresholdsWithLabels() throws IOException
    {
        Path commaSeparated = Paths.get( "testinput/commaseparated/testProbabilityThresholdsWithLabels.csv" );

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated, true, Operator.GREATER, null, null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ), Operator.GREATER, "A" ) );
        first.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.6 ), Operator.GREATER, "B" ) );
        first.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.8 ), Operator.GREATER, "C" ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ), Operator.GREATER, "A" ) );
        second.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ), Operator.GREATER, "B" ) );
        second.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ), Operator.GREATER, "C" ) );

        Feature secondFeature = new Feature( null, null, null, "DOLC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }

    /**
     * Tests the {@link CommaSeparatedReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     */

    @Test
    public void testValueThresholdsWithLabels() throws IOException
    {
        Path commaSeparated = Paths.get( "testinput/commaseparated/testValueThresholdsWithLabels.csv" );


        MetadataFactory meta = FACTORY.getMetadataFactory();
        Dimension dim = meta.getDimension( "CMS" );

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated, false, Operator.GREATER, null, dim );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 3.0 ), Operator.GREATER, "E", dim ) );
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 7.0 ), Operator.GREATER, "F", dim ) );
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 15.0 ), Operator.GREATER, "G", dim ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 23.0 ), Operator.GREATER, "E", dim ) );
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 12.0 ), Operator.GREATER, "F", dim ) );
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 99.7 ), Operator.GREATER, "G", dim ) );

        Feature secondFeature = new Feature( null, null, null, "DOLC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }

    /**
     * Tests the {@link CommaSeparatedReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testProbabiiltyThresholdsWithoutLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     */

    @Test
    public void testProbabilityThresholdsWithoutLabels() throws IOException
    {
        Path commaSeparated = Paths.get( "testinput/commaseparated/testProbabilityThresholdsWithoutLabels.csv" );

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated, true, Operator.GREATER, null, null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ), Operator.GREATER ) );
        first.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.6 ), Operator.GREATER ) );
        first.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.8 ), Operator.GREATER ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ), Operator.GREATER ) );
        second.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ), Operator.GREATER ) );
        second.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ), Operator.GREATER ) );

        Feature secondFeature = new Feature( null, null, null, "DOLC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }

    /**
     * Tests the {@link CommaSeparatedReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithoutLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     */

    @Test
    public void testValueThresholdsWithoutLabels() throws IOException
    {
        Path commaSeparated = Paths.get( "testinput/commaseparated/testValueThresholdsWithoutLabels.csv" );

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated, false, Operator.GREATER, null, null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 3.0 ), Operator.GREATER ) );
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 7.0 ), Operator.GREATER ) );
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 15.0 ), Operator.GREATER ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 23.0 ), Operator.GREATER ) );
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 12.0 ), Operator.GREATER ) );
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 99.7 ), Operator.GREATER ) );

        Feature secondFeature = new Feature( null, null, null, "DOLC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }

    /**
     * Tests the {@link CommaSeparatedReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithoutLabelsWithMissings.csv.
     * 
     * @throws IOException if the test data could not be read
     */

    @Test
    public void testValueThresholdsWithoutLabelsWithMissings() throws IOException
    {
        Path commaSeparated = Paths.get( "testinput/commaseparated/testValueThresholdsWithoutLabelsWithMissings.csv" );

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated, false, Operator.GREATER, -999.0, null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 3.0 ), Operator.GREATER ) );
        first.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 7.0 ), Operator.GREATER ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 23.0 ), Operator.GREATER ) );
        second.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 99.7 ), Operator.GREATER ) );

        Feature secondFeature = new Feature( null, null, null, "DOLC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }


}