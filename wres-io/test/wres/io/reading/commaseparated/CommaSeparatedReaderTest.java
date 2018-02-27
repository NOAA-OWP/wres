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
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;

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
                CommaSeparatedReader.readThresholds( commaSeparated, true, Operator.GREATER );

        DataFactory factory = DefaultDataFactory.getInstance();

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( factory.ofProbabilityThreshold( 0.4, Operator.GREATER, "A" ) );
        first.add( factory.ofProbabilityThreshold( 0.6, Operator.GREATER, "B" ) );
        first.add( factory.ofProbabilityThreshold( 0.8, Operator.GREATER, "C" ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( factory.ofProbabilityThreshold( 0.2, Operator.GREATER, "A" ) );
        second.add( factory.ofProbabilityThreshold( 0.3, Operator.GREATER, "B" ) );
        second.add( factory.ofProbabilityThreshold( 0.7, Operator.GREATER, "C" ) );

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

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated, false, Operator.GREATER );

        DataFactory factory = DefaultDataFactory.getInstance();

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( factory.ofThreshold( 3.0, Operator.GREATER, "E" ) );
        first.add( factory.ofThreshold( 7.0, Operator.GREATER, "F" ) );
        first.add( factory.ofThreshold( 15.0, Operator.GREATER, "G" ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( factory.ofThreshold( 23.0, Operator.GREATER, "E" ) );
        second.add( factory.ofThreshold( 12.0, Operator.GREATER, "F" ) );
        second.add( factory.ofThreshold( 99.7, Operator.GREATER, "G" ) );

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
                CommaSeparatedReader.readThresholds( commaSeparated, true, Operator.GREATER );

        DataFactory factory = DefaultDataFactory.getInstance();

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( factory.ofProbabilityThreshold( 0.4, Operator.GREATER ) );
        first.add( factory.ofProbabilityThreshold( 0.6, Operator.GREATER ) );
        first.add( factory.ofProbabilityThreshold( 0.8, Operator.GREATER ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( factory.ofProbabilityThreshold( 0.2, Operator.GREATER ) );
        second.add( factory.ofProbabilityThreshold( 0.3, Operator.GREATER ) );
        second.add( factory.ofProbabilityThreshold( 0.7, Operator.GREATER ) );

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
                CommaSeparatedReader.readThresholds( commaSeparated, false, Operator.GREATER );

        DataFactory factory = DefaultDataFactory.getInstance();

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( factory.ofThreshold( 3.0, Operator.GREATER ) );
        first.add( factory.ofThreshold( 7.0, Operator.GREATER ) );
        first.add( factory.ofThreshold( 15.0, Operator.GREATER ) );

        Feature firstFeature = new Feature( null, null, null, "DRRC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( factory.ofThreshold( 23.0, Operator.GREATER ) );
        second.add( factory.ofThreshold( 12.0, Operator.GREATER ) );
        second.add( factory.ofThreshold( 99.7, Operator.GREATER ) );

        Feature secondFeature = new Feature( null, null, null, "DOLC2", null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }


}