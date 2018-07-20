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
import wres.datamodel.Dimension;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link CommaSeparatedReader}.
 * 
 * @author james.brown@hydrosolved.com
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
                CommaSeparatedReader.readThresholds( commaSeparated,
                                                     true,
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     null,
                                                     null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.4 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT,
                                                   "A" ) );
        first.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.6 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT,
                                                   "B" ) );
        first.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.8 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT,
                                                   "C" ) );

        Feature firstFeature = new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.2 ),
                                                    Operator.GREATER,
                                                    ThresholdDataType.LEFT,
                                                    "A" ) );
        second.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.3 ),
                                                    Operator.GREATER,
                                                    ThresholdDataType.LEFT,
                                                    "B" ) );
        second.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.7 ),
                                                    Operator.GREATER,
                                                    ThresholdDataType.LEFT,
                                                    "C" ) );

        Feature secondFeature = new Feature( null,null, null,  null, null, "DOLC2", null, null, null, null, null, null, null );
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

        Dimension dim = MetadataFactory.getDimension( "CMS" );

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated,
                                                     false,
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     null,
                                                     dim );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 3.0 ),
                                        Operator.GREATER,
                                        ThresholdDataType.LEFT,
                                        "E",
                                        dim ) );
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 7.0 ),
                                        Operator.GREATER,
                                        ThresholdDataType.LEFT,
                                        "F",
                                        dim ) );
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 15.0 ),
                                        Operator.GREATER,
                                        ThresholdDataType.LEFT,
                                        "G",
                                        dim ) );

        Feature firstFeature = new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 23.0 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT,
                                         "E",
                                         dim ) );
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 12.0 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT,
                                         "F",
                                         dim ) );
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 99.7 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT,
                                         "G",
                                         dim ) );

        Feature secondFeature = new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
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
                CommaSeparatedReader.readThresholds( commaSeparated,
                                                     true,
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     null,
                                                     null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.4 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );
        first.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.6 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );
        first.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.8 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );

        Feature firstFeature = new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.2 ),
                                                    Operator.GREATER,
                                                    ThresholdDataType.LEFT ) );
        second.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.3 ),
                                                    Operator.GREATER,
                                                    ThresholdDataType.LEFT ) );
        second.add( DataFactory.ofProbabilityThreshold( DataFactory.ofOneOrTwoDoubles( 0.7 ),
                                                    Operator.GREATER,
                                                    ThresholdDataType.LEFT ) );

        Feature secondFeature = new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
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
                CommaSeparatedReader.readThresholds( commaSeparated,
                                                     false,
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     null,
                                                     null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 3.0 ), Operator.GREATER, ThresholdDataType.LEFT ) );
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 7.0 ), Operator.GREATER, ThresholdDataType.LEFT ) );
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 15.0 ), Operator.GREATER, ThresholdDataType.LEFT ) );

        Feature firstFeature = new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 23.0 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT ) );
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 12.0 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT ) );
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 99.7 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT ) );

        Feature secondFeature = new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
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
                CommaSeparatedReader.readThresholds( commaSeparated,
                                                     false,
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     -999.0,
                                                     null );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 3.0 ), Operator.GREATER, ThresholdDataType.LEFT ) );
        first.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 7.0 ), Operator.GREATER, ThresholdDataType.LEFT ) );

        Feature firstFeature = new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 23.0 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT ) );
        second.add( DataFactory.ofThreshold( DataFactory.ofOneOrTwoDoubles( 99.7 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT ) );

        Feature secondFeature = new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }


}