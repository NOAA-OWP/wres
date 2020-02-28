package wres.io.reading.commaseparated;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.io.retrieval.UnitMapper;

/**
 * Tests the {@link CommaSeparatedReader}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedReaderTest
{

    private UnitMapper unitMapper;

    private MeasurementUnit units = MeasurementUnit.of( "CMS" );

    @Before
    public void runBeforeEachTest()
    {
        this.unitMapper = Mockito.mock( UnitMapper.class );
        Mockito.when( this.unitMapper.getUnitMapper( "CMS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( this.units.toString() );
    }

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
                                                     null,
                                                     this.unitMapper );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     "A",
                                                     this.units ) );
        first.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.6 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     "B",
                                                     this.units ) );
        first.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.8 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     "C",
                                                     this.units ) );

        Feature firstFeature =
                new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT,
                                                      "A",
                                                      this.units ) );
        second.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT,
                                                      "B",
                                                      this.units ) );
        second.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT,
                                                      "C",
                                                      this.units ) );

        Feature secondFeature =
                new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertEquals( expected, actual );

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

        MeasurementUnit dim = MeasurementUnit.of( "CMS" );

        Map<FeaturePlus, Set<Threshold>> actual =
                CommaSeparatedReader.readThresholds( commaSeparated,
                                                     false,
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     null,
                                                     dim,
                                                     this.unitMapper );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 "E",
                                 dim ) );
        first.add( Threshold.of( OneOrTwoDoubles.of( 7.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 "F",
                                 dim ) );
        first.add( Threshold.of( OneOrTwoDoubles.of( 15.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 "G",
                                 dim ) );

        Feature firstFeature =
                new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( Threshold.of( OneOrTwoDoubles.of( 23.0 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  "E",
                                  dim ) );
        second.add( Threshold.of( OneOrTwoDoubles.of( 12.0 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  "F",
                                  dim ) );
        second.add( Threshold.of( OneOrTwoDoubles.of( 99.7 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  "G",
                                  dim ) );

        Feature secondFeature =
                new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
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
                                                     null,
                                                     this.unitMapper );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     this.units ) );
        first.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.6 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     this.units ) );
        first.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.8 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT,
                                                     this.units ) );

        Feature firstFeature =
                new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT,
                                                      this.units ) );
        second.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT,
                                                      this.units ) );
        second.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT,
                                                      this.units ) );

        Feature secondFeature =
                new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertEquals( expected, actual );

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
                                                     null,
                                                     this.unitMapper );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 this.units ) );
        first.add( Threshold.of( OneOrTwoDoubles.of( 7.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 this.units ) );
        first.add( Threshold.of( OneOrTwoDoubles.of( 15.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 this.units ) );

        Feature firstFeature =
                new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( Threshold.of( OneOrTwoDoubles.of( 23.0 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  this.units ) );
        second.add( Threshold.of( OneOrTwoDoubles.of( 12.0 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  this.units ) );
        second.add( Threshold.of( OneOrTwoDoubles.of( 99.7 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  this.units ) );

        Feature secondFeature =
                new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertEquals( expected, actual );

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
                                                     null,
                                                     this.unitMapper );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 this.units ) );
        first.add( Threshold.of( OneOrTwoDoubles.of( 7.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 this.units ) );

        Feature firstFeature =
                new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( Threshold.of( OneOrTwoDoubles.of( 23.0 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  this.units ) );
        second.add( Threshold.of( OneOrTwoDoubles.of( 99.7 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  this.units ) );

        Feature secondFeature =
                new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertEquals( expected, actual );

    }

    /**
     * Tests the {@link CommaSeparatedReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     */

    @Test
    public void testProbabilityThresholdsWithLabelsThrowsExpectedExceptions() throws IOException
    {
        Path commaSeparated =
                Paths.get( "testinput/commaseparated/testProbabilityThresholdsWithLabelsThrowsException.csv" );

        Exception actualException = assertThrows( IllegalArgumentException.class,
                                                  () -> CommaSeparatedReader.readThresholds( commaSeparated,
                                                                                             true,
                                                                                             Operator.GREATER,
                                                                                             ThresholdDataType.LEFT,
                                                                                             -999.0,
                                                                                             null,
                                                                                             this.unitMapper ) );

        String expectedMessage = "When processing thresholds by feature, 7 of 8 features contained in '"
                                 + commaSeparated
                                 + "' failed with exceptions, as follows. "
                                 + "These features failed with an inconsistency between the number of "
                                 + "labels and the number of thresholds: [LOCWITHWRONGCOUNT_A, LOCWITHWRONGCOUNT_B]. "
                                 + "These features failed because all thresholds matched the missing value: "
                                 + "[LOCWITHALLMISSING_A, LOCWITHALLMISSING_B, LOCWITHALLMISSING_C]. These features "
                                 + "failed with non-numeric input: [LOCWITHNONNUMERIC_A]. These features failed with "
                                 + "invalid input for the threshold type: [LOCWITHWRONGPROBS_A].";

        assertEquals( expectedMessage, actualException.getMessage() );
    }
}