package wres.io.reading.commaseparated;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.config.generated.FeatureType;
import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.io.retrieval.UnitMapper;
import wres.system.SystemSettings;

/**
 * Tests the {@link ThresholdReader}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ThresholdReaderTest
{

    private UnitMapper unitMapper;

    private MeasurementUnit units = MeasurementUnit.of( "CMS" );

    private SystemSettings systemSettings;

    @Before
    public void runBeforeEachTest()
    {
        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.systemSettings = SystemSettings.withDefaults();
        Mockito.when( this.unitMapper.getUnitMapper( "CMS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( this.units.toString() );
    }

    /**
     * Tests the {@link ThresholdReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testProbabiiltyThresholdsWithLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     * @throws URISyntaxException if the URI to the thresholds could not be resolved
     */

    @Test
    public void testProbabilityThresholdsWithLabels() throws IOException, URISyntaxException
    {
        String path = "testinput/commaseparated/testProbabilityThresholdsWithLabels.csv";
        URI uri = new URI( path );
        ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                uri,
                ThresholdFormat.CSV,
                "CMS",
                "-999",
                FeatureType.NWS_ID,
                null,
                null,
                null
        );
        ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                                 wres.config.generated.ThresholdDataType.LEFT,
                                                                 source,
                                                                 ThresholdOperator.GREATER_THAN );

        Map<FeaturePlus, Set<Threshold>> actual =
                ThresholdReader.readThresholds( this.systemSettings,
                                                thresholdConfig,
                                                this.units,
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
     * Tests the {@link ThresholdReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     * @throws URISyntaxException if the URI to the thresholds could not be resolved
     */

    @Test
    public void testValueThresholdsWithLabels() throws IOException, URISyntaxException
    {
        String path = "testinput/commaseparated/testValueThresholdsWithLabels.csv";
        URI uri = new URI( path );
        ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                uri,
                ThresholdFormat.CSV,
                "CMS",
                "-999",
                FeatureType.NWS_ID,
                null,
                null,
                null
        );
        ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.VALUE,
                                                                 wres.config.generated.ThresholdDataType.LEFT,
                                                                 source,
                                                                 ThresholdOperator.GREATER_THAN );

        Map<FeaturePlus, Set<Threshold>> actual =
                ThresholdReader.readThresholds( this.systemSettings,
                                                thresholdConfig,
                                                this.units,
                                                this.unitMapper );

        // Compare to expected
        Map<FeaturePlus, Set<Threshold>> expected = new TreeMap<>();

        Set<Threshold> first = new TreeSet<>();
        first.add( Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 "E",
                                 this.units ) );
        first.add( Threshold.of( OneOrTwoDoubles.of( 7.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 "F",
                                 this.units ) );
        first.add( Threshold.of( OneOrTwoDoubles.of( 15.0 ),
                                 Operator.GREATER,
                                 ThresholdDataType.LEFT,
                                 "G",
                                 this.units ) );

        Feature firstFeature =
                new Feature( null, null, null, null, null, "DRRC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( firstFeature ), first );

        Set<Threshold> second = new TreeSet<>();
        second.add( Threshold.of( OneOrTwoDoubles.of( 23.0 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  "E",
                                  this.units ) );
        second.add( Threshold.of( OneOrTwoDoubles.of( 12.0 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  "F",
                                  this.units ) );
        second.add( Threshold.of( OneOrTwoDoubles.of( 99.7 ),
                                  Operator.GREATER,
                                  ThresholdDataType.LEFT,
                                  "G",
                                  this.units ) );

        Feature secondFeature =
                new Feature( null, null, null, null, null, "DOLC2", null, null, null, null, null, null, null );
        expected.put( FeaturePlus.of( secondFeature ), second );

        // Compare
        assertTrue( "The actual thresholds do not match the expected thresholds.", actual.equals( expected ) );

    }

    /**
     * Tests the {@link ThresholdReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testProbabiiltyThresholdsWithoutLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     * @throws URISyntaxException if the URI to the thresholds could not be resolved
     */

    @Test
    public void testProbabilityThresholdsWithoutLabels() throws IOException, URISyntaxException
    {
        String path = "testinput/commaseparated/testProbabilityThresholdsWithoutLabels.csv";
        URI uri = new URI( path );
        ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                uri,
                ThresholdFormat.CSV,
                "CMS",
                "-999",
                FeatureType.NWS_ID,
                null,
                null,
                null
        );
        ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                                 wres.config.generated.ThresholdDataType.LEFT,
                                                                 source,
                                                                 ThresholdOperator.GREATER_THAN );
        Map<FeaturePlus, Set<Threshold>> actual =
                ThresholdReader.readThresholds( this.systemSettings,
                                                thresholdConfig,
                                                this.units,
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
     * Tests the {@link ThresholdReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithoutLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     * @throws URISyntaxException if the URI to the thresholds could not be resolved
     */

    @Test
    public void testValueThresholdsWithoutLabels() throws IOException, URISyntaxException
    {
        String path = "testinput/commaseparated/testValueThresholdsWithoutLabels.csv";
        URI uri = new URI( path );
        ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                uri,
                ThresholdFormat.CSV,
                "CMS",
                "-999",
                FeatureType.NWS_ID,
                null,
                null,
                null
        );
        ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.VALUE,
                                                                 wres.config.generated.ThresholdDataType.LEFT,
                                                                 source,
                                                                 ThresholdOperator.GREATER_THAN );
        Map<FeaturePlus, Set<Threshold>> actual =
                ThresholdReader.readThresholds( this.systemSettings,
                                                thresholdConfig,
                                                this.units,
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
     * Tests the {@link ThresholdReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithoutLabelsWithMissings.csv.
     * 
     * @throws IOException if the test data could not be read
     * @throws URISyntaxException if the URI to the thresholds could not be resolved
     */

    @Test
    public void testValueThresholdsWithoutLabelsWithMissings() throws IOException, URISyntaxException
    {
        String path = "testinput/commaseparated/testValueThresholdsWithoutLabelsWithMissings.csv";
        URI uri = new URI( path );
        ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                uri,
                ThresholdFormat.CSV,
                "CMS",
                "-999",
                FeatureType.NWS_ID,
                null,
                null,
                null
        );
        ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.VALUE,
                                                                 wres.config.generated.ThresholdDataType.LEFT,
                                                                 source,
                                                                 ThresholdOperator.GREATER_THAN );
        Map<FeaturePlus, Set<Threshold>> actual =
                ThresholdReader.readThresholds( this.systemSettings,
                                                thresholdConfig,
                                                this.units,
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
     * Tests the {@link ThresholdReader#readThresholds(java.net.URI, boolean, wres.datamodel.Threshold.Operator)}
     * using input from testinput/commaseparated/testValueThresholdsWithLabels.csv.
     * 
     * @throws IOException if the test data could not be read
     * @throws URISyntaxException if the URI to the thresholds could not be resolved
     */

    @Test
    public void testProbabilityThresholdsWithLabelsThrowsExpectedExceptions() throws IOException, URISyntaxException
    {
        String path = "testinput/commaseparated/testProbabilityThresholdsWithLabelsThrowsException.csv";
        URI uri = new URI( path );
        ThresholdsConfig.Source source = new ThresholdsConfig.Source(
                uri,
                ThresholdFormat.CSV,
                "CMS",
                "-999",
                FeatureType.NWS_ID,
                null,
                null,
                null
        );
        ThresholdsConfig thresholdConfig = new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                                 wres.config.generated.ThresholdDataType.LEFT,
                                                                 source,
                                                                 ThresholdOperator.GREATER_THAN );

        Exception actualException = assertThrows( IllegalArgumentException.class,
                                                  () -> ThresholdReader.readThresholds( this.systemSettings,
                                                                                        thresholdConfig,
                                                                                        this.units,
                                                                                        this.unitMapper ) );

        String nL = System.lineSeparator();

        Path expectedPath = this.systemSettings.getDataDirectory().resolve( uri.getPath() );

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
    }

}