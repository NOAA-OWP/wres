package wres.io.thresholds.wrds.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.datamodel.units.UnitMapper;
import wres.io.thresholds.wrds.NoThresholdsFoundException;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link ThresholdExtractor}.  This has been modified to include a test that was
 * inappropriately located within another test class.  So, if it looks like a hodgepodge, that's
 * because it reflects two tests in two different files combined into one.  I have not refactored
 * it further.
 * @author Hank Herr
 * @author James Brown
 */

class ThresholdExtractorTest
{
    private static final double EPSILON = 0.00001;

    private static WrdsLocation createFeature( final String featureId, final String usgsSiteCode, final String lid )
    {
        return new WrdsLocation( featureId, usgsSiteCode, lid );
    }

    private static final WrdsLocation PTSA1 = createFeature( "2323396", "02372250", "PTSA1" );
    private static final WrdsLocation MNTG1 = createFeature( "6444276", "02349605", "MNTG1" );


    private static final MeasurementUnit units = MeasurementUnit.of( "CMS" );
    private UnitMapper unitMapper;
    private ThresholdResponse oldResponse = null;

    /**
     * Test instance.
     */

    private ThresholdExtractor extractor = null;

    @BeforeEach
    void runBeforeEachTest()
    {
        // Based on the first threshold in the example from #82996-2. If the api is adapted in a breaking way, this will 
        // need to be adapted too.

        // Create a ThresholdResponse for testing and then create a ThresholdExtractor from that
        ThresholdResponse thresholdResponse = new ThresholdResponse();

        Collection<ThresholdDefinition> thresholds = new ArrayList<>();

        ThresholdDefinition aThreshold = new ThresholdDefinition();
        ThresholdMetadata aThresholdMetadata = new ThresholdMetadata();
        aThresholdMetadata.setLocationId( "BLUO2" );
        aThresholdMetadata.setNwsLid( "BLUO2" );
        aThresholdMetadata.setUsgsSiteCode( "07332500" );

        aThresholdMetadata.setNwmFeatureId( "700694" );
        aThresholdMetadata.setIdType( "NWS Station" );
        aThresholdMetadata.setThresholdSource( "NWS-NRLDB" );
        aThresholdMetadata.setThresholdSourceDescription( "National Weather Service - National River Location "
                                                          + "Database" );
        aThresholdMetadata.setRatingSource( "NRLDB" );
        aThresholdMetadata.setRatingSourceDescription( "NRLDB" );
        aThresholdMetadata.setFlowUnit( "CFS" );
        aThresholdMetadata.setStageUnit( "FT" );

        aThreshold.setMetadata( aThresholdMetadata );

        OriginalThresholdValues originalValues = new OriginalThresholdValues();
        originalValues.setLowStage( "1.5" );
        originalValues.setBankfullStage( "28.0" );
        originalValues.setActionStage( "26.0" );
        originalValues.setMinorStage( "28.0" );
        originalValues.setModerateStage( "31.0" );
        originalValues.setMajorStage( "43.0" );
        originalValues.setRecordStage( "50.83" );

        aThreshold.setOriginalValues( originalValues );

        CalculatedThresholdValues calculatedValues = new CalculatedThresholdValues();
        calculatedValues.setLowFlow( "0.0" );
        calculatedValues.setBankfullFlow( "7614.916318407959" );
        calculatedValues.setActionFlow( "6608.18" );
        calculatedValues.setMinorFlow( "7614.916318407959" );
        calculatedValues.setModerateFlow( "9248.87268292683" );
        calculatedValues.setMajorFlow( "49279.61515624999" );
        calculatedValues.setRecordFlow( "102933.03851562498" );

        aThreshold.setCalculatedValues( calculatedValues );

        thresholds.add( aThreshold );

        thresholdResponse.setThresholds( thresholds );
        Map<String, Double> metrics = new HashMap<>();
        metrics.put( "threshold_count", 1.0 );
        thresholdResponse.setMetrics( metrics );
        thresholdResponse.setDocumentation( "redacted/docs/dev/v2/location/swagger/" );

        this.extractor = new ThresholdExtractor( thresholdResponse );


        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.oldResponse = this.createOldThresholdResponse();
        Mockito.when( this.unitMapper.getUnitMapper( "FT" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "CFS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "MM" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( units.toString() );
    }

    @Test
    void testExtractThresholdsForOneFeatureGeneratesExpectedThresholdCounts()
    {
        // Mock the unit mapper
        UnitMapper mapper = Mockito.mock( UnitMapper.class );
        Mockito.when( mapper.getUnitMapper( "CFS" ) )
               .thenReturn( next -> next );

        Mockito.when( mapper.getUnitMapper( "FT" ) )
               .thenReturn( next -> next );

        // Assert against flow first
        Mockito.when( mapper.getDesiredMeasurementUnitName() )
               .thenReturn( "CFS" );

        Map<WrdsLocation, Set<Threshold>> extractedFlowThresholds = extractor.readFlow()
                                                                             .from( "NWS-NRLDB" )
                                                                             .ratingFrom( "NRLDB" )
                                                                             .convertTo( mapper )
                                                                             .extract();

        // Yes, this is a relatively weak assertion. Other tests could be added with stronger assertions or this
        // test could be replaced.
        assertEquals( 1, extractedFlowThresholds.size() );
        assertEquals( 7, extractedFlowThresholds.get( extractedFlowThresholds.keySet().iterator().next() ).size() );

        // Assert against stage next
        Mockito.when( mapper.getDesiredMeasurementUnitName() )
               .thenReturn( "FT" );

        Map<WrdsLocation, Set<Threshold>> extractedStageThresholds = extractor.readStage()
                                                                              .from( "NWS-NRLDB" )
                                                                              .ratingFrom( "NRLDB" )
                                                                              .convertTo( mapper )
                                                                              .extract();

        // Yes, this is a relatively weak assertion. Other tests could be added with stronger assertions or this
        // test could be replaced.
        assertEquals( 1, extractedStageThresholds.size() );
        assertEquals( 7, extractedStageThresholds.get( extractedStageThresholds.keySet().iterator().next() ).size() );
    }

    @Test
    void testExtractThresholdsWithUnexpectedRatingProviderThrowsExpectedException()
    {
        // Mock the unit mapper
        UnitMapper mapper = Mockito.mock( UnitMapper.class );
        Mockito.when( mapper.getUnitMapper( "CFS" ) )
               .thenReturn( next -> next );
        Mockito.when( mapper.getDesiredMeasurementUnitName() )
               .thenReturn( "CFS" );

        NoThresholdsFoundException actual = assertThrows( NoThresholdsFoundException.class, // NOSONAR
                                                          () -> this.extractor.readFlow()
                                                                              .from( "NWS-NRLDB" )
                                                                              .ratingFrom( "FooBar" )
                                                                              .convertTo( mapper )
                                                                              .extract() );

        String expectedMessage = "While attempting to filter WRDS thresholds against the user-declared threshold "
                                 + "ratings provider 'FooBar', discovered no thresholds that match the ratings "
                                 + "provider within the WRDS response. The WRDS response contained 1 threshold "
                                 + "definitions with the following ratings providers: [NRLDB]. Choose one of these "
                                 + "providers instead.";

        assertEquals( expectedMessage, actual.getMessage() );
    }

    @Test
    void testExtractThresholdsWithUnexpectedThresholdProviderThrowsExpectedException()
    {
        // Mock the unit mapper
        UnitMapper mapper = Mockito.mock( UnitMapper.class );
        Mockito.when( mapper.getUnitMapper( "CFS" ) )
               .thenReturn( next -> next );
        Mockito.when( mapper.getDesiredMeasurementUnitName() )
               .thenReturn( "CFS" );

        NoThresholdsFoundException actual = assertThrows( NoThresholdsFoundException.class, // NOSONAR
                                                          () -> this.extractor.readFlow()
                                                                              .from( "Baz" )
                                                                              .ratingFrom( "NRLDB" )
                                                                              .convertTo( mapper )
                                                                              .extract() );

        String expectedMessage = "While attempting to filter WRDS thresholds against the user-declared threshold "
                                 + "provider 'Baz', discovered no thresholds that match the provider within the WRDS "
                                 + "response. The WRDS response contained 1 threshold definitions with the following "
                                 + "threshold providers: [NWS-NRLDB]. Choose one of these providers instead.";

        assertEquals( expectedMessage, actual.getMessage() );
    }

    private ThresholdResponse createOldThresholdResponse()
    {
        ThresholdMetadata ptsa1NWSMetadata = new ThresholdMetadata();
        ptsa1NWSMetadata.setLocationId( "PTSA1" );
        ptsa1NWSMetadata.setNwsLid( "PTSA1" );
        ptsa1NWSMetadata.setUsgsSiteCode( "02372250" );
        ptsa1NWSMetadata.setNwmFeatureId( "2323396" );
        ptsa1NWSMetadata.setThresholdSource( "NWS-CMS" );
        ptsa1NWSMetadata.setStageUnit( "FT" );

        OriginalThresholdValues ptsa1NWSOriginalValues = new OriginalThresholdValues();
        ptsa1NWSOriginalValues.setLowStage( "None" );
        ptsa1NWSOriginalValues.setBankfullStage( "None" );
        ptsa1NWSOriginalValues.setActionStage( "0.0" );
        ptsa1NWSOriginalValues.setMinorStage( "0.0" );
        ptsa1NWSOriginalValues.setModerateStage( "0.0" );
        ptsa1NWSOriginalValues.setMajorStage( "0.0" );
        ptsa1NWSOriginalValues.setRecordStage( "None" );

        ThresholdDefinition ptsa1NWS = new ThresholdDefinition();
        ptsa1NWS.setMetadata( ptsa1NWSMetadata );
        ptsa1NWS.setOriginalValues( ptsa1NWSOriginalValues );
        ptsa1NWS.setCalculatedValues( new CalculatedThresholdValues() );

        ThresholdMetadata mntg1NWSMetadata = new ThresholdMetadata();
        mntg1NWSMetadata.setLocationId( "MNTG1" );
        mntg1NWSMetadata.setNwsLid( "MNTG1" );
        mntg1NWSMetadata.setUsgsSiteCode( "02349605" );
        mntg1NWSMetadata.setNwmFeatureId( "6444276" );
        mntg1NWSMetadata.setThresholdSource( "NWS-CMS" );
        mntg1NWSMetadata.setStageUnit( "FT" );

        OriginalThresholdValues mntg1NWSOriginalValues = new OriginalThresholdValues();
        mntg1NWSOriginalValues.setLowStage( "None" );
        mntg1NWSOriginalValues.setBankfullStage( "None" );
        mntg1NWSOriginalValues.setActionStage( "11.0" );
        mntg1NWSOriginalValues.setMinorStage( "20.0" );
        mntg1NWSOriginalValues.setModerateStage( "28.0" );
        mntg1NWSOriginalValues.setMajorStage( "31.0" );
        mntg1NWSOriginalValues.setRecordStage( "None" );

        ThresholdDefinition mntg1NWS = new ThresholdDefinition();
        mntg1NWS.setMetadata( mntg1NWSMetadata );
        mntg1NWS.setOriginalValues( mntg1NWSOriginalValues );
        mntg1NWS.setCalculatedValues( new CalculatedThresholdValues() );

        ThresholdMetadata mntg1NRLDBMetadata = new ThresholdMetadata();
        mntg1NRLDBMetadata.setLocationId( "MNTG1" );
        mntg1NRLDBMetadata.setNwsLid( "MNTG1" );
        mntg1NRLDBMetadata.setUsgsSiteCode( "02349605" );
        mntg1NRLDBMetadata.setNwmFeatureId( "6444276" );
        mntg1NRLDBMetadata.setThresholdSource( "NWS-NRLDB" );
        mntg1NRLDBMetadata.setStageUnit( "FT" );

        OriginalThresholdValues mntg1NRLDBOriginalValues = new OriginalThresholdValues();
        mntg1NRLDBOriginalValues.setLowStage( "None" );
        mntg1NRLDBOriginalValues.setBankfullStage( "11.0" );
        mntg1NRLDBOriginalValues.setActionStage( "11.0" );
        mntg1NRLDBOriginalValues.setMinorStage( "20.0" );
        mntg1NRLDBOriginalValues.setModerateStage( "28.0" );
        mntg1NRLDBOriginalValues.setMajorStage( "31.0" );
        mntg1NRLDBOriginalValues.setRecordStage( "34.11" );

        ThresholdDefinition mntg1NRLDB = new ThresholdDefinition();
        mntg1NRLDB.setMetadata( mntg1NRLDBMetadata );
        mntg1NRLDB.setOriginalValues( mntg1NRLDBOriginalValues );
        mntg1NRLDB.setCalculatedValues( new CalculatedThresholdValues() );

        ThresholdResponse response = new ThresholdResponse();
        response.setThresholds( List.of( ptsa1NWS, mntg1NWS, mntg1NRLDB ) );

        return response;
    }


    @Test
    void testExtractOld()
    {
        ThresholdExtractor extractor =
                new ThresholdExtractor( oldResponse ).readStage()
                                                     .convertTo( unitMapper )
                                                     .from( "NWS-NRLDB" )
                                                     .ratingFrom( null )
                                                     .operatesBy( ThresholdConstants.Operator.GREATER )
                                                     .onSide( ThresholdConstants.ThresholdDataType.LEFT );
        Map<WrdsLocation, Set<Threshold>> normalExtraction = extractor.extract();

        assertFalse( normalExtraction.containsKey( PTSA1 ) );
        assertTrue( normalExtraction.containsKey( MNTG1 ) );

        Map<String, Double> thresholdValues = new HashMap<>();
        thresholdValues.put( "record", 34.11 );
        thresholdValues.put( "bankfull", 11.0 );
        thresholdValues.put( "action", 11.0 );
        thresholdValues.put( "major", 31.0 );
        thresholdValues.put( "minor", 20.0 );
        thresholdValues.put( "moderate", 28.0 );

        Set<Threshold> normalOuterThresholds = normalExtraction.get( MNTG1 );

        assertEquals( 6, normalOuterThresholds.size() );

        for ( Threshold threshold : normalOuterThresholds )
        {
            assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            assertEquals(
                    thresholdValues.get( threshold.getName() ),
                    threshold.getLeftThresholdValue().getValue(),
                    EPSILON );
        }

    }

}
