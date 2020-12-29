package wres.io.thresholds.wrds.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.datamodel.thresholds.ThresholdOuter;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.exceptions.NoThresholdsFoundException;

/**
 * Tests the {@link ThresholdExtractor}.
 * 
 * @author james.brown@hydrosolved.com
 */

class ThresholdExtractorTest
{

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
        aThresholdMetadata.setLocation_id( "BLUO2" );
        aThresholdMetadata.setNws_lid( "BLUO2" );
        aThresholdMetadata.setUsgs_site_code( "07332500" );

        aThresholdMetadata.setNwm_feature_id( "700694" );
        aThresholdMetadata.setId_type( "NWS Station" );
        aThresholdMetadata.setThreshold_source( "NWS-NRLDB" );
        aThresholdMetadata.setThreshold_source_description( "National Weather Service - National River Location "
                                                            + "Database" );
        aThresholdMetadata.setRating_source( "NRLDB" );
        aThresholdMetadata.setRating_source_description( "NRLDB" );
        aThresholdMetadata.setFlow_unit( "CFS" );
        aThresholdMetadata.setStage_unit( "FT" );

        aThreshold.setMetadata( aThresholdMetadata );

        OriginalThresholdValues originalValues = new OriginalThresholdValues();
        originalValues.setLow_stage( "1.5" );
        originalValues.setBankfull_stage( "28.0" );
        originalValues.setAction_stage( "26.0" );
        originalValues.setMinor_stage( "28.0" );
        originalValues.setModerate_stage( "31.0" );
        originalValues.setMajor_stage( "43.0" );
        originalValues.setRecord_stage( "50.83" );

        aThreshold.setOriginal_values( originalValues );

        CalculatedThresholdValues calculatedValues = new CalculatedThresholdValues();
        calculatedValues.setLow_flow( "0.0" );
        calculatedValues.setBankfull_flow( "7614.916318407959" );
        calculatedValues.setAction_flow( "6608.18" );
        calculatedValues.setMinor_flow( "7614.916318407959" );
        calculatedValues.setModerate_flow( "9248.87268292683" );
        calculatedValues.setMajor_flow( "49279.61515624999" );
        calculatedValues.setRecord_flow( "102933.03851562498" );

        aThreshold.setCalculated_values( calculatedValues );

        thresholds.add( aThreshold );

        thresholdResponse.setThresholds( thresholds );
        Map<String, Double> metrics = new HashMap<>();
        metrics.put( "threshold_count", 1.0 );
        thresholdResponse.set_metrics( metrics );
        thresholdResponse.set_documentation( "***REMOVED***-dev.***REMOVED***.***REMOVED***/docs/dev/v2/location/swagger/" );

        this.extractor = new ThresholdExtractor( thresholdResponse );
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

        Map<WrdsLocation, Set<ThresholdOuter>> extractedFlowThresholds = extractor.readFlow()
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

        Map<WrdsLocation, Set<ThresholdOuter>> extractedStageThresholds = extractor.readStage()
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

        NoThresholdsFoundException actual = assertThrows( NoThresholdsFoundException.class,
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

        NoThresholdsFoundException actual = assertThrows( NoThresholdsFoundException.class,
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

}
