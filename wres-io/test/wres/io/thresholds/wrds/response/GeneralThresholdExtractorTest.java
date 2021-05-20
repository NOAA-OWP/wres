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

class GeneralThresholdExtractorTest
{

    /**
     * Test instance.
     */

    private GeneralThresholdExtractor extractor = null;

    @BeforeEach
    void runBeforeEachTest()
    {
        // Based on the first threshold in the example from #82996-2. If the api is adapted in a breaking way, this will 
        // need to be adapted too.

        // Create a ThresholdResponse for testing and then create a ThresholdExtractor from that
        GeneralThresholdResponse thresholdResponse = new GeneralThresholdResponse();

        Collection<GeneralThresholdDefinition> thresholds = new ArrayList<>();

        //Metadata
        GeneralThresholdDefinition aThreshold = new GeneralThresholdDefinition();
        GeneralThresholdMetadata aThresholdMetadata = new GeneralThresholdMetadata();
        aThresholdMetadata.setNws_lid( "BLUO2" );
        aThresholdMetadata.setUsgs_site_code( "07332500" );
        aThresholdMetadata.setNwm_feature_id( "700694" );
        aThresholdMetadata.setThreshold_source( "NWS-NRLDB" );
        aThresholdMetadata.setThreshold_source_description( "National Weather Service - National River Location "
                                                            + "Database" );
        aThresholdMetadata.setFlow_units( "CFS" );
        aThresholdMetadata.setCalc_flow_units( "CFS" );
        aThresholdMetadata.setStage_units( "FT" );
        aThreshold.setMetadata( aThresholdMetadata );
        
        //Stage thresholds
        GeneralThresholdValues stageValues = new GeneralThresholdValues();
        stageValues.add( "low", 1.5d );
        stageValues.add( "bankfull", 28.0d );
        stageValues.add( "action", 26.0d );
        stageValues.add( "minor", 28.0d );
        stageValues.add( "moderate", 31.0d );
        stageValues.add( "major", 43.0d );
        stageValues.add( "record", 50.83d );
        aThreshold.setStage_values( stageValues );
        
        //Calculated flow thresholds with rating curve info.
        GeneralThresholdValues calcFlowValues = new GeneralThresholdValues();
        calcFlowValues.add( "low", 0.0d );
        calcFlowValues.add( "bankfull", 7614.916318407959d );
        calcFlowValues.add( "action", 6608.18d );
        calcFlowValues.add( "minor", 7614.916318407959d );
        calcFlowValues.add( "moderate", 9248.87268292683d );
        calcFlowValues.add( "major", 49279.61515624999d );
        calcFlowValues.add( "record", 102933.03851562498d );
        RatingCurveInfo rcInfo = new RatingCurveInfo();
        rcInfo.setSource( "NRLDB" );
        rcInfo.setDescription( "NRLDB" );
        calcFlowValues.setRating_curve( rcInfo );
        aThreshold.setCalc_flow_values( calcFlowValues );

        thresholds.add( aThreshold );

        thresholdResponse.setThresholds( thresholds );
        Map<String, Double> metrics = new HashMap<>();
        metrics.put( "threshold_count", 1.0 );

        this.extractor = new GeneralThresholdExtractor( thresholdResponse );
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
