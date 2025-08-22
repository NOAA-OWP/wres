package wres.reading.wrds.thresholds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.components.ThresholdOperator;
import wres.config.components.ThresholdOrientation;
import wres.reading.wrds.geography.Location;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link ThresholdExtractor}.  This has been modified to include a test that was
 * inappropriately located within another test class.  So, if it looks like a hodgepodge, that's
 * because it reflects two tests in two different files combined into one.  I have not refactored
 * it further. TODO: reduce the number of assertions - assert against complete thresholds, not individual parts
 * @author Hank Herr
 * @author James Brown
 */

class ThresholdExtractorTest
{
    //The file used is created from this URL:
    //
    //https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/
    //
    //executed on 5/22/2021 in the afternoon.

    private static final double EPSILON = 0.00001;

    private static final Location PTSA1 = createFeature( "2323396", "02372250", "PTSA1" );
    private static final Location MNTG1 = createFeature( "6444276", "02349605", "MNTG1" );

    private static final Location STEAK = createFeature( null, null, "STEAK" );
    private static final Location BAKED_POTATO = createFeature( null, null, "BakedPotato" );
    private ThresholdResponse normalResponse = null;
    private ThresholdResponse funResponse = null;

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

        //Metadata
        ThresholdDefinition aThreshold = new ThresholdDefinition();
        ThresholdMetadata aThresholdMetadata = new ThresholdMetadata();
        aThresholdMetadata.setNwsLid( "BLUO2" );
        aThresholdMetadata.setUsgsSideCode( "07332500" );
        aThresholdMetadata.setNwmFeatureId( "700694" );
        aThresholdMetadata.setThresholdSource( "NWS-NRLDB" );
        aThresholdMetadata.setThresholdSourceDescription( "National Weather Service - National River Location "
                                                          + "Database" );
        aThresholdMetadata.setFlowUnits( "CFS" );
        aThresholdMetadata.setCalcFlowUnits( "CFS" );
        aThresholdMetadata.setStageUnits( "FT" );
        aThreshold.setMetadata( aThresholdMetadata );

        //Stage thresholds
        ThresholdValues stageValues = new ThresholdValues();
        stageValues.add( "low", 1.5d );
        stageValues.add( "bankfull", 28.0d );
        stageValues.add( "action", 26.0d );
        stageValues.add( "minor", 28.0d );
        stageValues.add( "moderate", 31.0d );
        stageValues.add( "major", 43.0d );
        stageValues.add( "record", 50.83d );
        aThreshold.setStageValues( stageValues );

        //Calculated flow thresholds with rating curve info.
        ThresholdValues calcFlowValues = new ThresholdValues();
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
        calcFlowValues.setRatingCurve( rcInfo );
        aThreshold.setCalcFlowValues( calcFlowValues );

        thresholds.add( aThreshold );

        thresholdResponse.setThresholds( thresholds );

        this.extractor = ThresholdExtractor.builder()
                                           .response( thresholdResponse )
                                           .build();

        this.normalResponse = this.createNormalThresholdResponse();
        this.funResponse = this.createFunThresholdResponse();
    }

    @Test
    void testExtractThresholdsForOneFeatureGeneratesExpectedThresholdCounts()
    {
        Map<Location, Set<Threshold>> extractedFlowThresholds = this.extractor.toBuilder()
                                                                              .type( ThresholdType.FLOW )
                                                                              .provider( "NWS-NRLDB" )
                                                                              .ratingProvider( "NRLDB" )
                                                                              .build()
                                                                              .extract();

        // Yes, this is a relatively weak assertion. Other tests could be added with stronger assertions or this
        // test could be replaced.
        assertEquals( 1, extractedFlowThresholds.size() );
        assertEquals( 7, extractedFlowThresholds.get( extractedFlowThresholds.keySet().iterator().next() ).size() );

        Map<Location, Set<Threshold>> extractedStageThresholds = this.extractor.toBuilder()
                                                                               .type( ThresholdType.STAGE )
                                                                               .provider( "NWS-NRLDB" )
                                                                               .ratingProvider( "NRLDB" )
                                                                               .build()
                                                                               .extract();

        // Yes, this is a relatively weak assertion. Other tests could be added with stronger assertions or this
        // test could be replaced.
        assertEquals( 1, extractedStageThresholds.size() );
        assertEquals( 7, extractedStageThresholds.get( extractedStageThresholds.keySet()
                                                                               .iterator()
                                                                               .next() )
                                                 .size() );
    }

    @Test
    void testExtract()
    {
        ThresholdExtractor extractor = ThresholdExtractor.builder()
                                                         .response( normalResponse )
                                                         .type( ThresholdType.STAGE )
                                                         .provider( "NWS-NRLDB" )
                                                         .operator( ThresholdOperator.GREATER )
                                                         .orientation( ThresholdOrientation.LEFT )
                                                         .build();

        Map<Location, Set<Threshold>> normalExtraction = extractor.extract();

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
            assertEquals( thresholdValues.get( threshold.getName() ),
                          threshold.getLeftThresholdValue(),
                          EPSILON );
        }


        //This a test of flow thresholds.  See the funThresholdConfig for more information.
        extractor = ThresholdExtractor.builder()
                                      .response( funResponse )
                                      .type( ThresholdType.FLOW )
                                      .provider( "FlavorTown" )
                                      .ratingProvider( "DonkeySauce" )
                                      .operator( ThresholdOperator.GREATER )
                                      .orientation( ThresholdOrientation.LEFT_AND_ANY_RIGHT )
                                      .build();

        Map<Location, Set<Threshold>> funExtraction = extractor.extract();

        assertTrue( funExtraction.containsKey( STEAK ) );
        assertTrue( funExtraction.containsKey( BAKED_POTATO ) );

        assertEquals( 14, funExtraction.get( STEAK ).size() );
        assertEquals( 11, funExtraction.get( BAKED_POTATO ).size() );

        thresholdValues.put( "bankfull", 14.586 );
        thresholdValues.put( "low", 5.7 );
        thresholdValues.put( "action", 13.5 );
        thresholdValues.put( "minor", 189.42 );
        thresholdValues.put( "moderate", 868.5 );
        thresholdValues.put( "major", 90144.2 );
        thresholdValues.put( "record", 4846844.5484 );

        thresholdValues.put( "DonkeySauce bankfull", -14.586 );
        thresholdValues.put( "DonkeySauce low", -5.7 );
        thresholdValues.put( "DonkeySauce action", -13.5 );
        thresholdValues.put( "DonkeySauce minor", -189.42 );
        thresholdValues.put( "DonkeySauce moderate", -868.5 );
        thresholdValues.put( "DonkeySauce major", -90144.2 );
        thresholdValues.put( "DonkeySauce record", -4846844.5484 );

        for ( Threshold threshold : funExtraction.get( STEAK ) )
        {
            assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                          threshold.getDataType() );
            assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            assertTrue( thresholdValues.containsKey( threshold.getName() ) );

            assertEquals( thresholdValues.get( threshold.getName() ),
                          threshold.getLeftThresholdValue(),
                          EPSILON );
        }

        thresholdValues = new HashMap<>();

        thresholdValues.put( "low", 57.0 );
        thresholdValues.put( "bankfull", 1458.6 );
        thresholdValues.put( "minor", 142.0 );
        thresholdValues.put( "moderate", 86.85 );
        thresholdValues.put( "major", 9.2 );
        thresholdValues.put( "record", 4.35 );

        thresholdValues.put( "DonkeySauce low", 54.7 );
        thresholdValues.put( "DonkeySauce minor", 18942.0 );
        thresholdValues.put( "DonkeySauce moderate", 88.5 );
        thresholdValues.put( "DonkeySauce major", 901.2 );
        thresholdValues.put( "DonkeySauce record", 6844.84 );

        for ( Threshold threshold : funExtraction.get( BAKED_POTATO ) )
        {
            assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT, threshold.getDataType() );
            assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            assertTrue( thresholdValues.containsKey( threshold.getName() ) );

            assertEquals( thresholdValues.get( threshold.getName() ),
                          threshold.getLeftThresholdValue(),
                          EPSILON );
        }

        //This is a test of stage thresholds
        extractor = ThresholdExtractor.builder()
                                      .response( normalResponse )
                                      .type( ThresholdType.STAGE )
                                      .provider( "NWS-CMS" )
                                      .operator( ThresholdOperator.LESS )
                                      .orientation( ThresholdOrientation.LEFT )
                                      .build();

        Map<Location, Set<Threshold>> alternativeNormalExtraction = extractor.extract();

        assertTrue( alternativeNormalExtraction.containsKey( PTSA1 ) );
        assertTrue( alternativeNormalExtraction.containsKey( MNTG1 ) );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "action", 11.0 );
        thresholdValues.put( "major", 31.0 );
        thresholdValues.put( "minor", 20.0 );
        thresholdValues.put( "moderate", 28.0 );

        assertEquals( 4, alternativeNormalExtraction.get( MNTG1 ).size() );

        for ( Threshold threshold : alternativeNormalExtraction.get( MNTG1 ) )
        {
            assertEquals( Threshold.ThresholdOperator.LESS, threshold.getOperator() );
            assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            assertEquals(
                    thresholdValues.get( threshold.getName() ),
                    threshold.getLeftThresholdValue(),
                    EPSILON );
        }

        assertEquals( 4, alternativeNormalExtraction.get( PTSA1 ).size() );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "action", 0.0 );
        thresholdValues.put( "major", 0.0 );
        thresholdValues.put( "minor", 0.0 );
        thresholdValues.put( "moderate", 0.0 );

        for ( Threshold threshold : alternativeNormalExtraction.get( PTSA1 ) )
        {
            assertEquals( Threshold.ThresholdOperator.LESS, threshold.getOperator() );
            assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            assertEquals( thresholdValues.get( threshold.getName() ),
                          threshold.getLeftThresholdValue(),
                          EPSILON );
        }

        //This is a test of stage thresholds
        extractor = ThresholdExtractor.builder()
                                      .response( funResponse )
                                      .type( ThresholdType.STAGE )
                                      .provider( "NWS-NRLDB" )
                                      .operator( ThresholdOperator.GREATER )
                                      .orientation( ThresholdOrientation.LEFT )
                                      .build();

        Map<Location, Set<Threshold>> normalButFunExtraction = extractor.extract();

        assertTrue( normalButFunExtraction.containsKey( STEAK ) );
        assertTrue( normalButFunExtraction.containsKey( BAKED_POTATO ) );

        //Since calculated stages aren't used, and STEAK includes no regular stage thresholds 
        //these counts and expected resultswere updated for API 3.0.
        assertEquals( 0, normalButFunExtraction.get( STEAK ).size() );
        assertEquals( 3, normalButFunExtraction.get( BAKED_POTATO ).size() );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "minor", 5.54 );
        thresholdValues.put( "moderate", 4.0 );
        thresholdValues.put( "major", 158.0 );

        for ( Threshold threshold : normalButFunExtraction.get( BAKED_POTATO ) )
        {
            assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            assertEquals( thresholdValues.get( threshold.getName() ),
                          threshold.getLeftThresholdValue(),
                          EPSILON );
        }
    }

    private static Location createFeature( final String featureId, final String usgsSiteCode, final String lid )
    {
        return new Location( featureId, usgsSiteCode, lid );
    }

    private ThresholdResponse createNormalThresholdResponse()
    {
        // ==== First set of thresholds.

        //Metadata
        ThresholdMetadata ptsa1NWSMetadata = new ThresholdMetadata();
        ptsa1NWSMetadata.setNwsLid( "PTSA1" );
        ptsa1NWSMetadata.setUsgsSideCode( "02372250" );
        ptsa1NWSMetadata.setNwmFeatureId( "2323396" );
        ptsa1NWSMetadata.setThresholdSource( "NWS-CMS" );
        ptsa1NWSMetadata.setThresholdSourceDescription( "NONE" );
        ptsa1NWSMetadata.setFlowUnits( "CFS" );
        ptsa1NWSMetadata.setCalcFlowUnits( "CFS" );
        ptsa1NWSMetadata.setStageUnits( "FT" );

        //Stage thresholds
        ThresholdValues ptsa1NWSOriginalValues = new ThresholdValues();
        ptsa1NWSOriginalValues.add( "low", null );
        ptsa1NWSOriginalValues.add( "bankfull", null );
        ptsa1NWSOriginalValues.add( "action", 0.0d );
        ptsa1NWSOriginalValues.add( "minor", 0.0d );
        ptsa1NWSOriginalValues.add( "moderate", 0.0d );
        ptsa1NWSOriginalValues.add( "major", 0.0d );
        ptsa1NWSOriginalValues.add( "record", null );

        //No flow or calculated.
        ThresholdDefinition ptsa1NWS = new ThresholdDefinition();
        ptsa1NWS.setMetadata( ptsa1NWSMetadata );
        ptsa1NWS.setStageValues( ptsa1NWSOriginalValues );

        // ==== Second set of thresholds.

        //Metadata
        ThresholdMetadata mntg1NWSMetadata = new ThresholdMetadata();
        mntg1NWSMetadata.setNwsLid( "MNTG1" );
        mntg1NWSMetadata.setUsgsSideCode( "02349605" );
        mntg1NWSMetadata.setNwmFeatureId( "6444276" );
        mntg1NWSMetadata.setThresholdSource( "NWS-CMS" );
        mntg1NWSMetadata.setThresholdSourceDescription( "NONE" );
        mntg1NWSMetadata.setFlowUnits( "CFS" );
        mntg1NWSMetadata.setCalcFlowUnits( "CFS" );
        mntg1NWSMetadata.setStageUnits( "FT" );

        //Stage thresholds
        ThresholdValues mntg1NWSOriginalValues = new ThresholdValues();
        mntg1NWSOriginalValues.add( "low", null );
        mntg1NWSOriginalValues.add( "bankfull", null );
        mntg1NWSOriginalValues.add( "action", 11.0d );
        mntg1NWSOriginalValues.add( "minor", 20.0d );
        mntg1NWSOriginalValues.add( "moderate", 28.0d );
        mntg1NWSOriginalValues.add( "major", 31.0d );
        mntg1NWSOriginalValues.add( "record", null );

        //No flow or calculated.
        ThresholdDefinition mntg1NWS = new ThresholdDefinition();
        mntg1NWS.setMetadata( mntg1NWSMetadata );
        mntg1NWS.setStageValues( mntg1NWSOriginalValues );

        // ==== Third set of thresholds.

        //Metadata
        ThresholdMetadata mntg1NRLDBMetadata = new ThresholdMetadata();
        mntg1NRLDBMetadata.setNwsLid( "MNTG1" );
        mntg1NRLDBMetadata.setUsgsSideCode( "02349605" );
        mntg1NRLDBMetadata.setNwmFeatureId( "6444276" );
        mntg1NRLDBMetadata.setThresholdSource( "NWS-NRLDB" );
        mntg1NRLDBMetadata.setThresholdSourceDescription( "NONE" );
        mntg1NRLDBMetadata.setFlowUnits( "CFS" );
        mntg1NRLDBMetadata.setCalcFlowUnits( "CFS" );
        mntg1NRLDBMetadata.setStageUnits( "FT" );

        //Stage thresholds
        ThresholdValues mntg1NRLDBOriginalValues = new ThresholdValues();
        mntg1NRLDBOriginalValues.add( "low", null );
        mntg1NRLDBOriginalValues.add( "bankfull", 11.0d );
        mntg1NRLDBOriginalValues.add( "action", 11.0d );
        mntg1NRLDBOriginalValues.add( "minor", 20.0d );
        mntg1NRLDBOriginalValues.add( "moderate", 28.0d );
        mntg1NRLDBOriginalValues.add( "major", 31.0d );
        mntg1NRLDBOriginalValues.add( "record", 34.11d );

        //No flow or calculated.
        ThresholdDefinition mntg1NRLDB = new ThresholdDefinition();
        mntg1NRLDB.setMetadata( mntg1NRLDBMetadata );
        mntg1NRLDB.setStageValues( mntg1NRLDBOriginalValues );

        ThresholdResponse response = new ThresholdResponse();
        response.setThresholds( List.of( ptsa1NWS, mntg1NWS, mntg1NRLDB ) );

        return response;
    }

    private ThresholdResponse createFunThresholdResponse()
    {
        // ==== First set of thresholds.

        //Metadata
        ThresholdMetadata steakMetadata = new ThresholdMetadata();
        steakMetadata.setNwsLid( "STEAK" );
        steakMetadata.setThresholdSource( "FlavorTown" );
        steakMetadata.setFlowUnits( "CFS" );
        steakMetadata.setCalcFlowUnits( "CFS" );
        steakMetadata.setStageUnits( "MM" );

        //Stage thresholds
        ThresholdValues steakStageValues = new ThresholdValues();
        steakStageValues.add( "low", 99.586168d );
        steakStageValues.add( "bankfull", 120.50d );
        steakStageValues.add( "action", 180.58d );
        steakStageValues.add( "minor", 350.5419d );
        steakStageValues.add( "moderate", 420.0d );
        steakStageValues.add( "major", 8054.54d );
        steakStageValues.add( "record", 9999.594d );

        //Flow thresholds
        ThresholdValues steakFlowValues = new ThresholdValues();
        steakFlowValues.add( "low", 5.7d );
        steakFlowValues.add( "bankfull", 14.586d );
        steakFlowValues.add( "action", 13.5d );
        steakFlowValues.add( "minor", 189.42d );
        steakFlowValues.add( "moderate", 868.5d );
        steakFlowValues.add( "major", 90144.2d );
        steakFlowValues.add( "record", 4846844.5484d );

        //Calculated flow thresholds with rating curve info.
        ThresholdValues steakCalcFlowValues = new ThresholdValues();
        steakCalcFlowValues.add( "low", -5.7d );
        steakCalcFlowValues.add( "bankfull", -14.586d );
        steakCalcFlowValues.add( "action", -13.5d );
        steakCalcFlowValues.add( "minor", -189.42d );
        steakCalcFlowValues.add( "moderate", -868.5d );
        steakCalcFlowValues.add( "major", -90144.2d );
        steakCalcFlowValues.add( "record", -4846844.5484d );
        RatingCurveInfo rcInfo = new RatingCurveInfo();
        rcInfo.setSource( "DonkeySauce" );
        rcInfo.setDescription( "NONE" );
        steakCalcFlowValues.setRatingCurve( rcInfo );

        //The definition
        ThresholdDefinition steakDef = new ThresholdDefinition();
        steakDef.setMetadata( steakMetadata );
        steakDef.setStageValues( steakStageValues );
        steakDef.setFlowValues( steakFlowValues );
        steakDef.setCalcFlowValues( steakCalcFlowValues );

        // ==== Second set of thresholds

        //Metadata
        ThresholdMetadata grossSteakMetadata = new ThresholdMetadata();
        grossSteakMetadata.setNwsLid( "STEAK" );
        grossSteakMetadata.setThresholdSource( "NWS-NRLDB" );
        grossSteakMetadata.setFlowUnits( "CFS" );
        grossSteakMetadata.setCalcFlowUnits( "CFS" );
        grossSteakMetadata.setStageUnits( "MM" );

        //Calculated flow thresholds
        ThresholdValues grossSteakCalcFlowValues = new ThresholdValues();
        grossSteakCalcFlowValues.add( "low", -57d );
        grossSteakCalcFlowValues.add( "bankfull", null );
        grossSteakCalcFlowValues.add( "action", -13.5d );
        grossSteakCalcFlowValues.add( "minor", null );
        grossSteakCalcFlowValues.add( "moderate", 14d );
        grossSteakCalcFlowValues.add( "major", -9014.2d );
        grossSteakCalcFlowValues.add( "record", -46844.5484d );
        RatingCurveInfo grossRCInfo = new RatingCurveInfo();
        grossRCInfo.setSource( "DuckSauce" );
        grossRCInfo.setDescription( "NONE" );
        grossSteakCalcFlowValues.setRatingCurve( grossRCInfo );

        //The definition; no stage or flow.
        ThresholdDefinition grossSteakDef = new ThresholdDefinition();
        grossSteakDef.setMetadata( grossSteakMetadata );
        grossSteakDef.setCalcFlowValues( grossSteakCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        ThresholdMetadata flatIronSteakMetadata = new ThresholdMetadata();
        flatIronSteakMetadata.setNwsLid( "STEAK" );
        flatIronSteakMetadata.setThresholdSource( "FlatIron" );
        flatIronSteakMetadata.setFlowUnits( "CFS" );
        flatIronSteakMetadata.setCalcFlowUnits( "CFS" );
        flatIronSteakMetadata.setStageUnits( "MM" );

        //Stage thresholds
        ThresholdValues flatIronSteakStageValues = new ThresholdValues();
        flatIronSteakStageValues.add( "low", 99.586168d );
        flatIronSteakStageValues.add( "bankfull", 120.50d );
        flatIronSteakStageValues.add( "action", 180.58d );
        flatIronSteakStageValues.add( "minor", 350.5419d );
        flatIronSteakStageValues.add( "moderate", 420.0d );
        flatIronSteakStageValues.add( "major", 8054.54d );
        flatIronSteakStageValues.add( "record", 9999.594d );

        //Flow thresholds
        ThresholdValues flatIronSteakFlowValues = new ThresholdValues();
        flatIronSteakFlowValues.add( "low", 5.7d );
        flatIronSteakFlowValues.add( "bankfull", 14.586d );
        flatIronSteakFlowValues.add( "action", 13.5d );
        flatIronSteakFlowValues.add( "minor", 189.42d );
        flatIronSteakFlowValues.add( "moderate", 868.5d );
        flatIronSteakFlowValues.add( "major", 90144.2d );
        flatIronSteakFlowValues.add( "record", 4846844.5484d );

        //Calculated flow thresholds with rating curve info.
        ThresholdValues flatIronSteakCalcFlowValues = new ThresholdValues();
        flatIronSteakCalcFlowValues.add( "low", -5.7d );
        flatIronSteakCalcFlowValues.add( "bankfull", -14.586d );
        flatIronSteakCalcFlowValues.add( "action", -13.5d );
        flatIronSteakCalcFlowValues.add( "minor", -189.42d );
        flatIronSteakCalcFlowValues.add( "moderate", -868.5d );
        flatIronSteakCalcFlowValues.add( "major", -90144.2d );
        flatIronSteakCalcFlowValues.add( "record", -4846844.5484d );
        RatingCurveInfo flatIronRCInfo = new RatingCurveInfo();
        flatIronRCInfo.setSource( "DonkeySauce" );
        flatIronRCInfo.setDescription( "NONE" );
        flatIronSteakCalcFlowValues.setRatingCurve( flatIronRCInfo );

        //The definition
        ThresholdDefinition flatIronSteakDef = new ThresholdDefinition();
        flatIronSteakDef.setMetadata( flatIronSteakMetadata );
        flatIronSteakDef.setStageValues( flatIronSteakStageValues );
        flatIronSteakDef.setFlowValues( flatIronSteakFlowValues );
        flatIronSteakDef.setCalcFlowValues( flatIronSteakCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        ThresholdMetadata bakedPotatoMetadata = new ThresholdMetadata();
        bakedPotatoMetadata.setNwsLid( "BakedPotato" );
        bakedPotatoMetadata.setThresholdSource( "FlavorTown" );
        bakedPotatoMetadata.setFlowUnits( "CFS" );
        bakedPotatoMetadata.setCalcFlowUnits( "CFS" );
        bakedPotatoMetadata.setStageUnits( "MM" );

        //Stage thresholds
        ThresholdValues bakedPotatoStageValues = new ThresholdValues();
        bakedPotatoStageValues.add( "low", 9.586168d );
        bakedPotatoStageValues.add( "bankfull", null );
        bakedPotatoStageValues.add( "action", null );
        bakedPotatoStageValues.add( "minor", 50.54d );
        bakedPotatoStageValues.add( "moderate", 42.0d );
        bakedPotatoStageValues.add( "major", null );
        bakedPotatoStageValues.add( "record", null );

        //Flow thresholds
        ThresholdValues bakedPotatoFlowValues = new ThresholdValues();
        bakedPotatoFlowValues.add( "low", 57d );
        bakedPotatoFlowValues.add( "bankfull", 1458.6d );
        bakedPotatoFlowValues.add( "action", null );
        bakedPotatoFlowValues.add( "minor", 142d );
        bakedPotatoFlowValues.add( "moderate", 86.85d );
        bakedPotatoFlowValues.add( "major", 9.2d );
        bakedPotatoFlowValues.add( "record", 4.35d );

        //Calculated flow thresholds with rating curve info.
        ThresholdValues bakedPotatoCalcFlowValues = new ThresholdValues();
        bakedPotatoCalcFlowValues.add( "low", 54.7d );
        bakedPotatoCalcFlowValues.add( "bankfull", null );
        bakedPotatoCalcFlowValues.add( "action", null );
        bakedPotatoCalcFlowValues.add( "minor", 18942d );
        bakedPotatoCalcFlowValues.add( "moderate", 88.5d );
        bakedPotatoCalcFlowValues.add( "major", 901.2d );
        bakedPotatoCalcFlowValues.add( "record", 6844.84 );
        RatingCurveInfo bakedPotatoRCInfo = new RatingCurveInfo();
        bakedPotatoRCInfo.setSource( "DonkeySauce" );
        bakedPotatoRCInfo.setDescription( "NONE" );
        bakedPotatoCalcFlowValues.setRatingCurve( bakedPotatoRCInfo );

        //The definition
        ThresholdDefinition bakedPotatoDef = new ThresholdDefinition();
        bakedPotatoDef.setMetadata( bakedPotatoMetadata );
        bakedPotatoDef.setStageValues( bakedPotatoStageValues );
        bakedPotatoDef.setFlowValues( bakedPotatoFlowValues );
        bakedPotatoDef.setCalcFlowValues( bakedPotatoCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        ThresholdMetadata grossBakedPotatoMetadata = new ThresholdMetadata();
        grossBakedPotatoMetadata.setNwsLid( "BakedPotato" );
        grossBakedPotatoMetadata.setThresholdSource( "NWS-NRLDB" );
        grossBakedPotatoMetadata.setThresholdSourceDescription( "NONE" );
        grossBakedPotatoMetadata.setFlowUnits( "CFS" );
        grossBakedPotatoMetadata.setCalcFlowUnits( "CFS" );
        grossBakedPotatoMetadata.setStageUnits( "MM" );

        //Stage thresholds
        ThresholdValues grossBakedPotatoStageValues = new ThresholdValues();
        grossBakedPotatoStageValues.add( "low", null );
        grossBakedPotatoStageValues.add( "bankfull", null );
        grossBakedPotatoStageValues.add( "action", null );
        grossBakedPotatoStageValues.add( "minor", 5.54d );
        grossBakedPotatoStageValues.add( "moderate", 4.0d );
        grossBakedPotatoStageValues.add( "major", 158d );
        grossBakedPotatoStageValues.add( "record", null );

        //Flow thresholds
        ThresholdValues grossBakedPotatoFlowValues = new ThresholdValues();
        grossBakedPotatoFlowValues.add( "low", null );
        grossBakedPotatoFlowValues.add( "bankfull", null );
        grossBakedPotatoFlowValues.add( "action", null );
        grossBakedPotatoFlowValues.add( "minor", 1.42d );
        grossBakedPotatoFlowValues.add( "moderate", 186.85d );
        grossBakedPotatoFlowValues.add( "major", 92d );
        grossBakedPotatoFlowValues.add( "record", 45d );

        //Calculated flow thresholds with rating curve info.
        ThresholdValues grossBakedPotatoCalcFlowValues = new ThresholdValues();
        grossBakedPotatoCalcFlowValues.add( "low", 547d );
        grossBakedPotatoCalcFlowValues.add( "bankfull", null );
        grossBakedPotatoCalcFlowValues.add( "action", null );
        grossBakedPotatoCalcFlowValues.add( "minor", null );
        grossBakedPotatoCalcFlowValues.add( "moderate", 88d );
        grossBakedPotatoCalcFlowValues.add( "major", null );
        grossBakedPotatoCalcFlowValues.add( "record", 6.84d );
        RatingCurveInfo grossBakedPotatoRCInfo = new RatingCurveInfo();
        grossBakedPotatoRCInfo.setSource( "DonkeySauce" );
        grossBakedPotatoRCInfo.setDescription( "NONE" );
        grossBakedPotatoCalcFlowValues.setRatingCurve( grossBakedPotatoRCInfo );

        //The definition
        ThresholdDefinition grossBakedPotatoDef = new ThresholdDefinition();
        grossBakedPotatoDef.setMetadata( grossBakedPotatoMetadata );
        grossBakedPotatoDef.setStageValues( grossBakedPotatoStageValues );
        grossBakedPotatoDef.setFlowValues( grossBakedPotatoFlowValues );
        grossBakedPotatoDef.setCalcFlowValues( grossBakedPotatoCalcFlowValues );


        //Put together the response.
        ThresholdResponse response = new ThresholdResponse();
        response.setThresholds(
                List.of(
                        steakDef,
                        grossSteakDef,
                        flatIronSteakDef,
                        bakedPotatoDef,
                        grossBakedPotatoDef ) );
        return response;
    }
}
