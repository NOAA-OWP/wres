package wres.io.reading.wrds.thresholds.v3;

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

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.pools.MeasurementUnit;
import wres.io.reading.wrds.geography.WrdsLocation;
import wres.datamodel.units.UnitMapper;
import wres.io.reading.wrds.thresholds.NoThresholdsFoundException;
import wres.io.reading.wrds.thresholds.v2.ThresholdExtractor;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link ThresholdExtractor}.  This has been modified to include a test that was
 * inappropriately located within another test class.  So, if it looks like a hodgepodge, that's
 * because it reflects two tests in two different files combined into one.  I have not refactored
 * it further. TODO: reduce the number of assertions - assert against complete thresholds, not individual parts
 * @author Hank Herr
 * @author James Brown
 */

class GeneralThresholdExtractorTest
{
    //The file used is created from this URL:
    //
    //https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/
    //
    //executed on 5/22/2021 in the afternoon.

    private static final double EPSILON = 0.00001;

    private static WrdsLocation createFeature( final String featureId, final String usgsSiteCode, final String lid )
    {
        return new WrdsLocation( featureId, usgsSiteCode, lid );
    }

    private static final WrdsLocation PTSA1 = createFeature( "2323396", "02372250", "PTSA1" );
    private static final WrdsLocation MNTG1 = createFeature( "6444276", "02349605", "MNTG1" );

    private static final WrdsLocation STEAK = createFeature( null, null, "STEAK" );
    private static final WrdsLocation BAKED_POTATO = createFeature( null, null, "BakedPotato" );

    private UnitMapper unitMapper;
    private GeneralThresholdResponse normalResponse = null;
    private GeneralThresholdResponse funResponse = null;

    private static final MeasurementUnit units = MeasurementUnit.of( "CMS" );


    private GeneralThresholdResponse createNormalThresholdResponse()
    {

        // ==== First set of thresholds.

        //Metadata
        GeneralThresholdMetadata ptsa1NWSMetadata = new GeneralThresholdMetadata();
        ptsa1NWSMetadata.setNwsLid( "PTSA1" );
        ptsa1NWSMetadata.setUsgsSideCode( "02372250" );
        ptsa1NWSMetadata.setNwmFeatureId( "2323396" );
        ptsa1NWSMetadata.setThresholdSource( "NWS-CMS" );
        ptsa1NWSMetadata.setThresholdSourceDescription( "NONE" );
        ptsa1NWSMetadata.setFlowUnits( "CFS" );
        ptsa1NWSMetadata.setCalcFlowUnits( "CFS" );
        ptsa1NWSMetadata.setStageUnits( "FT" );

        //Stage thresholds
        GeneralThresholdValues ptsa1NWSOriginalValues = new GeneralThresholdValues();
        ptsa1NWSOriginalValues.add( "low", null );
        ptsa1NWSOriginalValues.add( "bankfull", null );
        ptsa1NWSOriginalValues.add( "action", 0.0d );
        ptsa1NWSOriginalValues.add( "minor", 0.0d );
        ptsa1NWSOriginalValues.add( "moderate", 0.0d );
        ptsa1NWSOriginalValues.add( "major", 0.0d );
        ptsa1NWSOriginalValues.add( "record", null );

        //No flow or calculated.
        GeneralThresholdDefinition ptsa1NWS = new GeneralThresholdDefinition();
        ptsa1NWS.setMetadata( ptsa1NWSMetadata );
        ptsa1NWS.setStageValues( ptsa1NWSOriginalValues );

        // ==== Second set of thresholds.

        //Metadata
        GeneralThresholdMetadata mntg1NWSMetadata = new GeneralThresholdMetadata();
        mntg1NWSMetadata.setNwsLid( "MNTG1" );
        mntg1NWSMetadata.setUsgsSideCode( "02349605" );
        mntg1NWSMetadata.setNwmFeatureId( "6444276" );
        mntg1NWSMetadata.setThresholdSource( "NWS-CMS" );
        mntg1NWSMetadata.setThresholdSourceDescription( "NONE" );
        mntg1NWSMetadata.setFlowUnits( "CFS" );
        mntg1NWSMetadata.setCalcFlowUnits( "CFS" );
        mntg1NWSMetadata.setStageUnits( "FT" );

        //Stage thresholds
        GeneralThresholdValues mntg1NWSOriginalValues = new GeneralThresholdValues();
        mntg1NWSOriginalValues.add( "low", null );
        mntg1NWSOriginalValues.add( "bankfull", null );
        mntg1NWSOriginalValues.add( "action", 11.0d );
        mntg1NWSOriginalValues.add( "minor", 20.0d );
        mntg1NWSOriginalValues.add( "moderate", 28.0d );
        mntg1NWSOriginalValues.add( "major", 31.0d );
        mntg1NWSOriginalValues.add( "record", null );

        //No flow or calculated.
        GeneralThresholdDefinition mntg1NWS = new GeneralThresholdDefinition();
        mntg1NWS.setMetadata( mntg1NWSMetadata );
        mntg1NWS.setStageValues( mntg1NWSOriginalValues );

        // ==== Third set of thresholds.

        //Metadata
        GeneralThresholdMetadata mntg1NRLDBMetadata = new GeneralThresholdMetadata();
        mntg1NRLDBMetadata.setNwsLid( "MNTG1" );
        mntg1NRLDBMetadata.setUsgsSideCode( "02349605" );
        mntg1NRLDBMetadata.setNwmFeatureId( "6444276" );
        mntg1NRLDBMetadata.setThresholdSource( "NWS-NRLDB" );
        mntg1NRLDBMetadata.setThresholdSourceDescription( "NONE" );
        mntg1NRLDBMetadata.setFlowUnits( "CFS" );
        mntg1NRLDBMetadata.setCalcFlowUnits( "CFS" );
        mntg1NRLDBMetadata.setStageUnits( "FT" );

        //Stage thresholds
        GeneralThresholdValues mntg1NRLDBOriginalValues = new GeneralThresholdValues();
        mntg1NRLDBOriginalValues.add( "low", null );
        mntg1NRLDBOriginalValues.add( "bankfull", 11.0d );
        mntg1NRLDBOriginalValues.add( "action", 11.0d );
        mntg1NRLDBOriginalValues.add( "minor", 20.0d );
        mntg1NRLDBOriginalValues.add( "moderate", 28.0d );
        mntg1NRLDBOriginalValues.add( "major", 31.0d );
        mntg1NRLDBOriginalValues.add( "record", 34.11d );

        //No flow or calculated.
        GeneralThresholdDefinition mntg1NRLDB = new GeneralThresholdDefinition();
        mntg1NRLDB.setMetadata( mntg1NRLDBMetadata );
        mntg1NRLDB.setStageValues( mntg1NRLDBOriginalValues );

        GeneralThresholdResponse response = new GeneralThresholdResponse();
        response.setThresholds( List.of( ptsa1NWS, mntg1NWS, mntg1NRLDB ) );

        return response;
    }

    private GeneralThresholdResponse createFunThresholdResponse()
    {

        // ==== First set of thresholds.

        //Metadata
        GeneralThresholdMetadata steakMetadata = new GeneralThresholdMetadata();
        steakMetadata.setNwsLid( "STEAK" );
        steakMetadata.setThresholdSource( "FlavorTown" );
        steakMetadata.setFlowUnits( "CFS" );
        steakMetadata.setCalcFlowUnits( "CFS" );
        steakMetadata.setStageUnits( "MM" );

        //Stage thresholds
        GeneralThresholdValues steakStageValues = new GeneralThresholdValues();
        steakStageValues.add( "low", 99.586168d );
        steakStageValues.add( "bankfull", 120.50d );
        steakStageValues.add( "action", 180.58d );
        steakStageValues.add( "minor", 350.5419d );
        steakStageValues.add( "moderate", 420.0d );
        steakStageValues.add( "major", 8054.54d );
        steakStageValues.add( "record", 9999.594d );

        //Flow thresholds
        GeneralThresholdValues steakFlowValues = new GeneralThresholdValues();
        steakFlowValues.add( "low", 5.7d );
        steakFlowValues.add( "bankfull", 14.586d );
        steakFlowValues.add( "action", 13.5d );
        steakFlowValues.add( "minor", 189.42d );
        steakFlowValues.add( "moderate", 868.5d );
        steakFlowValues.add( "major", 90144.2d );
        steakFlowValues.add( "record", 4846844.5484d );

        //Calculated flow thresholds with rating curve info.
        GeneralThresholdValues steakCalcFlowValues = new GeneralThresholdValues();
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
        GeneralThresholdDefinition steakDef = new GeneralThresholdDefinition();
        steakDef.setMetadata( steakMetadata );
        steakDef.setStageValues( steakStageValues );
        steakDef.setFlowValues( steakFlowValues );
        steakDef.setCalcFlowValues( steakCalcFlowValues );

        // ==== Second set of thresholds

        //Metadata
        GeneralThresholdMetadata grossSteakMetadata = new GeneralThresholdMetadata();
        grossSteakMetadata.setNwsLid( "STEAK" );
        grossSteakMetadata.setThresholdSource( "NWS-NRLDB" );
        grossSteakMetadata.setFlowUnits( "CFS" );
        grossSteakMetadata.setCalcFlowUnits( "CFS" );
        grossSteakMetadata.setStageUnits( "MM" );

        //Calculated flow thresholds
        GeneralThresholdValues grossSteakCalcFlowValues = new GeneralThresholdValues();
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
        GeneralThresholdDefinition grossSteakDef = new GeneralThresholdDefinition();
        grossSteakDef.setMetadata( grossSteakMetadata );
        grossSteakDef.setCalcFlowValues( grossSteakCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        GeneralThresholdMetadata flatIronSteakMetadata = new GeneralThresholdMetadata();
        flatIronSteakMetadata.setNwsLid( "STEAK" );
        flatIronSteakMetadata.setThresholdSource( "FlatIron" );
        flatIronSteakMetadata.setFlowUnits( "CFS" );
        flatIronSteakMetadata.setCalcFlowUnits( "CFS" );
        flatIronSteakMetadata.setStageUnits( "MM" );

        //Stage thresholds
        GeneralThresholdValues flatIronSteakStageValues = new GeneralThresholdValues();
        flatIronSteakStageValues.add( "low", 99.586168d );
        flatIronSteakStageValues.add( "bankfull", 120.50d );
        flatIronSteakStageValues.add( "action", 180.58d );
        flatIronSteakStageValues.add( "minor", 350.5419d );
        flatIronSteakStageValues.add( "moderate", 420.0d );
        flatIronSteakStageValues.add( "major", 8054.54d );
        flatIronSteakStageValues.add( "record", 9999.594d );

        //Flow thresholds
        GeneralThresholdValues flatIronSteakFlowValues = new GeneralThresholdValues();
        flatIronSteakFlowValues.add( "low", 5.7d );
        flatIronSteakFlowValues.add( "bankfull", 14.586d );
        flatIronSteakFlowValues.add( "action", 13.5d );
        flatIronSteakFlowValues.add( "minor", 189.42d );
        flatIronSteakFlowValues.add( "moderate", 868.5d );
        flatIronSteakFlowValues.add( "major", 90144.2d );
        flatIronSteakFlowValues.add( "record", 4846844.5484d );

        //Calculated flow thresholds with rating curve info.
        GeneralThresholdValues flatIronSteakCalcFlowValues = new GeneralThresholdValues();
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
        GeneralThresholdDefinition flatIronSteakDef = new GeneralThresholdDefinition();
        flatIronSteakDef.setMetadata( flatIronSteakMetadata );
        flatIronSteakDef.setStageValues( flatIronSteakStageValues );
        flatIronSteakDef.setFlowValues( flatIronSteakFlowValues );
        flatIronSteakDef.setCalcFlowValues( flatIronSteakCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        GeneralThresholdMetadata bakedPotatoMetadata = new GeneralThresholdMetadata();
        bakedPotatoMetadata.setNwsLid( "BakedPotato" );
        bakedPotatoMetadata.setThresholdSource( "FlavorTown" );
        bakedPotatoMetadata.setFlowUnits( "CFS" );
        bakedPotatoMetadata.setCalcFlowUnits( "CFS" );
        bakedPotatoMetadata.setStageUnits( "MM" );

        //Stage thresholds
        GeneralThresholdValues bakedPotatoStageValues = new GeneralThresholdValues();
        bakedPotatoStageValues.add( "low", 9.586168d );
        bakedPotatoStageValues.add( "bankfull", null );
        bakedPotatoStageValues.add( "action", null );
        bakedPotatoStageValues.add( "minor", 50.54d );
        bakedPotatoStageValues.add( "moderate", 42.0d );
        bakedPotatoStageValues.add( "major", null );
        bakedPotatoStageValues.add( "record", null );

        //Flow thresholds
        GeneralThresholdValues bakedPotatoFlowValues = new GeneralThresholdValues();
        bakedPotatoFlowValues.add( "low", 57d );
        bakedPotatoFlowValues.add( "bankfull", 1458.6d );
        bakedPotatoFlowValues.add( "action", null );
        bakedPotatoFlowValues.add( "minor", 142d );
        bakedPotatoFlowValues.add( "moderate", 86.85d );
        bakedPotatoFlowValues.add( "major", 9.2d );
        bakedPotatoFlowValues.add( "record", 4.35d );

        //Calculated flow thresholds with rating curve info.
        GeneralThresholdValues bakedPotatoCalcFlowValues = new GeneralThresholdValues();
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
        GeneralThresholdDefinition bakedPotatoDef = new GeneralThresholdDefinition();
        bakedPotatoDef.setMetadata( bakedPotatoMetadata );
        bakedPotatoDef.setStageValues( bakedPotatoStageValues );
        bakedPotatoDef.setFlowValues( bakedPotatoFlowValues );
        bakedPotatoDef.setCalcFlowValues( bakedPotatoCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        GeneralThresholdMetadata grossBakedPotatoMetadata = new GeneralThresholdMetadata();
        grossBakedPotatoMetadata.setNwsLid( "BakedPotato" );
        grossBakedPotatoMetadata.setThresholdSource( "NWS-NRLDB" );
        grossBakedPotatoMetadata.setThresholdSourceDescription( "NONE" );
        grossBakedPotatoMetadata.setFlowUnits( "CFS" );
        grossBakedPotatoMetadata.setCalcFlowUnits( "CFS" );
        grossBakedPotatoMetadata.setStageUnits( "MM" );

        //Stage thresholds
        GeneralThresholdValues grossBakedPotatoStageValues = new GeneralThresholdValues();
        grossBakedPotatoStageValues.add( "low", null );
        grossBakedPotatoStageValues.add( "bankfull", null );
        grossBakedPotatoStageValues.add( "action", null );
        grossBakedPotatoStageValues.add( "minor", 5.54d );
        grossBakedPotatoStageValues.add( "moderate", 4.0d );
        grossBakedPotatoStageValues.add( "major", 158d );
        grossBakedPotatoStageValues.add( "record", null );

        //Flow thresholds
        GeneralThresholdValues grossBakedPotatoFlowValues = new GeneralThresholdValues();
        grossBakedPotatoFlowValues.add( "low", null );
        grossBakedPotatoFlowValues.add( "bankfull", null );
        grossBakedPotatoFlowValues.add( "action", null );
        grossBakedPotatoFlowValues.add( "minor", 1.42d );
        grossBakedPotatoFlowValues.add( "moderate", 186.85d );
        grossBakedPotatoFlowValues.add( "major", 92d );
        grossBakedPotatoFlowValues.add( "record", 45d );

        //Calculated flow thresholds with rating curve info.
        GeneralThresholdValues grossBakedPotatoCalcFlowValues = new GeneralThresholdValues();
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
        GeneralThresholdDefinition grossBakedPotatoDef = new GeneralThresholdDefinition();
        grossBakedPotatoDef.setMetadata( grossBakedPotatoMetadata );
        grossBakedPotatoDef.setStageValues( grossBakedPotatoStageValues );
        grossBakedPotatoDef.setFlowValues( grossBakedPotatoFlowValues );
        grossBakedPotatoDef.setCalcFlowValues( grossBakedPotatoCalcFlowValues );


        //Put together the response.
        GeneralThresholdResponse response = new GeneralThresholdResponse();
        response.setThresholds(
                List.of(
                        steakDef,
                        grossSteakDef,
                        flatIronSteakDef,
                        bakedPotatoDef,
                        grossBakedPotatoDef ) );
        return response;
    }

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
        GeneralThresholdValues stageValues = new GeneralThresholdValues();
        stageValues.add( "low", 1.5d );
        stageValues.add( "bankfull", 28.0d );
        stageValues.add( "action", 26.0d );
        stageValues.add( "minor", 28.0d );
        stageValues.add( "moderate", 31.0d );
        stageValues.add( "major", 43.0d );
        stageValues.add( "record", 50.83d );
        aThreshold.setStageValues( stageValues );

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
        calcFlowValues.setRatingCurve( rcInfo );
        aThreshold.setCalcFlowValues( calcFlowValues );

        thresholds.add( aThreshold );

        thresholdResponse.setThresholds( thresholds );

        this.extractor = new GeneralThresholdExtractor( thresholdResponse );

        //For the tests that used to be external to this test.
        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.normalResponse = this.createNormalThresholdResponse();
        this.funResponse = this.createFunThresholdResponse();
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
        assertEquals( 7, extractedStageThresholds.get( extractedStageThresholds.keySet()
                                                                               .iterator()
                                                                               .next() )
                                                 .size() );
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

    /*
     * This test used to be badly located in another file.
     */
    @Test
    void testExtract()
    {
        GeneralThresholdExtractor extractor =
                new GeneralThresholdExtractor( normalResponse ).readStage()
                                                               .convertTo( unitMapper )
                                                               .from( "NWS-NRLDB" )
                                                               .ratingFrom( null )
                                                               .operatesBy( ThresholdOperator.GREATER )
                                                               .onSide( ThresholdOrientation.LEFT );
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
            assertEquals( thresholdValues.get( threshold.getName() ),
                          threshold.getLeftThresholdValue().getValue(),
                          EPSILON );
        }


        //This a test of flow thresholds.  See the funThresholdConfig for more information.
        extractor = new GeneralThresholdExtractor( funResponse ).readFlow()
                                                                .convertTo( unitMapper )
                                                                .from( "FlavorTown" )
                                                                .ratingFrom( "DonkeySauce" )
                                                                .operatesBy( ThresholdOperator.GREATER )
                                                                .onSide( ThresholdOrientation.LEFT_AND_ANY_RIGHT );
        Map<WrdsLocation, Set<Threshold>> funExtraction = extractor.extract();

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
                          threshold.getLeftThresholdValue().getValue(),
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
                          threshold.getLeftThresholdValue().getValue(),
                          EPSILON );
        }

        //This is a test of stage thresholds; see alternativeThresholdConfig.
        extractor = new GeneralThresholdExtractor( normalResponse ).readStage()
                                                                   .convertTo( unitMapper )
                                                                   .from( "NWS-CMS" )
                                                                   .ratingFrom( null )
                                                                   .operatesBy( ThresholdOperator.LESS )
                                                                   .onSide( ThresholdOrientation.LEFT );
        Map<WrdsLocation, Set<Threshold>> alternativeNormalExtraction = extractor.extract();

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
                    threshold.getLeftThresholdValue().getValue(),
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
                    threshold.getLeftThresholdValue().getValue(),
                    EPSILON );
        }

        //This is a test of stage thresholds; see the normalThresholdConfig.
        extractor = new GeneralThresholdExtractor( funResponse ).readStage()
                                                                .convertTo( unitMapper )
                                                                .from( "NWS-NRLDB" )
                                                                .ratingFrom( null )
                                                                .operatesBy( ThresholdOperator.GREATER )
                                                                .onSide( ThresholdOrientation.LEFT );
        Map<WrdsLocation, Set<Threshold>> normalButFunExtraction = extractor.extract();

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
                    threshold.getLeftThresholdValue().getValue(),
                    EPSILON );
        }
    }

}