package wres.io.reading.wrds.thresholds;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.pools.MeasurementUnit;
import wres.io.reading.wrds.geography.WrdsLocation;
import wres.datamodel.units.UnitMapper;
import wres.io.reading.wrds.thresholds.v3.GeneralThresholdDefinition;
import wres.io.reading.wrds.thresholds.v3.GeneralThresholdExtractor;
import wres.io.reading.wrds.thresholds.v3.GeneralThresholdMetadata;
import wres.io.reading.wrds.thresholds.v3.GeneralThresholdResponse;
import wres.io.reading.wrds.thresholds.v3.GeneralThresholdValues;
import wres.io.reading.wrds.thresholds.v3.RatingCurveInfo;
import wres.statistics.generated.Threshold;

public class GeneralThresholdExtractorTest
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

    @Before
    public void runBeforeEachTest()
    {
        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.normalResponse = this.createNormalThresholdResponse();
        this.funResponse = this.createFunThresholdResponse();
        Mockito.when( this.unitMapper.getUnitMapper( "FT" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "CFS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "MM" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( units.toString() );
    }

    @Test
    public void testExtract()
    {
        GeneralThresholdExtractor extractor =
                new GeneralThresholdExtractor( normalResponse ).readStage()
                                                               .convertTo( unitMapper )
                                                               .from( "NWS-NRLDB" )
                                                               .ratingFrom( null )
                                                               .operatesBy( ThresholdOperator.GREATER )
                                                               .onSide( ThresholdOrientation.LEFT );
        Map<WrdsLocation, Set<Threshold>> normalExtraction = extractor.extract();

        Assert.assertFalse( normalExtraction.containsKey( PTSA1 ) );
        Assert.assertTrue( normalExtraction.containsKey( MNTG1 ) );

        Map<String, Double> thresholdValues = new HashMap<>();
        thresholdValues.put( "record", 34.11 );
        thresholdValues.put( "bankfull", 11.0 );
        thresholdValues.put( "action", 11.0 );
        thresholdValues.put( "major", 31.0 );
        thresholdValues.put( "minor", 20.0 );
        thresholdValues.put( "moderate", 28.0 );

        Set<Threshold> normalOuterThresholds = normalExtraction.get( MNTG1 );

        Assert.assertEquals( 6, normalOuterThresholds.size() );

        for ( Threshold threshold : normalOuterThresholds )
        {
            Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            Assert.assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            Assert.assertEquals( thresholdValues.get( threshold.getName() ),
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

        Assert.assertTrue( funExtraction.containsKey( STEAK ) );
        Assert.assertTrue( funExtraction.containsKey( BAKED_POTATO ) );

        Assert.assertEquals( 14, funExtraction.get( STEAK ).size() );
        Assert.assertEquals( 11, funExtraction.get( BAKED_POTATO ).size() );

        thresholdValues = new HashMap<>();

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
            Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 threshold.getDataType() );
            Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            Assert.assertTrue( thresholdValues.containsKey( threshold.getName() ) );

            Assert.assertEquals( thresholdValues.get( threshold.getName() ),
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
            Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 threshold.getDataType() );
            Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            Assert.assertTrue( thresholdValues.containsKey( threshold.getName() ) );

            Assert.assertEquals( thresholdValues.get( threshold.getName() ),
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

        Assert.assertTrue( alternativeNormalExtraction.containsKey( PTSA1 ) );
        Assert.assertTrue( alternativeNormalExtraction.containsKey( MNTG1 ) );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "action", 11.0 );
        thresholdValues.put( "major", 31.0 );
        thresholdValues.put( "minor", 20.0 );
        thresholdValues.put( "moderate", 28.0 );

        Assert.assertEquals( 4, alternativeNormalExtraction.get( MNTG1 ).size() );

        for ( Threshold threshold : alternativeNormalExtraction.get( MNTG1 ) )
        {
            Assert.assertEquals( Threshold.ThresholdOperator.LESS, threshold.getOperator() );
            Assert.assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            Assert.assertEquals( thresholdValues.get( threshold.getName() ),
                                 threshold.getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        Assert.assertEquals( 4, alternativeNormalExtraction.get( PTSA1 ).size() );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "action", 0.0 );
        thresholdValues.put( "major", 0.0 );
        thresholdValues.put( "minor", 0.0 );
        thresholdValues.put( "moderate", 0.0 );

        for ( Threshold threshold : alternativeNormalExtraction.get( PTSA1 ) )
        {
            Assert.assertEquals( Threshold.ThresholdOperator.LESS, threshold.getOperator() );
            Assert.assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            Assert.assertEquals( thresholdValues.get( threshold.getName() ),
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

        Assert.assertTrue( normalButFunExtraction.containsKey( STEAK ) );
        Assert.assertTrue( normalButFunExtraction.containsKey( BAKED_POTATO ) );

        //Since calculated stages aren't used, and STEAK includes no regular stage thresholds 
        //these counts and expected resultswere updated for API 3.0.
        Assert.assertEquals( 0, normalButFunExtraction.get( STEAK ).size() );
        Assert.assertEquals( 3, normalButFunExtraction.get( BAKED_POTATO ).size() );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "minor", 5.54 );
        thresholdValues.put( "moderate", 4.0 );
        thresholdValues.put( "major", 158.0 );

        for ( Threshold threshold : normalButFunExtraction.get( BAKED_POTATO ) )
        {
            Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
            Assert.assertEquals( Threshold.ThresholdDataType.LEFT, threshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( threshold.getName() ) );
            Assert.assertEquals( thresholdValues.get( threshold.getName() ),
                                 threshold.getLeftThresholdValue().getValue(),
                                 EPSILON );
        }
    }

}
