package wres.io.thresholds.wrds;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.wrds.v2.CalculatedThresholdValues;
import wres.io.thresholds.wrds.v2.OriginalThresholdValues;
import wres.io.thresholds.wrds.v2.ThresholdDefinition;
import wres.io.thresholds.wrds.v2.ThresholdExtractor;
import wres.io.thresholds.wrds.v2.ThresholdMetadata;
import wres.io.thresholds.wrds.v2.ThresholdResponse;
import wres.io.thresholds.wrds.v3.GeneralThresholdDefinition;
import wres.io.thresholds.wrds.v3.GeneralThresholdExtractor;
import wres.io.thresholds.wrds.v3.GeneralThresholdMetadata;
import wres.io.thresholds.wrds.v3.GeneralThresholdResponse;
import wres.io.thresholds.wrds.v3.GeneralThresholdValues;
import wres.io.thresholds.wrds.v3.RatingCurveInfo;
import wres.system.SystemSettings;

public class GeneralThresholdExtractorTest
{
    //The file used is created from this URL:
    //
    //https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/
    //
    //executed on 5/22/2021 in the afternoon.
    private static final URI path2 = URI.create( "testinput/thresholds/wrds/recurrence_v3.json" );

    private static final double EPSILON = 0.00001;

    private static WrdsLocation createFeature( final String featureId, final String usgsSiteCode, final String lid )
    {
        return new WrdsLocation( featureId, usgsSiteCode, lid );
    }

    private static final WrdsLocation PTSA1 = createFeature( "2323396", "02372250", "PTSA1" );
    private static final WrdsLocation MNTG1 = createFeature( "6444276", "02349605", "MNTG1" );


    private static final List<WrdsLocation> DESIRED_FEATURES = List.of(
                                                                        PTSA1,
                                                                        MNTG1 );

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
        ptsa1NWSMetadata.setNws_lid( "PTSA1" );
        ptsa1NWSMetadata.setUsgs_site_code( "02372250" );
        ptsa1NWSMetadata.setNwm_feature_id( "2323396" );
        ptsa1NWSMetadata.setThreshold_source( "NWS-CMS" );
        ptsa1NWSMetadata.setThreshold_source_description( "NONE" );
        ptsa1NWSMetadata.setFlow_units( "CFS" );
        ptsa1NWSMetadata.setCalc_flow_units( "CFS" );
        ptsa1NWSMetadata.setStage_units( "FT" );

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
        ptsa1NWS.setStage_values( ptsa1NWSOriginalValues );

        // ==== Second set of thresholds.

        //Metadata
        GeneralThresholdMetadata mntg1NWSMetadata = new GeneralThresholdMetadata();
        mntg1NWSMetadata.setNws_lid( "MNTG1" );
        mntg1NWSMetadata.setUsgs_site_code( "02349605" );
        mntg1NWSMetadata.setNwm_feature_id( "6444276" );
        mntg1NWSMetadata.setThreshold_source( "NWS-CMS" );
        mntg1NWSMetadata.setThreshold_source_description( "NONE" );
        mntg1NWSMetadata.setFlow_units( "CFS" );
        mntg1NWSMetadata.setCalc_flow_units( "CFS" );
        mntg1NWSMetadata.setStage_units( "FT" );

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
        mntg1NWS.setStage_values( mntg1NWSOriginalValues );

        // ==== Third set of thresholds.

        //Metadata
        GeneralThresholdMetadata mntg1NRLDBMetadata = new GeneralThresholdMetadata();
        mntg1NRLDBMetadata.setNws_lid( "MNTG1" );
        mntg1NRLDBMetadata.setUsgs_site_code( "02349605" );
        mntg1NRLDBMetadata.setNwm_feature_id( "6444276" );
        mntg1NRLDBMetadata.setThreshold_source( "NWS-NRLDB" );
        mntg1NRLDBMetadata.setThreshold_source_description( "NONE" );
        mntg1NRLDBMetadata.setFlow_units( "CFS" );
        mntg1NRLDBMetadata.setCalc_flow_units( "CFS" );
        mntg1NRLDBMetadata.setStage_units( "FT" );

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
        mntg1NRLDB.setStage_values( mntg1NRLDBOriginalValues );

        GeneralThresholdResponse response = new GeneralThresholdResponse();
        response.setThresholds( List.of( ptsa1NWS, mntg1NWS, mntg1NRLDB ) );

        return response;
    }

    private GeneralThresholdResponse createFunThresholdResponse()
    {

        // ==== First set of thresholds.

        //Metadata
        GeneralThresholdMetadata steakMetadata = new GeneralThresholdMetadata();
        steakMetadata.setNws_lid( "STEAK" );
        steakMetadata.setThreshold_source( "FlavorTown" );
        steakMetadata.setFlow_units( "CFS" );
        steakMetadata.setCalc_flow_units( "CFS" );
        steakMetadata.setStage_units( "MM" );

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
        steakCalcFlowValues.setRating_curve( rcInfo );

        //The definition
        GeneralThresholdDefinition steakDef = new GeneralThresholdDefinition();
        steakDef.setMetadata( steakMetadata );
        steakDef.setStage_values( steakStageValues );
        steakDef.setFlow_values( steakFlowValues );
        steakDef.setCalc_flow_values( steakCalcFlowValues );

        // ==== Second set of thresholds

        //Metadata
        GeneralThresholdMetadata grossSteakMetadata = new GeneralThresholdMetadata();
        grossSteakMetadata.setNws_lid( "STEAK" );
        grossSteakMetadata.setThreshold_source( "NWS-NRLDB" );
        grossSteakMetadata.setFlow_units( "CFS" );
        grossSteakMetadata.setCalc_flow_units( "CFS" );
        grossSteakMetadata.setStage_units( "MM" );

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
        grossSteakCalcFlowValues.setRating_curve( grossRCInfo );

        //The definition; no stage or flow.
        GeneralThresholdDefinition grossSteakDef = new GeneralThresholdDefinition();
        grossSteakDef.setMetadata( grossSteakMetadata );
        grossSteakDef.setCalc_flow_values( grossSteakCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        GeneralThresholdMetadata flatIronSteakMetadata = new GeneralThresholdMetadata();
        flatIronSteakMetadata.setNws_lid( "STEAK" );
        flatIronSteakMetadata.setThreshold_source( "FlatIron" );
        flatIronSteakMetadata.setFlow_units( "CFS" );
        flatIronSteakMetadata.setCalc_flow_units( "CFS" );
        flatIronSteakMetadata.setStage_units( "MM" );

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
        flatIronSteakCalcFlowValues.setRating_curve( flatIronRCInfo );

        //The definition
        GeneralThresholdDefinition flatIronSteakDef = new GeneralThresholdDefinition();
        flatIronSteakDef.setMetadata( flatIronSteakMetadata );
        flatIronSteakDef.setStage_values( flatIronSteakStageValues );
        flatIronSteakDef.setFlow_values( flatIronSteakFlowValues );
        flatIronSteakDef.setCalc_flow_values( flatIronSteakCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        GeneralThresholdMetadata bakedPotatoMetadata = new GeneralThresholdMetadata();
        bakedPotatoMetadata.setNws_lid( "BakedPotato" );
        bakedPotatoMetadata.setThreshold_source( "FlavorTown" );
        bakedPotatoMetadata.setFlow_units( "CFS" );
        bakedPotatoMetadata.setCalc_flow_units( "CFS" );
        bakedPotatoMetadata.setStage_units( "MM" );

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
        bakedPotatoCalcFlowValues.setRating_curve( bakedPotatoRCInfo );

        //The definition
        GeneralThresholdDefinition bakedPotatoDef = new GeneralThresholdDefinition();
        bakedPotatoDef.setMetadata( bakedPotatoMetadata );
        bakedPotatoDef.setStage_values( bakedPotatoStageValues );
        bakedPotatoDef.setFlow_values( bakedPotatoFlowValues );
        bakedPotatoDef.setCalc_flow_values( bakedPotatoCalcFlowValues );

        // ==== Another set of thresholds.

        //Metadata
        GeneralThresholdMetadata grossBakedPotatoMetadata = new GeneralThresholdMetadata();
        grossBakedPotatoMetadata.setNws_lid( "BakedPotato" );
        grossBakedPotatoMetadata.setThreshold_source( "NWS-NRLDB" );
        grossBakedPotatoMetadata.setThreshold_source_description( "NONE" );
        grossBakedPotatoMetadata.setFlow_units( "CFS" );
        grossBakedPotatoMetadata.setCalc_flow_units( "CFS" );
        grossBakedPotatoMetadata.setStage_units( "MM" );

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
        grossBakedPotatoCalcFlowValues.setRating_curve( grossBakedPotatoRCInfo );

        //The definition
        GeneralThresholdDefinition grossBakedPotatoDef = new GeneralThresholdDefinition();
        grossBakedPotatoDef.setMetadata( grossBakedPotatoMetadata );
        grossBakedPotatoDef.setStage_values( grossBakedPotatoStageValues );
        grossBakedPotatoDef.setFlow_values( grossBakedPotatoFlowValues );
        grossBakedPotatoDef.setCalc_flow_values( grossBakedPotatoCalcFlowValues );


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

    private SystemSettings systemSettings;

    @Before
    public void runBeforeEachTest()
    {
        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.systemSettings = SystemSettings.withDefaults();
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
                                                               .operatesBy( ThresholdConstants.Operator.GREATER )
                                                               .onSide( ThresholdConstants.ThresholdDataType.LEFT );
        Map<WrdsLocation, Set<ThresholdOuter>> normalExtraction = extractor.extract();

        Assert.assertFalse( normalExtraction.containsKey( PTSA1 ) );
        Assert.assertTrue( normalExtraction.containsKey( MNTG1 ) );

        Map<String, Double> thresholdValues = new HashMap<>();
        thresholdValues.put( "record", 34.11 );
        thresholdValues.put( "bankfull", 11.0 );
        thresholdValues.put( "action", 11.0 );
        thresholdValues.put( "major", 31.0 );
        thresholdValues.put( "minor", 20.0 );
        thresholdValues.put( "moderate", 28.0 );

        Set<ThresholdOuter> normalOuterThresholds = normalExtraction.get( MNTG1 );

        Assert.assertEquals( 6, normalOuterThresholds.size() );

        for ( ThresholdOuter outerThreshold : normalOuterThresholds )
        {
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT, outerThreshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );
            Assert.assertEquals(
                                 thresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }


        //This a test of flow thresholds.  See the funThresholdConfig for more information.

        extractor = new GeneralThresholdExtractor( funResponse ).readFlow()
                                                                .convertTo( unitMapper )
                                                                .from( "FlavorTown" )
                                                                .ratingFrom( "DonkeySauce" )
                                                                .operatesBy( ThresholdConstants.Operator.GREATER )
                                                                .onSide( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT );
        Map<WrdsLocation, Set<ThresholdOuter>> funExtraction = extractor.extract();

        Assert.assertTrue( funExtraction.containsKey( STEAK ) );
        Assert.assertTrue( funExtraction.containsKey( BAKED_POTATO ) );

        Assert.assertEquals( 7, funExtraction.get( STEAK ).size() );
        Assert.assertEquals( 6, funExtraction.get( BAKED_POTATO ).size() );

        thresholdValues = new HashMap<>();

        thresholdValues.put( "bankfull", -14.586 );
        thresholdValues.put( "low", -5.7 );
        thresholdValues.put( "action", -13.5 );
        thresholdValues.put( "minor", -189.42 );
        thresholdValues.put( "moderate", -868.5 );
        thresholdValues.put( "major", -90144.2 );
        thresholdValues.put( "record", -4846844.5484 );

        for ( ThresholdOuter outerThreshold : funExtraction.get( STEAK ) )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( thresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 thresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        thresholdValues = new HashMap<>();

        thresholdValues.put( "low", 54.7 );
        thresholdValues.put( "bankfull", 1458.6 );
        thresholdValues.put( "minor", 18942.0 );
        thresholdValues.put( "moderate", 88.5 );
        thresholdValues.put( "major", 901.2 );
        thresholdValues.put( "record", 6844.84 );

        for ( ThresholdOuter outerThreshold : funExtraction.get( BAKED_POTATO ) )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( thresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 thresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        //This is a test of stage thresholds; see alternativeThresholdConfig.
        extractor = new GeneralThresholdExtractor( normalResponse ).readStage()
                                                                   .convertTo( unitMapper )
                                                                   .from( "NWS-CMS" )
                                                                   .ratingFrom( null )
                                                                   .operatesBy( ThresholdConstants.Operator.LESS )
                                                                   .onSide( ThresholdConstants.ThresholdDataType.LEFT );
        Map<WrdsLocation, Set<ThresholdOuter>> alternativeNormalExtraction = extractor.extract();

        Assert.assertTrue( alternativeNormalExtraction.containsKey( PTSA1 ) );
        Assert.assertTrue( alternativeNormalExtraction.containsKey( MNTG1 ) );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "action", 11.0 );
        thresholdValues.put( "major", 31.0 );
        thresholdValues.put( "minor", 20.0 );
        thresholdValues.put( "moderate", 28.0 );

        Assert.assertEquals( 4, alternativeNormalExtraction.get( MNTG1 ).size() );

        for ( ThresholdOuter outerThreshold : alternativeNormalExtraction.get( MNTG1 ) )
        {
            Assert.assertEquals( Operator.LESS, outerThreshold.getOperator() );
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT, outerThreshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );
            Assert.assertEquals(
                                 thresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        Assert.assertEquals( 4, alternativeNormalExtraction.get( PTSA1 ).size() );

        thresholdValues = new HashMap<>();
        thresholdValues.put( "action", 0.0 );
        thresholdValues.put( "major", 0.0 );
        thresholdValues.put( "minor", 0.0 );
        thresholdValues.put( "moderate", 0.0 );

        for ( ThresholdOuter outerThreshold : alternativeNormalExtraction.get( PTSA1 ) )
        {
            Assert.assertEquals( Operator.LESS, outerThreshold.getOperator() );
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT, outerThreshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );
            Assert.assertEquals(
                                 thresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        //This is a test of stage thresholds; see the normalThresholdConfig.
        extractor = new GeneralThresholdExtractor( funResponse ).readStage()
                                                                .convertTo( unitMapper )
                                                                .from( "NWS-NRLDB" )
                                                                .ratingFrom( null )
                                                                .operatesBy( ThresholdConstants.Operator.GREATER )
                                                                .onSide( ThresholdConstants.ThresholdDataType.LEFT );
        Map<WrdsLocation, Set<ThresholdOuter>> normalButFunExtraction = extractor.extract();

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

        for ( ThresholdOuter outerThreshold : normalButFunExtraction.get( BAKED_POTATO ) )
        {
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT, outerThreshold.getDataType() );

            Assert.assertTrue( thresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );
            Assert.assertEquals(
                                 thresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }
    }

}
