package wres.io.thresholds.wrds;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import wres.config.generated.*;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.wrds.response.*;
import wres.system.SystemSettings;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class GeneralWRDSReaderTest 
{
    //The file used is created from this URL:
    //
    //https://***REMOVED***.***REMOVED***.***REMOVED***/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/
    //
    //executed on 5/20/2021 at 10:15am.
    private static final URI path = URI.create( "testinput/thresholds/wrds/thresholds_v3.json" );
    
    //The file used is created from this URL:
    //
    //https://***REMOVED***.***REMOVED***.***REMOVED***/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/
    //
    //executed on 5/22/2021 in the afternoon.
    private static final URI path2 = URI.create( "testinput/thresholds/wrds/recurrence_v3.json" );
    
    private static final double EPSILON = 0.00001;

    private static WrdsLocation createFeature(final String featureId, final String usgsSiteCode, final String lid )
    {
        return new WrdsLocation(featureId, usgsSiteCode, lid);
    }

    private static final WrdsLocation PTSA1 = GeneralWRDSReaderTest.createFeature("2323396", "02372250","PTSA1");
    private static final WrdsLocation MNTG1 = GeneralWRDSReaderTest.createFeature("6444276", "02349605", "MNTG1");
    private static final WrdsLocation BLOF1 = GeneralWRDSReaderTest.createFeature("2297254", "02358700", "BLOF1");
    private static final WrdsLocation CEDG1 = GeneralWRDSReaderTest.createFeature("2310009", "02343940", "CEDG1");
    private static final WrdsLocation SMAF1 = GeneralWRDSReaderTest.createFeature("2298964", "02359170", "SMAF1");
    private static final WrdsLocation CHAF1 = GeneralWRDSReaderTest.createFeature("2293124", "02358000", "CHAF1");
    private static final WrdsLocation OKFG1 = GeneralWRDSReaderTest.createFeature("6447636", "02350512","OKFG1");
    private static final WrdsLocation TLPT2 = GeneralWRDSReaderTest.createFeature("13525368", "07311630", "TLPT2");
    private static final WrdsLocation NUTF1 = GeneralWRDSReaderTest.createFeature(null, null, "NUTF1");
    private static final WrdsLocation CDRA1 = GeneralWRDSReaderTest.createFeature(null, null, "CDRA1");
    private static final WrdsLocation MUCG1 = GeneralWRDSReaderTest.createFeature(null, null, "MUCG1");
    private static final WrdsLocation PRSG1 = GeneralWRDSReaderTest.createFeature(null, null, "PRSG1");
    private static final WrdsLocation LSNO2 = GeneralWRDSReaderTest.createFeature(null, null, "LSNO2");
    private static final WrdsLocation HDGA4 = GeneralWRDSReaderTest.createFeature(null, null, "HDGA4");
    private static final WrdsLocation FAKE3 = GeneralWRDSReaderTest.createFeature(null, null, "FAKE3");
    private static final WrdsLocation CNMP1 = GeneralWRDSReaderTest.createFeature(null, null, "CNMP1");
    private static final WrdsLocation WLLM2 = GeneralWRDSReaderTest.createFeature(null, null, "WLLM2");
    private static final WrdsLocation RCJD2 = GeneralWRDSReaderTest.createFeature(null, null, "RCJD2");
    private static final WrdsLocation MUSM5 = GeneralWRDSReaderTest.createFeature(null, null, "MUSM5");
    private static final WrdsLocation DUMM5 = GeneralWRDSReaderTest.createFeature(null, null, "DUMM5");
    private static final WrdsLocation DMTM5 = GeneralWRDSReaderTest.createFeature(null, null, "DMTM5");
    private static final WrdsLocation PONS2 = GeneralWRDSReaderTest.createFeature(null, null, "PONS2");
    private static final WrdsLocation MCKG1 = GeneralWRDSReaderTest.createFeature(null, null, "MCKG1");
    private static final WrdsLocation DSNG1 = GeneralWRDSReaderTest.createFeature(null, null, "DSNG1");
    private static final WrdsLocation BVAW2 = GeneralWRDSReaderTest.createFeature(null, null, "BVAW2");
    private static final WrdsLocation CNEO2 = GeneralWRDSReaderTest.createFeature(null, null, "CNEO2");
    private static final WrdsLocation CMKT2 = GeneralWRDSReaderTest.createFeature(null, null, "CMKT2");
    private static final WrdsLocation BDWN6 = GeneralWRDSReaderTest.createFeature(null, null, "BDWN6");
    private static final WrdsLocation CFBN6 = GeneralWRDSReaderTest.createFeature(null, null, "CFBN6");
    private static final WrdsLocation CCSA1 = GeneralWRDSReaderTest.createFeature(null, null, "CCSA1");
    private static final WrdsLocation LGNN8 = GeneralWRDSReaderTest.createFeature(null, null, "LGNN8");
    private static final WrdsLocation BCLN7 = GeneralWRDSReaderTest.createFeature(null, null, "BCLN7");
    private static final WrdsLocation KERV2 = GeneralWRDSReaderTest.createFeature(null, null, "KERV2");
    private static final WrdsLocation ARDS1 = GeneralWRDSReaderTest.createFeature(null, null, "ARDS1");
    private static final WrdsLocation WINW2 = GeneralWRDSReaderTest.createFeature(null, null, "WINW2");
    private static final WrdsLocation SRDN5 = GeneralWRDSReaderTest.createFeature(null, null, "SRDN5");
    private static final WrdsLocation MNTN1 = GeneralWRDSReaderTest.createFeature(null, null, "MNTN1");
    private static final WrdsLocation GNSW4 = GeneralWRDSReaderTest.createFeature(null, null, "GNSW4");
    private static final WrdsLocation JAIO1 = GeneralWRDSReaderTest.createFeature(null, null, "JAIO1");
    private static final WrdsLocation INCO1 = GeneralWRDSReaderTest.createFeature(null, null, "INCO1");
    private static final WrdsLocation PRMO1 = GeneralWRDSReaderTest.createFeature(null, null, "PRMO1");
    private static final WrdsLocation PARO1 = GeneralWRDSReaderTest.createFeature(null, null, "PARO1");
    private static final WrdsLocation BRCO1 = GeneralWRDSReaderTest.createFeature(null, null, "BRCO1");
    private static final WrdsLocation WRNO1 = GeneralWRDSReaderTest.createFeature(null, null, "WRNO1");
    private static final WrdsLocation BLEO1 = GeneralWRDSReaderTest.createFeature(null, null, "BLEO1");

    private static final List<WrdsLocation> DESIRED_FEATURES = List.of(
            PTSA1,
            MNTG1,
            BLOF1,
            CEDG1,
            SMAF1,
            CHAF1,
            OKFG1,
            TLPT2,
            NUTF1,
            CDRA1,
            MUCG1,
            PRSG1,
            LSNO2,
            HDGA4,
            FAKE3,
            CNMP1,
            WLLM2,
            RCJD2,
            MUSM5,
            DUMM5,
            DMTM5,
            PONS2,
            MCKG1,
            DSNG1,
            BVAW2,
            CNEO2,
            CMKT2,
            BDWN6,
            CFBN6,
            CCSA1,
            LGNN8,
            BCLN7,
            KERV2,
            ARDS1,
            WINW2,
            SRDN5,
            MNTN1,
            GNSW4,
            JAIO1,
            INCO1,
            PRMO1,
            PARO1,
            BRCO1,
            WRNO1,
            BLEO1
    );

    private static final WrdsLocation STEAK = GeneralWRDSReaderTest.createFeature(null, null, "STEAK");
    private static final WrdsLocation BAKED_POTATO = GeneralWRDSReaderTest.createFeature(null, null, "BakedPotato");

    private UnitMapper unitMapper;

    private GeneralThresholdResponse normalResponse = null;
    private GeneralThresholdResponse funResponse = null;

    private static final MeasurementUnit units = MeasurementUnit.of( "CMS" );


    private static final ThresholdsConfig normalRecurrenceConfig = new ThresholdsConfig(
            ThresholdType.VALUE,
            ThresholdDataType.LEFT,
            new ThresholdsConfig.Source(
                    path2,
                    ThresholdFormat.WRDS,
                    null,
                    null,
                    null,
                    null,//null ratings provider.
                    null,
                    LeftOrRightOrBaseline.LEFT
            ),
            ThresholdOperator.GREATER_THAN
    );
    
    private static final ThresholdsConfig normalThresholdConfig = new ThresholdsConfig(
            ThresholdType.VALUE,
            ThresholdDataType.LEFT,
            new ThresholdsConfig.Source(
                    path,
                    ThresholdFormat.WRDS,
                    null,
                    null,
                    "NWS-NRLDB",
                    null,//null ratings provider.
                    "stage",
                    LeftOrRightOrBaseline.LEFT
            ),
            ThresholdOperator.GREATER_THAN
    );

    private static final ThresholdsConfig alternativeThresholdConfig = new ThresholdsConfig(
            ThresholdType.VALUE,
            ThresholdDataType.LEFT,
            new ThresholdsConfig.Source(
                    path,
                    ThresholdFormat.WRDS,
                    null,
                    null,
                    "NWS-CMS",
                    null,
                    "stage",
                    LeftOrRightOrBaseline.LEFT
            ),
            ThresholdOperator.LESS_THAN
    );

    private static final ThresholdsConfig funThresholdConfig = new ThresholdsConfig(
            ThresholdType.PROBABILITY_CLASSIFIER,
            ThresholdDataType.LEFT_AND_ANY_RIGHT,
            new ThresholdsConfig.Source(
                    URI.create("this/is/a/nonsense/path"),
                    ThresholdFormat.WRDS,
                    "The unit does not matter here",
                    "Totally doesn't matter",
                    "FlavorTown",
                    "DonkeySauce",
                    "flow",
                    LeftOrRightOrBaseline.LEFT
            ),
            ThresholdOperator.GREATER_THAN
    );

    private GeneralThresholdResponse createNormalThresholdResponse() {
        
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
        response.setThresholds(List.of(ptsa1NWS, mntg1NWS, mntg1NRLDB));

        return response;
    }

    private GeneralThresholdResponse createFunThresholdResponse() {
        
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
        bakedPotatoCalcFlowValues.add( "record", 6844.84);
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
        grossBakedPotatoCalcFlowValues.add( "record", 6.84d);
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
                        grossBakedPotatoDef
                )
        );
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
        Map<WrdsLocation, Set<ThresholdOuter>> normalExtraction = GeneralWRDSReader.extract(
                normalResponse,
                normalThresholdConfig,
                this.unitMapper
        );

        Assert.assertFalse(normalExtraction.containsKey(PTSA1));
        Assert.assertTrue(normalExtraction.containsKey(MNTG1));

        Map<String, Double> thresholdValues = new HashMap<>();
        thresholdValues.put("record", 34.11);
        thresholdValues.put("bankfull", 11.0);
        thresholdValues.put("action", 11.0);
        thresholdValues.put("major", 31.0);
        thresholdValues.put("minor", 20.0);
        thresholdValues.put("moderate", 28.0);

        Set<ThresholdOuter> normalOuterThresholds = normalExtraction.get(MNTG1);

        Assert.assertEquals(6, normalOuterThresholds.size());

        for (ThresholdOuter outerThreshold : normalOuterThresholds) {
            Assert.assertEquals(outerThreshold.getOperator(), Operator.GREATER);
            Assert.assertEquals(outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT);

            Assert.assertTrue(thresholdValues.containsKey(outerThreshold.getThreshold().getName()));
            Assert.assertEquals(
                    thresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }

        
        //This a test of flow thresholds.  See the funThresholdConfig for more information.
        Map<WrdsLocation, Set<ThresholdOuter>> funExtraction = GeneralWRDSReader.extract(
                funResponse,
                funThresholdConfig,
                this.unitMapper
        );

        Assert.assertTrue(funExtraction.containsKey(STEAK));
        Assert.assertTrue(funExtraction.containsKey(BAKED_POTATO));

        Assert.assertEquals(7, funExtraction.get(STEAK).size()); 
        Assert.assertEquals(6, funExtraction.get(BAKED_POTATO).size());

        thresholdValues = new HashMap<>();

        thresholdValues.put("bankfull", -14.586);
        thresholdValues.put("low", -5.7);
        thresholdValues.put("action", -13.5);
        thresholdValues.put("minor", -189.42);
        thresholdValues.put("moderate", -868.5);
        thresholdValues.put("major", -90144.2);
        thresholdValues.put("record", -4846844.5484);

        for (ThresholdOuter outerThreshold : funExtraction.get(STEAK)) {
            Assert.assertEquals(outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT);
            Assert.assertEquals(outerThreshold.getOperator(), Operator.GREATER);
            Assert.assertTrue(thresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    thresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }

        thresholdValues = new HashMap<>();

        thresholdValues.put("low", 54.7);
        thresholdValues.put("bankfull", 1458.6);
        thresholdValues.put("minor", 18942.0);
        thresholdValues.put("moderate", 88.5);
        thresholdValues.put("major", 901.2);
        thresholdValues.put("record", 6844.84);

        for (ThresholdOuter outerThreshold : funExtraction.get(BAKED_POTATO)) {
            Assert.assertEquals(outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT);
            Assert.assertEquals(outerThreshold.getOperator(), Operator.GREATER);
            Assert.assertTrue(thresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    thresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }

        //This is a test of stage thresholds; see alternativeThresholdConfig.
        Map<WrdsLocation, Set<ThresholdOuter>> alternativeNormalExtraction = GeneralWRDSReader.extract(
                normalResponse,
                alternativeThresholdConfig,
                this.unitMapper
        );

        Assert.assertTrue(alternativeNormalExtraction.containsKey(PTSA1));
        Assert.assertTrue(alternativeNormalExtraction.containsKey(MNTG1));

        thresholdValues = new HashMap<>();
        thresholdValues.put("action", 11.0);
        thresholdValues.put("major", 31.0);
        thresholdValues.put("minor", 20.0);
        thresholdValues.put("moderate", 28.0);

        Assert.assertEquals(4, alternativeNormalExtraction.get(MNTG1).size());

        for (ThresholdOuter outerThreshold : alternativeNormalExtraction.get(MNTG1)) {
            Assert.assertEquals(outerThreshold.getOperator(), Operator.LESS);
            Assert.assertEquals(outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT);

            Assert.assertTrue(thresholdValues.containsKey(outerThreshold.getThreshold().getName()));
            Assert.assertEquals(
                    thresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }

        Assert.assertEquals(4, alternativeNormalExtraction.get(PTSA1).size());

        thresholdValues = new HashMap<>();
        thresholdValues.put("action", 0.0);
        thresholdValues.put("major", 0.0);
        thresholdValues.put("minor", 0.0);
        thresholdValues.put("moderate", 0.0);

        for (ThresholdOuter outerThreshold : alternativeNormalExtraction.get(PTSA1)) {
            Assert.assertEquals(outerThreshold.getOperator(), Operator.LESS);
            Assert.assertEquals(outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT);

            Assert.assertTrue(thresholdValues.containsKey(outerThreshold.getThreshold().getName()));
            Assert.assertEquals(
                    thresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }

        //This is a test of stage thresholds; see the normalThresholdConfig.
        Map<WrdsLocation, Set<ThresholdOuter>> normalButFunExtraction = GeneralWRDSReader.extract(
                funResponse,
                normalThresholdConfig,
                this.unitMapper
        );

        Assert.assertTrue(normalButFunExtraction.containsKey(STEAK));
        Assert.assertTrue(normalButFunExtraction.containsKey(BAKED_POTATO));

        //Since calculated stages aren't used, and STEAK includes no regular stage thresholds 
        //these counts and expected resultswere updated for API 3.0.
        Assert.assertEquals(0, normalButFunExtraction.get(STEAK).size());
        Assert.assertEquals(3, normalButFunExtraction.get(BAKED_POTATO).size());

        thresholdValues = new HashMap<>();
        thresholdValues.put("minor", 5.54);
        thresholdValues.put("moderate", 4.0);
        thresholdValues.put("major", 158.0);

        for (ThresholdOuter outerThreshold : normalButFunExtraction.get(BAKED_POTATO)) {
            Assert.assertEquals(outerThreshold.getOperator(), Operator.GREATER);
            Assert.assertEquals(outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT);

            Assert.assertTrue(thresholdValues.containsKey(outerThreshold.getThreshold().getName()));
            Assert.assertEquals(
                    thresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }
    }

    @Test
    public void testGroupLocations()
    {
        Set<String> desiredFeatures = DESIRED_FEATURES.stream().map(WrdsLocation::getNwsLid).collect(Collectors.toSet());
        Set<String> groupedLocations = WRDSReader.groupLocations( desiredFeatures );
        Assert.assertEquals(groupedLocations.size(), 3);

        StringJoiner firstGroupBuilder = new StringJoiner(",");
        StringJoiner secondGroupBuilder = new StringJoiner(",");
        StringJoiner thirdGroupBuilder = new StringJoiner(",");


        Iterator<String> desiredIterator = desiredFeatures.iterator();

        for ( int i = 0; i < WRDSReader.LOCATION_REQUEST_COUNT; i++ )
        {
            firstGroupBuilder.add( desiredIterator.next() );
        }

        for ( int i = 0; i < WRDSReader.LOCATION_REQUEST_COUNT; i++ )
        {
            secondGroupBuilder.add( desiredIterator.next() );
        }

        for ( int i = 0; i < WRDSReader.LOCATION_REQUEST_COUNT && desiredIterator.hasNext(); i++ )
        {
            thirdGroupBuilder.add( desiredIterator.next() );
        }

        String firstGroup = firstGroupBuilder.toString();
        String secondGroup = secondGroupBuilder.toString();
        String thirdGroup = thirdGroupBuilder.toString();

        Assert.assertTrue( groupedLocations.contains( firstGroup ) );
        Assert.assertTrue( groupedLocations.contains( secondGroup ) );
        Assert.assertTrue( groupedLocations.contains( thirdGroup ) );
    }

    @Test
    public void testGetResponse()
    {
        GeneralWRDSReader reader = new GeneralWRDSReader( systemSettings );
        
        GeneralThresholdResponse response = reader.getResponse(path);

        Assert.assertEquals(10, response.getThresholds().size());
        Iterator<GeneralThresholdDefinition> iterator = response.getThresholds().iterator();

        GeneralThresholdDefinition activeCheckedThresholds = iterator.next();

        //==== PTSA1 is first.  This is a mostly empty set of thresholds.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals("NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", activeCheckedThresholds.getMetadata().getThreshold_source_description());
        Assert.assertEquals("FT", activeCheckedThresholds.getMetadata().getStage_units());
        Assert.assertEquals("CFS", activeCheckedThresholds.getMetadata().getFlow_units());
        Assert.assertEquals("NRLDB", activeCheckedThresholds.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", activeCheckedThresholds.getThresholdProvider());
        Assert.assertEquals("PTSA1", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getLocation_id());
        Assert.assertEquals("NWS Station", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getId_type());
        Assert.assertEquals("National Weather Service - National River Location Database", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getDescription());
        Assert.assertEquals("NRLDB", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getSource());
        
        //Check the values with calculated flow included.
        Map<WrdsLocation, Set<ThresholdOuter>> results = activeCheckedThresholds.getThresholds( 
            WRDSThresholdType.FLOW, 
            Operator.GREATER, 
            ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, 
            true, 
            this.unitMapper );
        
        Set<ThresholdOuter> thresholds = results.values().iterator().next();
        Map<String, Double> expectedThresholdValues = new HashMap<>();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues.put("low", 0.0);

        for (ThresholdOuter outerThreshold : thresholds) {
            Assert.assertEquals(ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, outerThreshold.getDataType());
            Assert.assertEquals(Operator.GREATER, outerThreshold.getOperator());
            Assert.assertTrue(expectedThresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    expectedThresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }
        
        activeCheckedThresholds = iterator.next();

        //==== PTSA1 is second.  On difference: rating info.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals("NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", activeCheckedThresholds.getMetadata().getThreshold_source_description());
        Assert.assertEquals("FT", activeCheckedThresholds.getMetadata().getStage_units());
        Assert.assertEquals("CFS", activeCheckedThresholds.getMetadata().getFlow_units());
        Assert.assertEquals("USGS Rating Depot", activeCheckedThresholds.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", activeCheckedThresholds.getThresholdProvider());
        Assert.assertEquals("02372250", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getLocation_id());
        Assert.assertEquals("USGS Gage", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getId_type());
        Assert.assertEquals("The EXSA rating curves provided by USGS", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getDescription());
        Assert.assertEquals("USGS Rating Depot", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getSource());
        
        //Check the values with calculated flow included.
        results = activeCheckedThresholds.getThresholds( 
            WRDSThresholdType.FLOW, 
            Operator.GREATER, 
            ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, 
            true, 
            this.unitMapper );
        
        thresholds = results.values().iterator().next();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues.put("low", 0.0);

        for (ThresholdOuter outerThreshold : thresholds) {
            Assert.assertEquals(ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, outerThreshold.getDataType());
            Assert.assertEquals(Operator.GREATER, outerThreshold.getOperator());
            Assert.assertTrue(expectedThresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    expectedThresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }
        
        activeCheckedThresholds = iterator.next();
        
        //==== MTNG1 is third.  On difference: rating info.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals("NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", activeCheckedThresholds.getMetadata().getThreshold_source_description());
        Assert.assertEquals("FT", activeCheckedThresholds.getMetadata().getStage_units());
        Assert.assertEquals("CFS", activeCheckedThresholds.getMetadata().getFlow_units());
        Assert.assertEquals("NRLDB", activeCheckedThresholds.getRatingProvider());
        
        //Check the values with calculated flow included.
        results = activeCheckedThresholds.getThresholds( 
            WRDSThresholdType.FLOW, 
            Operator.GREATER, 
            ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, 
            true, 
            this.unitMapper );
        
        thresholds = results.values().iterator().next();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues.put("low", 557.8);
        expectedThresholdValues.put("bankfull", 9379.0);
        expectedThresholdValues.put("action", 9379.0);
        expectedThresholdValues.put("flood", 35331.0);
        expectedThresholdValues.put("minor", 35331.0);
        expectedThresholdValues.put("moderate", 102042.0);
        expectedThresholdValues.put("major", 142870.0);
        expectedThresholdValues.put("record", 136000.0);
        
        for (ThresholdOuter outerThreshold : thresholds) {
            Assert.assertEquals(ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, outerThreshold.getDataType());
            Assert.assertEquals(Operator.GREATER, outerThreshold.getOperator());
            Assert.assertTrue(expectedThresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    expectedThresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }
        
        activeCheckedThresholds = iterator.next(); //Skip the 4th. 
        activeCheckedThresholds = iterator.next(); //Frankly, I'm not even sure we need to check the 5th.
        
        //==== BLOF1 is fifth.  On difference: rating info.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals("NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", activeCheckedThresholds.getMetadata().getThreshold_source_description());
        Assert.assertEquals("FT", activeCheckedThresholds.getMetadata().getStage_units());
        Assert.assertEquals("CFS", activeCheckedThresholds.getMetadata().getFlow_units());
        Assert.assertEquals("NRLDB", activeCheckedThresholds.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", activeCheckedThresholds.getThresholdProvider());
        Assert.assertEquals("BLOF1", activeCheckedThresholds.getMetadata().getNws_lid() );
        Assert.assertEquals("all (stage,flow)", activeCheckedThresholds.getMetadata().getThreshold_type() );
        Assert.assertEquals("BLOF1", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getLocation_id());
        Assert.assertEquals("NWS Station", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getId_type());
        Assert.assertEquals("National Weather Service - National River Location Database", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getDescription());
        Assert.assertEquals("NRLDB", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getSource());
        
        //Stage
        results = activeCheckedThresholds.getThresholds( 
            WRDSThresholdType.STAGE, 
            Operator.GREATER, 
            ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, 
            true, 
            this.unitMapper );
        
        thresholds = results.values().iterator().next();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues.put("bankfull", 15.0);
        expectedThresholdValues.put("action", 13.0);
        expectedThresholdValues.put("flood", 17.0);
        expectedThresholdValues.put("minor", 17.0);
        expectedThresholdValues.put("moderate", 23.5);
        expectedThresholdValues.put("major", 26.0);
        expectedThresholdValues.put("record", 28.6);

        for (ThresholdOuter outerThreshold : thresholds) {
            Assert.assertEquals(ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, outerThreshold.getDataType());
            Assert.assertEquals(Operator.GREATER, outerThreshold.getOperator());
            Assert.assertTrue(expectedThresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    expectedThresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }
        
        //Check the values with calculated flow included.
        results = activeCheckedThresholds.getThresholds( 
            WRDSThresholdType.FLOW, 
            Operator.GREATER, 
            ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, 
            true, 
            this.unitMapper );
        
        thresholds = results.values().iterator().next();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues.put("bankfull", 38633.0);
        expectedThresholdValues.put("action", 31313.0);
        expectedThresholdValues.put("flood", 48628.0);
        expectedThresholdValues.put("minor", 48628.0);
        expectedThresholdValues.put("moderate", 144077.0);
        expectedThresholdValues.put("major", 216266.0);
        expectedThresholdValues.put("record", 209000.0);

        for (ThresholdOuter outerThreshold : thresholds) {
            Assert.assertEquals(ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, outerThreshold.getDataType());
            Assert.assertEquals(Operator.GREATER, outerThreshold.getOperator());
            Assert.assertTrue(expectedThresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    expectedThresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }
        
        //Check the values with raw flow only.
        results = activeCheckedThresholds.getThresholds( 
            WRDSThresholdType.FLOW, 
            Operator.GREATER, 
            ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, 
            false, 
            this.unitMapper );
        
        thresholds = results.values().iterator().next();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues.put("flood", 36900.0);;
        expectedThresholdValues.put("record", 209000.0);

        for (ThresholdOuter outerThreshold : thresholds) {
            Assert.assertEquals(ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT, outerThreshold.getDataType());
            Assert.assertEquals(Operator.GREATER, outerThreshold.getOperator());
            Assert.assertTrue(expectedThresholdValues.containsKey(outerThreshold.getThreshold().getName()));

            Assert.assertEquals(
                    expectedThresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }
        
        //I believe additional testing of the remaining thresholds is unnecessary.
    }

    @Test
    public void testReadThresholds() throws IOException {
        Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds(
                systemSettings,
                normalThresholdConfig,
                this.unitMapper,
                DESIRED_FEATURES.stream().map(WrdsLocation::getNwsLid).collect(Collectors.toSet())
        );

        Assert.assertTrue(readThresholds.containsKey(PTSA1));
        Assert.assertTrue(readThresholds.containsKey(MNTG1));
        Assert.assertTrue(readThresholds.containsKey(BLOF1));
        Assert.assertTrue(readThresholds.containsKey(SMAF1));
        Assert.assertTrue(readThresholds.containsKey(CEDG1));

        //The two low thresholds available are identical in both label and value, so only one is included.
        Set<ThresholdOuter> ptsa1Thresholds = readThresholds.get(PTSA1);
        Assert.assertEquals(1, ptsa1Thresholds.size()); 
        
        Set<ThresholdOuter> blof1Thresholds = readThresholds.get(BLOF1);
        Assert.assertEquals(7, blof1Thresholds.size());

        boolean hasLow = false;
        boolean hasBankfull = false;
        boolean hasAction = false;
        boolean hasMinor = false;
        boolean hasModerate = false;
        boolean hasMajor = false;
        boolean hasRecord = false;

        List<String> properThresholds = List.of(
                "bankfull",
                "action",
                "flood",
                "minor",
                "moderate",
                "major",
                "record"
        );

        for (ThresholdOuter thresholdOuter : blof1Thresholds) {
            String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

            Assert.assertTrue(properThresholds.contains(thresholdName));

            switch (thresholdName) {
                case "bankfull":
                    hasBankfull = true;
                    Assert.assertEquals(
                            15.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "action":
                    hasAction = true;
                    Assert.assertEquals(
                            13.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "flood":
                    hasMinor = true;
                    Assert.assertEquals(
                            17.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "minor":
                    hasMinor = true;
                    Assert.assertEquals(
                            17.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "moderate":
                    hasModerate = true;
                    Assert.assertEquals(
                            23.5,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "major":
                    hasMajor = true;
                    Assert.assertEquals(
                            26.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "record":
                    hasRecord = true;
                    Assert.assertEquals(
                            28.6,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
            }
        }

        Assert.assertFalse(hasLow);
        Assert.assertTrue(hasBankfull);
        Assert.assertTrue(hasAction);
        Assert.assertTrue(hasMinor);
        Assert.assertTrue(hasModerate);
        Assert.assertTrue(hasMajor);
        Assert.assertTrue(hasRecord);
    }
    
    @Test
    public void testReadRecurrenceFlows() throws IOException {
        Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds(
                systemSettings,
                normalRecurrenceConfig,
                this.unitMapper,
                DESIRED_FEATURES.stream().map(WrdsLocation::getNwsLid).collect(Collectors.toSet())
        );

        Assert.assertTrue(readThresholds.containsKey(PTSA1));
        Assert.assertTrue(readThresholds.containsKey(MNTG1));
        Assert.assertTrue(readThresholds.containsKey(BLOF1));
        Assert.assertTrue(readThresholds.containsKey(SMAF1));
        Assert.assertTrue(readThresholds.containsKey(CEDG1));

        
        Set<ThresholdOuter> blof1Thresholds = readThresholds.get(BLOF1);
        Assert.assertEquals(6, blof1Thresholds.size());
        
        boolean has1_5 = false;
        boolean has2_0 = false;
        boolean has3_0 = false;
        boolean has4_0 = false;
        boolean has5_0 = false;
        boolean has10_0 = false;

        List<String> properThresholds = List.of(
                "year_1_5",
                "year_2_0",
                "year_3_0",
                "year_4_0",
                "year_5_0",
                "year_10_0"
        );
        
        for (ThresholdOuter thresholdOuter : blof1Thresholds) {
            String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

            Assert.assertTrue(properThresholds.contains(thresholdName));

            switch (thresholdName) {
                case "year_1_5":
                    has1_5 = true;
                    Assert.assertEquals(
                            58864.26,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "year_2_0":
                    has2_0 = true;
                    Assert.assertEquals(
                            87362.48,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "year_3_0":
                    has3_0 = true;
                    Assert.assertEquals(
                            109539.05,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "year_4_0":
                    has4_0 = true;
                    Assert.assertEquals(
                            128454.64,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "year_5_0":
                    has5_0 = true;
                    Assert.assertEquals(
                            176406.6,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "year_10_0":
                    has10_0 = true;
                    Assert.assertEquals(
                            216831.58000000002,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
            }
        }

        Assert.assertTrue(has1_5);
        Assert.assertTrue(has2_0);
        Assert.assertTrue(has3_0);
        Assert.assertTrue(has4_0);
        Assert.assertTrue(has5_0);
        Assert.assertTrue(has10_0);
    }
}
