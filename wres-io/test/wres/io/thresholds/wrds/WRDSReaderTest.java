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
import wres.io.thresholds.wrds.v2.CalculatedThresholdValues;
import wres.io.thresholds.wrds.v2.OriginalThresholdValues;
import wres.io.thresholds.wrds.v2.ThresholdDefinition;
import wres.io.thresholds.wrds.v2.ThresholdMetadata;
import wres.io.thresholds.wrds.v2.ThresholdResponse;
import wres.system.SystemSettings;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class WRDSReaderTest {
    private static final URI path = URI.create( "testinput/thresholds/wrds/thresholds.json" );
    private static final double EPSILON = 0.00001;

    private static WrdsLocation createFeature(final String featureId, final String usgsSiteCode, final String lid )
    {
        return new WrdsLocation(featureId, usgsSiteCode, lid);
    }

    private static final WrdsLocation PTSA1 = WRDSReaderTest.createFeature("2323396", "02372250","PTSA1");
    private static final WrdsLocation MNTG1 = WRDSReaderTest.createFeature("6444276", "02349605", "MNTG1");
    private static final WrdsLocation BLOF1 = WRDSReaderTest.createFeature("2297254", "02358700", "BLOF1");
    private static final WrdsLocation CEDG1 = WRDSReaderTest.createFeature("2310009", "02343940", "CEDG1");
    private static final WrdsLocation SMAF1 = WRDSReaderTest.createFeature("2298964", "02359170", "SMAF1");
    private static final WrdsLocation CHAF1 = WRDSReaderTest.createFeature("2293124", "02358000", "CHAF1");
    private static final WrdsLocation OKFG1 = WRDSReaderTest.createFeature("6447636", "02350512","OKFG1");
    private static final WrdsLocation TLPT2 = WRDSReaderTest.createFeature("13525368", "07311630", "TLPT2");
    private static final WrdsLocation NUTF1 = WRDSReaderTest.createFeature(null, null, "NUTF1");
    private static final WrdsLocation CDRA1 = WRDSReaderTest.createFeature(null, null, "CDRA1");
    private static final WrdsLocation MUCG1 = WRDSReaderTest.createFeature(null, null, "MUCG1");
    private static final WrdsLocation PRSG1 = WRDSReaderTest.createFeature(null, null, "PRSG1");
    private static final WrdsLocation LSNO2 = WRDSReaderTest.createFeature(null, null, "LSNO2");
    private static final WrdsLocation HDGA4 = WRDSReaderTest.createFeature(null, null, "HDGA4");
    private static final WrdsLocation FAKE3 = WRDSReaderTest.createFeature(null, null, "FAKE3");
    private static final WrdsLocation CNMP1 = WRDSReaderTest.createFeature(null, null, "CNMP1");
    private static final WrdsLocation WLLM2 = WRDSReaderTest.createFeature(null, null, "WLLM2");
    private static final WrdsLocation RCJD2 = WRDSReaderTest.createFeature(null, null, "RCJD2");
    private static final WrdsLocation MUSM5 = WRDSReaderTest.createFeature(null, null, "MUSM5");
    private static final WrdsLocation DUMM5 = WRDSReaderTest.createFeature(null, null, "DUMM5");
    private static final WrdsLocation DMTM5 = WRDSReaderTest.createFeature(null, null, "DMTM5");
    private static final WrdsLocation PONS2 = WRDSReaderTest.createFeature(null, null, "PONS2");
    private static final WrdsLocation MCKG1 = WRDSReaderTest.createFeature(null, null, "MCKG1");
    private static final WrdsLocation DSNG1 = WRDSReaderTest.createFeature(null, null, "DSNG1");
    private static final WrdsLocation BVAW2 = WRDSReaderTest.createFeature(null, null, "BVAW2");
    private static final WrdsLocation CNEO2 = WRDSReaderTest.createFeature(null, null, "CNEO2");
    private static final WrdsLocation CMKT2 = WRDSReaderTest.createFeature(null, null, "CMKT2");
    private static final WrdsLocation BDWN6 = WRDSReaderTest.createFeature(null, null, "BDWN6");
    private static final WrdsLocation CFBN6 = WRDSReaderTest.createFeature(null, null, "CFBN6");
    private static final WrdsLocation CCSA1 = WRDSReaderTest.createFeature(null, null, "CCSA1");
    private static final WrdsLocation LGNN8 = WRDSReaderTest.createFeature(null, null, "LGNN8");
    private static final WrdsLocation BCLN7 = WRDSReaderTest.createFeature(null, null, "BCLN7");
    private static final WrdsLocation KERV2 = WRDSReaderTest.createFeature(null, null, "KERV2");
    private static final WrdsLocation ARDS1 = WRDSReaderTest.createFeature(null, null, "ARDS1");
    private static final WrdsLocation WINW2 = WRDSReaderTest.createFeature(null, null, "WINW2");
    private static final WrdsLocation SRDN5 = WRDSReaderTest.createFeature(null, null, "SRDN5");
    private static final WrdsLocation MNTN1 = WRDSReaderTest.createFeature(null, null, "MNTN1");
    private static final WrdsLocation GNSW4 = WRDSReaderTest.createFeature(null, null, "GNSW4");
    private static final WrdsLocation JAIO1 = WRDSReaderTest.createFeature(null, null, "JAIO1");
    private static final WrdsLocation INCO1 = WRDSReaderTest.createFeature(null, null, "INCO1");
    private static final WrdsLocation PRMO1 = WRDSReaderTest.createFeature(null, null, "PRMO1");
    private static final WrdsLocation PARO1 = WRDSReaderTest.createFeature(null, null, "PARO1");
    private static final WrdsLocation BRCO1 = WRDSReaderTest.createFeature(null, null, "BRCO1");
    private static final WrdsLocation WRNO1 = WRDSReaderTest.createFeature(null, null, "WRNO1");
    private static final WrdsLocation BLEO1 = WRDSReaderTest.createFeature(null, null, "BLEO1");

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

    private static final WrdsLocation STEAK = WRDSReaderTest.createFeature(null, null, "STEAK");
    private static final WrdsLocation BAKED_POTATO = WRDSReaderTest.createFeature(null, null, "BakedPotato");

    private UnitMapper unitMapper;

    private ThresholdResponse normalResponse = null;
    private ThresholdResponse funResponse = null;

    private static final MeasurementUnit units = MeasurementUnit.of( "CMS" );

    private static final ThresholdsConfig normalThresholdConfig = new ThresholdsConfig(
            ThresholdType.VALUE,
            ThresholdDataType.LEFT,
            new ThresholdsConfig.Source(
                    path,
                    ThresholdFormat.WRDS,
                    null,
                    null,
                    "NWS-NRLDB",
                    null,
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

    private ThresholdResponse createNormalThresholdResponse() {
        ThresholdMetadata ptsa1NWSMetadata = new ThresholdMetadata();
        ptsa1NWSMetadata.setLocation_id("PTSA1");
        ptsa1NWSMetadata.setNws_lid("PTSA1");
        ptsa1NWSMetadata.setUsgs_site_code("02372250");
        ptsa1NWSMetadata.setNwm_feature_id("2323396");
        ptsa1NWSMetadata.setThreshold_source("NWS-CMS");
        ptsa1NWSMetadata.setStage_unit("FT");

        OriginalThresholdValues ptsa1NWSOriginalValues = new OriginalThresholdValues();
        ptsa1NWSOriginalValues.setLow_stage("None");
        ptsa1NWSOriginalValues.setBankfull_stage("None");
        ptsa1NWSOriginalValues.setAction_stage("0.0");
        ptsa1NWSOriginalValues.setMinor_stage("0.0");
        ptsa1NWSOriginalValues.setModerate_stage("0.0");
        ptsa1NWSOriginalValues.setMajor_stage("0.0");
        ptsa1NWSOriginalValues.setRecord_stage("None");

        ThresholdDefinition ptsa1NWS = new ThresholdDefinition();
        ptsa1NWS.setMetadata(ptsa1NWSMetadata);
        ptsa1NWS.setOriginal_values(ptsa1NWSOriginalValues);
        ptsa1NWS.setCalculated_values(new CalculatedThresholdValues());

        ThresholdMetadata mntg1NWSMetadata = new ThresholdMetadata();
        mntg1NWSMetadata.setLocation_id("MNTG1");
        mntg1NWSMetadata.setNws_lid("MNTG1");
        mntg1NWSMetadata.setUsgs_site_code("02349605");
        mntg1NWSMetadata.setNwm_feature_id("6444276");
        mntg1NWSMetadata.setThreshold_source("NWS-CMS");
        mntg1NWSMetadata.setStage_unit("FT");

        OriginalThresholdValues mntg1NWSOriginalValues = new OriginalThresholdValues();
        mntg1NWSOriginalValues.setLow_stage("None");
        mntg1NWSOriginalValues.setBankfull_stage("None");
        mntg1NWSOriginalValues.setAction_stage("11.0");
        mntg1NWSOriginalValues.setMinor_stage("20.0");
        mntg1NWSOriginalValues.setModerate_stage("28.0");
        mntg1NWSOriginalValues.setMajor_stage("31.0");
        mntg1NWSOriginalValues.setRecord_stage("None");

        ThresholdDefinition mntg1NWS = new ThresholdDefinition();
        mntg1NWS.setMetadata(mntg1NWSMetadata);
        mntg1NWS.setOriginal_values(mntg1NWSOriginalValues);
        mntg1NWS.setCalculated_values(new CalculatedThresholdValues());

        ThresholdMetadata mntg1NRLDBMetadata = new ThresholdMetadata();
        mntg1NRLDBMetadata.setLocation_id("MNTG1");
        mntg1NRLDBMetadata.setNws_lid("MNTG1");
        mntg1NRLDBMetadata.setUsgs_site_code("02349605");
        mntg1NRLDBMetadata.setNwm_feature_id("6444276");
        mntg1NRLDBMetadata.setThreshold_source("NWS-NRLDB");
        mntg1NRLDBMetadata.setStage_unit("FT");

        OriginalThresholdValues mntg1NRLDBOriginalValues = new OriginalThresholdValues();
        mntg1NRLDBOriginalValues.setLow_stage("None");
        mntg1NRLDBOriginalValues.setBankfull_stage("11.0");
        mntg1NRLDBOriginalValues.setAction_stage("11.0");
        mntg1NRLDBOriginalValues.setMinor_stage("20.0");
        mntg1NRLDBOriginalValues.setModerate_stage("28.0");
        mntg1NRLDBOriginalValues.setMajor_stage("31.0");
        mntg1NRLDBOriginalValues.setRecord_stage("34.11");

        ThresholdDefinition mntg1NRLDB = new ThresholdDefinition();
        mntg1NRLDB.setMetadata(mntg1NRLDBMetadata);
        mntg1NRLDB.setOriginal_values(mntg1NRLDBOriginalValues);
        mntg1NRLDB.setCalculated_values(new CalculatedThresholdValues());

        ThresholdResponse response = new ThresholdResponse();
        response.setThresholds(List.of(ptsa1NWS, mntg1NWS, mntg1NRLDB));

        return response;
    }

    private ThresholdResponse createFunThresholdResponse() {
        ThresholdMetadata steakMetadata = new ThresholdMetadata();
        steakMetadata.setLocation_id("STEAK");
        steakMetadata.setNws_lid("STEAK");
        steakMetadata.setFlow_unit("CFS");
        steakMetadata.setStage_unit("MM");
        steakMetadata.setThreshold_source("FlavorTown");
        steakMetadata.setRating_source("DonkeySauce");

        OriginalThresholdValues steakOriginalValues = new OriginalThresholdValues();
        steakOriginalValues.setLow_flow("5.7");
        steakOriginalValues.setLow_stage("99.586168");
        steakOriginalValues.setBankfull_flow("14.586");
        steakOriginalValues.setBankfull_stage("120.50");
        steakOriginalValues.setAction_flow("13.5");
        steakOriginalValues.setAction_stage("180.58");
        steakOriginalValues.setMinor_flow("189.42");
        steakOriginalValues.setMinor_stage("350.5419");
        steakOriginalValues.setModerate_flow("868.5");
        steakOriginalValues.setModerate_stage("420.0");
        steakOriginalValues.setMajor_flow("90144.2");
        steakOriginalValues.setMajor_stage("8054.54");
        steakOriginalValues.setRecord_flow("4846844.5484");
        steakOriginalValues.setRecord_stage("9999.594");

        CalculatedThresholdValues steakCalculatedValues = new CalculatedThresholdValues();
        steakCalculatedValues.setLow_flow("-5.7");
        steakCalculatedValues.setLow_stage("-99.586168");
        steakCalculatedValues.setBankfull_flow("-14.586");
        steakCalculatedValues.setBankfull_stage("-120.50");
        steakCalculatedValues.setAction_flow("-13.5");
        steakCalculatedValues.setAction_stage("-180.58");
        steakCalculatedValues.setMinor_flow("-189.42");
        steakCalculatedValues.setMinor_stage("-350.5419");
        steakCalculatedValues.setModerate_flow("-868.5");
        steakCalculatedValues.setModerate_stage("-420.0");
        steakCalculatedValues.setMajor_flow("-90144.2");
        steakCalculatedValues.setMajor_stage("-8054.54");
        steakCalculatedValues.setRecord_flow("-4846844.5484");
        steakCalculatedValues.setRecord_stage("-9999.594");

        ThresholdDefinition steakDefinition = new ThresholdDefinition();
        steakDefinition.setMetadata(steakMetadata);
        steakDefinition.setOriginal_values(steakOriginalValues);
        steakDefinition.setCalculated_values(steakCalculatedValues);

        ThresholdMetadata grossSteakMetadata = new ThresholdMetadata();
        grossSteakMetadata.setLocation_id("STEAK");
        grossSteakMetadata.setNws_lid("STEAK");
        grossSteakMetadata.setFlow_unit("CFS");
        grossSteakMetadata.setStage_unit("MM");
        grossSteakMetadata.setThreshold_source("NWS-NRLDB");
        grossSteakMetadata.setRating_source("DuckSauce");

        OriginalThresholdValues grossSteakOriginalValues = new OriginalThresholdValues();

        CalculatedThresholdValues grossSteakCalculatedValues = new CalculatedThresholdValues();
        grossSteakCalculatedValues.setLow_flow("-57");
        grossSteakCalculatedValues.setLow_stage("990.168");
        grossSteakCalculatedValues.setBankfull_flow("None");
        grossSteakCalculatedValues.setBankfull_stage("-120.50");
        grossSteakCalculatedValues.setAction_flow("-13.5");
        grossSteakCalculatedValues.setAction_stage("-1080.58");
        grossSteakCalculatedValues.setMinor_flow("None");
        grossSteakCalculatedValues.setMinor_stage("-3510.419");
        grossSteakCalculatedValues.setModerate_flow("14");
        grossSteakCalculatedValues.setModerate_stage("5644");
        grossSteakCalculatedValues.setMajor_flow("-9014.2");
        grossSteakCalculatedValues.setMajor_stage("None");
        grossSteakCalculatedValues.setRecord_flow("-46844.5484");
        grossSteakCalculatedValues.setRecord_stage("999.594");

        ThresholdDefinition grossSteakDefinition = new ThresholdDefinition();
        grossSteakDefinition.setMetadata(grossSteakMetadata);
        grossSteakDefinition.setOriginal_values(grossSteakOriginalValues);
        grossSteakDefinition.setCalculated_values(grossSteakCalculatedValues);

        ThresholdMetadata flatIronSsteakMetadata = new ThresholdMetadata();
        flatIronSsteakMetadata.setLocation_id("STEAK");
        flatIronSsteakMetadata.setNws_lid("STEAK");
        flatIronSsteakMetadata.setFlow_unit("CFS");
        flatIronSsteakMetadata.setStage_unit("MM");
        flatIronSsteakMetadata.setThreshold_source("FlatIron");
        flatIronSsteakMetadata.setRating_source("DonkeySauce");

        OriginalThresholdValues flatIronSteakOriginalValues = new OriginalThresholdValues();
        flatIronSteakOriginalValues.setLow_flow("5.7");
        flatIronSteakOriginalValues.setLow_stage("99.586168");
        flatIronSteakOriginalValues.setBankfull_flow("14.586");
        flatIronSteakOriginalValues.setBankfull_stage("120.50");
        flatIronSteakOriginalValues.setAction_flow("13.5");
        flatIronSteakOriginalValues.setAction_stage("180.58");
        flatIronSteakOriginalValues.setMinor_flow("189.42");
        flatIronSteakOriginalValues.setMinor_stage("350.5419");
        flatIronSteakOriginalValues.setModerate_flow("868.5");
        flatIronSteakOriginalValues.setModerate_stage("420.0");
        flatIronSteakOriginalValues.setMajor_flow("90144.2");
        flatIronSteakOriginalValues.setMajor_stage("8054.54");
        flatIronSteakOriginalValues.setRecord_flow("4846844.5484");
        flatIronSteakOriginalValues.setRecord_stage("9999.594");

        CalculatedThresholdValues flatIronSteakCalculatedValues = new CalculatedThresholdValues();
        flatIronSteakCalculatedValues.setLow_flow("-5.7");
        flatIronSteakCalculatedValues.setLow_stage("-99.586168");
        flatIronSteakCalculatedValues.setBankfull_flow("-14.586");
        flatIronSteakCalculatedValues.setBankfull_stage("-120.50");
        flatIronSteakCalculatedValues.setAction_flow("-13.5");
        flatIronSteakCalculatedValues.setAction_stage("-180.58");
        flatIronSteakCalculatedValues.setMinor_flow("-189.42");
        flatIronSteakCalculatedValues.setMinor_stage("-350.5419");
        flatIronSteakCalculatedValues.setModerate_flow("-868.5");
        flatIronSteakCalculatedValues.setModerate_stage("-420.0");
        flatIronSteakCalculatedValues.setMajor_flow("-90144.2");
        flatIronSteakCalculatedValues.setMajor_stage("-8054.54");
        flatIronSteakCalculatedValues.setRecord_flow("-4846844.5484");
        flatIronSteakCalculatedValues.setRecord_stage("-9999.594");

        ThresholdDefinition flatIronSteakDefinition = new ThresholdDefinition();
        flatIronSteakDefinition.setMetadata(flatIronSsteakMetadata);
        flatIronSteakDefinition.setOriginal_values(flatIronSteakOriginalValues);
        flatIronSteakDefinition.setCalculated_values(flatIronSteakCalculatedValues);

        ThresholdMetadata bakedPotatoMetadata = new ThresholdMetadata();
        bakedPotatoMetadata.setLocation_id("BakedPotato");
        bakedPotatoMetadata.setNws_lid("BakedPotato");
        bakedPotatoMetadata.setFlow_unit("CFS");
        bakedPotatoMetadata.setStage_unit("MM");
        bakedPotatoMetadata.setThreshold_source("FlavorTown");
        bakedPotatoMetadata.setRating_source("DonkeySauce");

        OriginalThresholdValues bakedPotatoOriginalValues = new OriginalThresholdValues();
        bakedPotatoOriginalValues.setLow_flow("57");
        bakedPotatoOriginalValues.setLow_stage("9.586168");
        bakedPotatoOriginalValues.setBankfull_flow("1458.6");
        bakedPotatoOriginalValues.setBankfull_stage("None");
        bakedPotatoOriginalValues.setAction_flow("None");
        bakedPotatoOriginalValues.setAction_stage("None");
        bakedPotatoOriginalValues.setMinor_flow("142");
        bakedPotatoOriginalValues.setMinor_stage("50.54");
        bakedPotatoOriginalValues.setModerate_flow("86.85");
        bakedPotatoOriginalValues.setModerate_stage("42.0");
        bakedPotatoOriginalValues.setMajor_flow("9.2");
        bakedPotatoOriginalValues.setMajor_stage("None");
        bakedPotatoOriginalValues.setRecord_flow("4.35");
        bakedPotatoOriginalValues.setRecord_stage("None");

        CalculatedThresholdValues bakedPotatoCalculatedValues = new CalculatedThresholdValues();
        bakedPotatoCalculatedValues.setLow_flow("54.7");
        bakedPotatoCalculatedValues.setLow_stage("949.168");
        bakedPotatoCalculatedValues.setBankfull_flow("None");
        bakedPotatoCalculatedValues.setBankfull_stage("170.50");
        bakedPotatoCalculatedValues.setAction_flow("None");
        bakedPotatoCalculatedValues.setAction_stage("None");
        bakedPotatoCalculatedValues.setMinor_flow("18942");
        bakedPotatoCalculatedValues.setMinor_stage("35.59");
        bakedPotatoCalculatedValues.setModerate_flow("88.5");
        bakedPotatoCalculatedValues.setModerate_stage("4240");
        bakedPotatoCalculatedValues.setMajor_flow("901.2");
        bakedPotatoCalculatedValues.setMajor_stage("84.54");
        bakedPotatoCalculatedValues.setRecord_flow("6844.84");
        bakedPotatoCalculatedValues.setRecord_stage("None");

        ThresholdDefinition bakedPotatoDefinition = new ThresholdDefinition();
        bakedPotatoDefinition.setMetadata(bakedPotatoMetadata);
        bakedPotatoDefinition.setOriginal_values(bakedPotatoOriginalValues);
        bakedPotatoDefinition.setCalculated_values(bakedPotatoCalculatedValues);

        ThresholdMetadata grossBakedPotatoMetadata = new ThresholdMetadata();
        grossBakedPotatoMetadata.setLocation_id("BakedPotato");
        grossBakedPotatoMetadata.setNws_lid("BakedPotato");
        grossBakedPotatoMetadata.setFlow_unit("CFS");
        grossBakedPotatoMetadata.setStage_unit("MM");
        grossBakedPotatoMetadata.setThreshold_source("NWS-NRLDB");
        grossBakedPotatoMetadata.setRating_source("DonkeySauce");

        OriginalThresholdValues grossBakedPotatoOriginalValues = new OriginalThresholdValues();
        grossBakedPotatoOriginalValues.setLow_flow("None");
        grossBakedPotatoOriginalValues.setLow_stage("None");
        grossBakedPotatoOriginalValues.setBankfull_flow("None");
        grossBakedPotatoOriginalValues.setBankfull_stage("None");
        grossBakedPotatoOriginalValues.setAction_flow("None");
        grossBakedPotatoOriginalValues.setAction_stage("None");
        grossBakedPotatoOriginalValues.setMinor_flow("1.42");
        grossBakedPotatoOriginalValues.setMinor_stage("5.54");
        grossBakedPotatoOriginalValues.setModerate_flow("186.85");
        grossBakedPotatoOriginalValues.setModerate_stage("4.0");
        grossBakedPotatoOriginalValues.setMajor_flow("92");
        grossBakedPotatoOriginalValues.setMajor_stage("158");
        grossBakedPotatoOriginalValues.setRecord_flow("45");
        grossBakedPotatoOriginalValues.setRecord_stage("None");

        CalculatedThresholdValues grossBakedPotatoCalculatedValues = new CalculatedThresholdValues();
        grossBakedPotatoCalculatedValues.setLow_flow("547");
        grossBakedPotatoCalculatedValues.setLow_stage("949");
        grossBakedPotatoCalculatedValues.setBankfull_flow("None");
        grossBakedPotatoCalculatedValues.setBankfull_stage("1750");
        grossBakedPotatoCalculatedValues.setAction_flow("None");
        grossBakedPotatoCalculatedValues.setAction_stage("8");
        grossBakedPotatoCalculatedValues.setMinor_flow("None");
        grossBakedPotatoCalculatedValues.setMinor_stage("359");
        grossBakedPotatoCalculatedValues.setModerate_flow("88");
        grossBakedPotatoCalculatedValues.setModerate_stage("44");
        grossBakedPotatoCalculatedValues.setMajor_flow("None");
        grossBakedPotatoCalculatedValues.setMajor_stage("844");
        grossBakedPotatoCalculatedValues.setRecord_flow("6.84");
        grossBakedPotatoCalculatedValues.setRecord_stage("None");

        ThresholdDefinition grossBakedPotatoDefinition = new ThresholdDefinition();
        grossBakedPotatoDefinition.setMetadata(grossBakedPotatoMetadata);
        grossBakedPotatoDefinition.setOriginal_values(grossBakedPotatoOriginalValues);
        grossBakedPotatoDefinition.setCalculated_values(grossBakedPotatoCalculatedValues);

        ThresholdResponse response = new ThresholdResponse();
        response.setThresholds(
                List.of(
                        steakDefinition,
                        grossSteakDefinition,
                        flatIronSteakDefinition,
                        bakedPotatoDefinition,
                        grossBakedPotatoDefinition
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
        Map<WrdsLocation, Set<ThresholdOuter>> normalExtraction = WRDSReader.extract(
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

        Map<WrdsLocation, Set<ThresholdOuter>> funExtraction = WRDSReader.extract(
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

        Map<WrdsLocation, Set<ThresholdOuter>> alternativeNormalExtraction = WRDSReader.extract(
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

        Map<WrdsLocation, Set<ThresholdOuter>> normalButFunExtraction = WRDSReader.extract(
                funResponse,
                normalThresholdConfig,
                this.unitMapper
        );

        Assert.assertTrue(normalButFunExtraction.containsKey(STEAK));
        Assert.assertTrue(normalButFunExtraction.containsKey(BAKED_POTATO));

        Assert.assertEquals(6, normalButFunExtraction.get(STEAK).size());
        Assert.assertEquals(6, normalButFunExtraction.get(BAKED_POTATO).size());

        thresholdValues = new HashMap<>();
        thresholdValues.put("low", 990.168);
        thresholdValues.put("bankfull", -120.50);
        thresholdValues.put("action", -1080.58);
        thresholdValues.put("minor", -3510.419);
        thresholdValues.put("moderate", 5644.0);
        thresholdValues.put("record", 999.594);

        for (ThresholdOuter outerThreshold : normalButFunExtraction.get(STEAK)) {
            Assert.assertEquals(outerThreshold.getOperator(), Operator.GREATER);
            Assert.assertEquals(outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT);

            Assert.assertTrue(thresholdValues.containsKey(outerThreshold.getThreshold().getName()));
            Assert.assertEquals(
                    thresholdValues.get(outerThreshold.getThreshold().getName()),
                    outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                    EPSILON
            );
        }

        thresholdValues = new HashMap<>();
        thresholdValues.put("low", 949.0);
        thresholdValues.put("bankfull", 1750.0);
        thresholdValues.put("action", 8.0);
        thresholdValues.put("minor", 359.0);
        thresholdValues.put("moderate", 44.0);
        thresholdValues.put("major", 844.0);

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
        WRDSReader reader = new WRDSReader( systemSettings );
        ThresholdResponse response = reader.getResponse(path);

        Assert.assertEquals(13, response.getThresholds().size());

        Iterator<ThresholdDefinition> definitionIterator = response.getThresholds().iterator();

        ThresholdDefinition ptsa1NWSCMS = definitionIterator.next();

        // Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals("PTSA1", ptsa1NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", ptsa1NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", ptsa1NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", ptsa1NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", ptsa1NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", ptsa1NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", ptsa1NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", ptsa1NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", ptsa1NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", ptsa1NWSCMS.getThresholdProvider());
        Assert.assertTrue(ptsa1NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(0.0, ptsa1NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(0.0, ptsa1NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(0.0, ptsa1NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(0.0, ptsa1NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(ptsa1NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(ptsa1NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(ptsa1NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(ptsa1NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(ptsa1NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(ptsa1NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(ptsa1NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, ptsa1NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, ptsa1NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, ptsa1NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, ptsa1NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(ptsa1NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(ptsa1NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition mntg1NWSCMS = definitionIterator.next();

        // Check the MNTG1: NWS-CMS metadata
        Assert.assertEquals("MNTG1", mntg1NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", mntg1NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", mntg1NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", mntg1NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", mntg1NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", mntg1NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", mntg1NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", mntg1NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", mntg1NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", mntg1NWSCMS.getThresholdProvider());
        Assert.assertTrue(mntg1NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(11.0, mntg1NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(20.0, mntg1NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(28.0, mntg1NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(31.0, mntg1NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(mntg1NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(mntg1NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(mntg1NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(mntg1NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(mntg1NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(mntg1NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(mntg1NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(11.0, mntg1NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(20.0, mntg1NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(28.0, mntg1NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(31.0, mntg1NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(mntg1NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition mntg1NRLDB = definitionIterator.next();

        // Check the MNTG1: NRLDB metadata
        Assert.assertEquals("MNTG1", mntg1NRLDB.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", mntg1NRLDB.getMetadata().getId_type());
        Assert.assertEquals("NWS-NRLDB", mntg1NRLDB.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", mntg1NRLDB.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", mntg1NRLDB.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", mntg1NRLDB.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", mntg1NRLDB.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", mntg1NRLDB.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", mntg1NRLDB.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", mntg1NRLDB.getThresholdProvider());
        Assert.assertTrue(mntg1NRLDB.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getLow_flow());
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getLow_stage());
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getBankfull_flow());
        Assert.assertEquals(11.0, mntg1NRLDB.getOriginal_values().getBankfull_stage(), EPSILON);
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getAction_flow());
        Assert.assertEquals(11.0, mntg1NRLDB.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getMinor_flow());
        Assert.assertEquals(20.0, mntg1NRLDB.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getModerate_flow());
        Assert.assertEquals(28.0, mntg1NRLDB.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getMajor_flow());
        Assert.assertEquals(31.0, mntg1NRLDB.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(mntg1NRLDB.getOriginal_values().getRecord_flow());
        Assert.assertEquals(34.11, mntg1NRLDB.getOriginal_values().getRecord_stage(), EPSILON);

        // Check the calculated values
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getLow_flow());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getLow_stage());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getBankfull_flow());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getBankfull_stage());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getAction_flow());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getAction_stage());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getMinor_flow());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getMinor_stage());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getModerate_flow());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getModerate_stage());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getMajor_flow());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getMajor_stage());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getRecord_flow());
        Assert.assertNull(mntg1NRLDB.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(mntg1NRLDB.getLowFlow(true, this.unitMapper));
        Assert.assertNull(mntg1NRLDB.getLowStage(true, this.unitMapper));
        Assert.assertNull(mntg1NRLDB.getBankfulFlow(true, this.unitMapper));
        Assert.assertEquals(11.0, mntg1NRLDB.getBankfulStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NRLDB.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(11.0, mntg1NRLDB.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NRLDB.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(20.0, mntg1NRLDB.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NRLDB.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(28.0, mntg1NRLDB.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NRLDB.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(31.0, mntg1NRLDB.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(mntg1NRLDB.getRecordFlow(true, this.unitMapper));
        Assert.assertEquals(34.11, mntg1NRLDB.getRecordStage(true, this.unitMapper), EPSILON);

        ThresholdDefinition blof1NWSCMS = definitionIterator.next();

        // Check the BLOF1: NWS-CMS metadata
        Assert.assertEquals("BLOF1", blof1NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", blof1NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", blof1NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", blof1NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", blof1NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", blof1NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", blof1NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", blof1NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", blof1NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", blof1NWSCMS.getThresholdProvider());
        Assert.assertTrue(blof1NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(13.0, blof1NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(17.0, blof1NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(23.5, blof1NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(26.0, blof1NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(blof1NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(blof1NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(blof1NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(blof1NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(blof1NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(blof1NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(blof1NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(13.0, blof1NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(17.0, blof1NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(23.5, blof1NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(26.0, blof1NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(blof1NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition blof1NRLDB = definitionIterator.next();

        // Check the BLOF1: NRLDB metadata
        Assert.assertEquals("BLOF1", blof1NRLDB.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", blof1NRLDB.getMetadata().getId_type());
        Assert.assertEquals("NWS-NRLDB", blof1NRLDB.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", blof1NRLDB.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", blof1NRLDB.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", blof1NRLDB.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", blof1NRLDB.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", blof1NRLDB.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", blof1NRLDB.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", blof1NRLDB.getThresholdProvider());
        Assert.assertTrue(blof1NRLDB.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(blof1NRLDB.getOriginal_values().getLow_flow());
        Assert.assertNull(blof1NRLDB.getOriginal_values().getLow_stage());
        Assert.assertNull(blof1NRLDB.getOriginal_values().getBankfull_flow());
        Assert.assertEquals(15.0, blof1NRLDB.getOriginal_values().getBankfull_stage(), EPSILON);
        Assert.assertNull(blof1NRLDB.getOriginal_values().getAction_flow());
        Assert.assertEquals(13.0, blof1NRLDB.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(blof1NRLDB.getOriginal_values().getMinor_flow());
        Assert.assertEquals(17.0, blof1NRLDB.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(blof1NRLDB.getOriginal_values().getModerate_flow());
        Assert.assertEquals(23.5, blof1NRLDB.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(blof1NRLDB.getOriginal_values().getMajor_flow());
        Assert.assertEquals(26.0, blof1NRLDB.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(blof1NRLDB.getOriginal_values().getRecord_flow());
        Assert.assertEquals(28.6, blof1NRLDB.getOriginal_values().getRecord_stage(), EPSILON);

        // Check the calculated values
        Assert.assertNull(blof1NRLDB.getCalculated_values().getLow_flow());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getLow_stage());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getBankfull_flow());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getBankfull_stage());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getAction_flow());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getAction_stage());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getMinor_flow());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getMinor_stage());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getModerate_flow());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getModerate_stage());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getMajor_flow());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getMajor_stage());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getRecord_flow());
        Assert.assertNull(blof1NRLDB.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(blof1NRLDB.getLowFlow(true, this.unitMapper));
        Assert.assertNull(blof1NRLDB.getLowStage(true, this.unitMapper));
        Assert.assertNull(blof1NRLDB.getBankfulFlow(true, this.unitMapper));
        Assert.assertEquals(15.0, blof1NRLDB.getBankfulStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NRLDB.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(13.0, blof1NRLDB.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NRLDB.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(17.0, blof1NRLDB.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NRLDB.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(23.5, blof1NRLDB.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NRLDB.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(26.0, blof1NRLDB.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(blof1NRLDB.getRecordFlow(true, this.unitMapper));
        Assert.assertEquals(28.6, blof1NRLDB.getRecordStage(true, this.unitMapper), EPSILON);

        ThresholdDefinition cedg1NWSCMS = definitionIterator.next();

        // Check the CEDG1: NWS-CMS metadata
        Assert.assertEquals("CEDG1", cedg1NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", cedg1NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", cedg1NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", cedg1NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", cedg1NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", cedg1NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", cedg1NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", cedg1NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", cedg1NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", cedg1NWSCMS.getThresholdProvider());
        Assert.assertTrue(cedg1NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(0.0, cedg1NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(0.0, cedg1NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(0.0, cedg1NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(0.0, cedg1NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(cedg1NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(cedg1NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(cedg1NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(cedg1NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(cedg1NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(cedg1NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(cedg1NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, cedg1NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, cedg1NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, cedg1NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, cedg1NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(cedg1NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(cedg1NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition smaf1NWSCMS = definitionIterator.next();

        // Check the SMAF1: NWS-CMS metadata
        Assert.assertEquals("SMAF1", smaf1NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", smaf1NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", smaf1NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", smaf1NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", smaf1NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", smaf1NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", smaf1NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", smaf1NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", smaf1NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", smaf1NWSCMS.getThresholdProvider());
        Assert.assertTrue(smaf1NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(8.0, smaf1NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(9.5, smaf1NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(11.5, smaf1NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(13.5, smaf1NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(smaf1NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(smaf1NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(smaf1NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(smaf1NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(smaf1NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(smaf1NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(smaf1NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(8.0, smaf1NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(9.5, smaf1NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(11.5, smaf1NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(13.5, smaf1NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(smaf1NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition smaf1NRLDB = definitionIterator.next();

        // Check the SMAF1: NRLDB metadata
        Assert.assertEquals("SMAF1", smaf1NRLDB.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", smaf1NRLDB.getMetadata().getId_type());
        Assert.assertEquals("NWS-NRLDB", smaf1NRLDB.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", smaf1NRLDB.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", smaf1NRLDB.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", smaf1NRLDB.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", smaf1NRLDB.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", smaf1NRLDB.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", smaf1NRLDB.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", smaf1NRLDB.getThresholdProvider());
        Assert.assertTrue(smaf1NRLDB.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getLow_flow());
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getLow_stage());
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getBankfull_flow());
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getBankfull_stage());
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getAction_flow());
        Assert.assertEquals(8.0, smaf1NRLDB.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getMinor_flow());
        Assert.assertEquals(9.5, smaf1NRLDB.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getModerate_flow());
        Assert.assertEquals(11.5, smaf1NRLDB.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getMajor_flow());
        Assert.assertEquals(13.5, smaf1NRLDB.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(smaf1NRLDB.getOriginal_values().getRecord_flow());
        Assert.assertEquals(15.36, smaf1NRLDB.getOriginal_values().getRecord_stage(), EPSILON);

        // Check the calculated values
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getLow_flow());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getLow_stage());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getBankfull_flow());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getBankfull_stage());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getAction_flow());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getAction_stage());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getMinor_flow());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getMinor_stage());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getModerate_flow());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getModerate_stage());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getMajor_flow());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getMajor_stage());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getRecord_flow());
        Assert.assertNull(smaf1NRLDB.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(smaf1NRLDB.getLowFlow(true, this.unitMapper));
        Assert.assertNull(smaf1NRLDB.getLowStage(true, this.unitMapper));
        Assert.assertNull(smaf1NRLDB.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(smaf1NRLDB.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(smaf1NRLDB.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(8.0, smaf1NRLDB.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NRLDB.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(9.5, smaf1NRLDB.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NRLDB.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(11.5, smaf1NRLDB.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NRLDB.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(13.5, smaf1NRLDB.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(smaf1NRLDB.getRecordFlow(true, this.unitMapper));
        Assert.assertEquals(15.36, smaf1NRLDB.getRecordStage(true, this.unitMapper), EPSILON);

        ThresholdDefinition chaf1NWSCMS = definitionIterator.next();

        // Check the CHAF1: NWS-CMS metadata
        Assert.assertEquals("CHAF1", chaf1NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", chaf1NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", chaf1NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", chaf1NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", chaf1NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", chaf1NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", chaf1NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", chaf1NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", chaf1NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", chaf1NWSCMS.getThresholdProvider());
        Assert.assertTrue(chaf1NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(56.0, chaf1NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(0.0, chaf1NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(0.0, chaf1NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(0.0, chaf1NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(chaf1NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(chaf1NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(chaf1NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(chaf1NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(chaf1NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(chaf1NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(chaf1NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(56.0, chaf1NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, chaf1NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, chaf1NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, chaf1NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(chaf1NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(chaf1NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition okfg1NWSCMS = definitionIterator.next();

        // Check the OKFG1: NWS-CMS metadata
        Assert.assertEquals("OKFG1", okfg1NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", okfg1NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", okfg1NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", okfg1NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", okfg1NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", okfg1NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", okfg1NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", okfg1NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", okfg1NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", okfg1NWSCMS.getThresholdProvider());
        Assert.assertTrue(okfg1NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(18.0, okfg1NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(23.0, okfg1NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(0.0, okfg1NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(0.0, okfg1NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(okfg1NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(okfg1NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(okfg1NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(okfg1NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(okfg1NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(okfg1NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(okfg1NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(18.0, okfg1NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(23.0, okfg1NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, okfg1NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, okfg1NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(okfg1NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(okfg1NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition okfg1NRLDB = definitionIterator.next();

        // Check the OKFG1: NRLDB metadata
        Assert.assertEquals("OKFG1", okfg1NRLDB.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", okfg1NRLDB.getMetadata().getId_type());
        Assert.assertEquals("NWS-NRLDB", okfg1NRLDB.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", okfg1NRLDB.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", okfg1NRLDB.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", okfg1NRLDB.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", okfg1NRLDB.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", okfg1NRLDB.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", okfg1NRLDB.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", okfg1NRLDB.getThresholdProvider());
        Assert.assertTrue(okfg1NRLDB.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getLow_flow());
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getLow_stage());
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getBankfull_flow());
        Assert.assertEquals(0.0, okfg1NRLDB.getOriginal_values().getBankfull_stage(), EPSILON);
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getAction_flow());
        Assert.assertEquals(18.0, okfg1NRLDB.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getMinor_flow());
        Assert.assertEquals(23.0, okfg1NRLDB.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getModerate_flow());
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getModerate_stage());
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getMajor_flow());
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getMajor_stage());
        Assert.assertNull(okfg1NRLDB.getOriginal_values().getRecord_flow());
        Assert.assertEquals(40.1, okfg1NRLDB.getOriginal_values().getRecord_stage(), EPSILON);

        // Check the calculated values
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getLow_flow());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getLow_stage());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getBankfull_flow());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getBankfull_stage());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getAction_flow());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getAction_stage());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getMinor_flow());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getMinor_stage());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getModerate_flow());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getModerate_stage());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getMajor_flow());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getMajor_stage());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getRecord_flow());
        Assert.assertNull(okfg1NRLDB.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(okfg1NRLDB.getLowFlow(true, this.unitMapper));
        Assert.assertNull(okfg1NRLDB.getLowStage(true, this.unitMapper));
        Assert.assertNull(okfg1NRLDB.getBankfulFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, okfg1NRLDB.getBankfulStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(okfg1NRLDB.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(18.0, okfg1NRLDB.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(okfg1NRLDB.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(23.0, okfg1NRLDB.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(okfg1NRLDB.getModerateFlow(true, this.unitMapper));
        Assert.assertNull(okfg1NRLDB.getModerateStage(true, this.unitMapper));
        Assert.assertNull(okfg1NRLDB.getMajorFlow(true, this.unitMapper));
        Assert.assertNull(okfg1NRLDB.getMajorStage(true, this.unitMapper));
        Assert.assertNull(okfg1NRLDB.getRecordFlow(true, this.unitMapper));
        Assert.assertEquals(40.1, okfg1NRLDB.getRecordStage(true, this.unitMapper), EPSILON);

        ThresholdDefinition tlpt2NWSCMS = definitionIterator.next();

        // Check the TLPT2: NWS-CMS metadata
        Assert.assertEquals("TLPT2", tlpt2NWSCMS.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", tlpt2NWSCMS.getMetadata().getId_type());
        Assert.assertEquals("NWS-CMS", tlpt2NWSCMS.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - CMS", tlpt2NWSCMS.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", tlpt2NWSCMS.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", tlpt2NWSCMS.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", tlpt2NWSCMS.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", tlpt2NWSCMS.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", tlpt2NWSCMS.getRatingProvider());
        Assert.assertEquals("NWS-CMS", tlpt2NWSCMS.getThresholdProvider());
        Assert.assertTrue(tlpt2NWSCMS.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getLow_flow());
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getLow_stage());
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getBankfull_flow());
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getBankfull_stage());
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getAction_flow());
        Assert.assertEquals(0.0, tlpt2NWSCMS.getOriginal_values().getAction_stage(), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getMinor_flow());
        Assert.assertEquals(15.0, tlpt2NWSCMS.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getModerate_flow());
        Assert.assertEquals(0.0, tlpt2NWSCMS.getOriginal_values().getModerate_stage(), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getMajor_flow());
        Assert.assertEquals(0.0, tlpt2NWSCMS.getOriginal_values().getMajor_stage(), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getRecord_flow());
        Assert.assertNull(tlpt2NWSCMS.getOriginal_values().getRecord_stage());

        // Check the calculated values
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getLow_flow());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getLow_stage());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getBankfull_flow());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getBankfull_stage());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getAction_flow());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getAction_stage());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getMinor_flow());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getMinor_stage());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getModerate_flow());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getModerate_stage());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getMajor_flow());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getMajor_stage());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getRecord_flow());
        Assert.assertNull(tlpt2NWSCMS.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(tlpt2NWSCMS.getLowFlow(true, this.unitMapper));
        Assert.assertNull(tlpt2NWSCMS.getLowStage(true, this.unitMapper));
        Assert.assertNull(tlpt2NWSCMS.getBankfulFlow(true, this.unitMapper));
        Assert.assertNull(tlpt2NWSCMS.getBankfulStage(true, this.unitMapper));
        Assert.assertNull(tlpt2NWSCMS.getActionFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, tlpt2NWSCMS.getActionStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(15.0, tlpt2NWSCMS.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getModerateFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, tlpt2NWSCMS.getModerateStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getMajorFlow(true, this.unitMapper));
        Assert.assertEquals(0.0, tlpt2NWSCMS.getMajorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(tlpt2NWSCMS.getRecordFlow(true, this.unitMapper));
        Assert.assertNull(tlpt2NWSCMS.getRecordStage(true, this.unitMapper));

        ThresholdDefinition tlpt2NRLDB = definitionIterator.next();

        // Check the TLPT2: NRLDB metadata
        Assert.assertEquals("TLPT2", tlpt2NRLDB.getMetadata().getLocation_id());
        Assert.assertEquals("NWS Station", tlpt2NRLDB.getMetadata().getId_type());
        Assert.assertEquals("NWS-NRLDB", tlpt2NRLDB.getMetadata().getThreshold_source());
        Assert.assertEquals("National Weather Service - National River Location Database", tlpt2NRLDB.getMetadata().getThreshold_source_description());
        Assert.assertEquals("NRLDB", tlpt2NRLDB.getMetadata().getRating_source());
        Assert.assertEquals("NRLDB", tlpt2NRLDB.getMetadata().getRating_source_description());
        Assert.assertEquals("FT", tlpt2NRLDB.getMetadata().getStage_unit());
        Assert.assertEquals("CFS", tlpt2NRLDB.getMetadata().getFlow_unit());
        Assert.assertEquals("NRLDB", tlpt2NRLDB.getRatingProvider());
        Assert.assertEquals("NWS-NRLDB", tlpt2NRLDB.getThresholdProvider());
        Assert.assertTrue(tlpt2NRLDB.getMetadata().getRating().isEmpty());

        // Check the original values
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getLow_flow());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getLow_stage());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getBankfull_flow());
        Assert.assertEquals(15.0, tlpt2NRLDB.getOriginal_values().getBankfull_stage(), EPSILON);
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getAction_flow());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getAction_stage());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getMinor_flow());
        Assert.assertEquals(15.0, tlpt2NRLDB.getOriginal_values().getMinor_stage(), EPSILON);
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getModerate_flow());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getModerate_stage());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getMajor_flow());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getMajor_stage());
        Assert.assertNull(tlpt2NRLDB.getOriginal_values().getRecord_flow());
        Assert.assertEquals(16.02, tlpt2NRLDB.getOriginal_values().getRecord_stage(), EPSILON);

        // Check the calculated values
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getLow_flow());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getLow_stage());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getBankfull_flow());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getBankfull_stage());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getAction_flow());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getAction_stage());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getMinor_flow());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getMinor_stage());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getModerate_flow());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getModerate_stage());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getMajor_flow());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getMajor_stage());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getRecord_flow());
        Assert.assertNull(tlpt2NRLDB.getCalculated_values().getRecord_stage());

        // Check forwarded values
        Assert.assertNull(tlpt2NRLDB.getLowFlow(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getLowStage(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getBankfulFlow(true, this.unitMapper));
        Assert.assertEquals(15.0, tlpt2NRLDB.getBankfulStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(tlpt2NRLDB.getActionFlow(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getActionStage(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getMinorFlow(true, this.unitMapper));
        Assert.assertEquals(15.0, tlpt2NRLDB.getMinorStage(true, this.unitMapper), EPSILON);
        Assert.assertNull(tlpt2NRLDB.getModerateFlow(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getModerateStage(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getMajorFlow(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getMajorStage(true, this.unitMapper));
        Assert.assertNull(tlpt2NRLDB.getRecordFlow(true, this.unitMapper));
        Assert.assertEquals(16.02, tlpt2NRLDB.getRecordStage(true, this.unitMapper), EPSILON);
    }

    @Test
    public void testReadThresholds() throws IOException {
        Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = WRDSReader.readThresholds(
                systemSettings,
                normalThresholdConfig,
                this.unitMapper,
                DESIRED_FEATURES.stream().map(WrdsLocation::getNwsLid).collect(Collectors.toSet())
        );

        Assert.assertTrue(readThresholds.containsKey(MNTG1));
        Assert.assertTrue(readThresholds.containsKey(BLOF1));
        Assert.assertTrue(readThresholds.containsKey(SMAF1));
        Assert.assertTrue(readThresholds.containsKey(OKFG1));
        Assert.assertTrue(readThresholds.containsKey(TLPT2));

        Set<ThresholdOuter> mntg1Thresholds = readThresholds.get(MNTG1);

        Assert.assertEquals(6, mntg1Thresholds.size());

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
                "minor",
                "moderate",
                "major",
                "record"
        );

        for (ThresholdOuter thresholdOuter : mntg1Thresholds) {
            String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

            Assert.assertTrue(properThresholds.contains(thresholdName));

            switch (thresholdName) {
                case "bankfull":
                    hasBankfull = true;
                    Assert.assertEquals(
                            11.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "action":
                    hasAction = true;
                    Assert.assertEquals(
                            11.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "minor":
                    hasMinor = true;
                    Assert.assertEquals(
                            20.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "moderate":
                    hasModerate = true;
                    Assert.assertEquals(
                            28.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "major":
                    hasMajor = true;
                    Assert.assertEquals(
                            31.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "record":
                    hasRecord = true;
                    Assert.assertEquals(
                            34.11,
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

        Set<ThresholdOuter> blof1Thresholds = readThresholds.get(BLOF1);

        Assert.assertEquals(6, blof1Thresholds.size());

        hasLow = false;
        hasBankfull = false;
        hasAction = false;
        hasMinor = false;
        hasModerate = false;
        hasMajor = false;
        hasRecord = false;

        properThresholds = List.of(
                "bankfull",
                "action",
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

        Set<ThresholdOuter> smaf1Thresholds = readThresholds.get(SMAF1);

        Assert.assertEquals(5, smaf1Thresholds.size());

        hasLow = false;
        hasBankfull = false;
        hasAction = false;
        hasMinor = false;
        hasModerate = false;
        hasMajor = false;
        hasRecord = false;

        properThresholds = List.of(
                "action",
                "minor",
                "moderate",
                "major",
                "record"
        );

        for (ThresholdOuter thresholdOuter : smaf1Thresholds) {
            String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

            Assert.assertTrue(properThresholds.contains(thresholdName));

            switch (thresholdName) {
                case "action":
                    hasAction = true;
                    Assert.assertEquals(
                            8.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "minor":
                    hasMinor = true;
                    Assert.assertEquals(
                            9.5,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "moderate":
                    hasModerate = true;
                    Assert.assertEquals(
                            11.5,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "major":
                    hasMajor = true;
                    Assert.assertEquals(
                            13.5,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "record":
                    hasRecord = true;
                    Assert.assertEquals(
                            15.36,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
            }
        }

        Assert.assertFalse(hasLow);
        Assert.assertFalse(hasBankfull);
        Assert.assertTrue(hasAction);
        Assert.assertTrue(hasMinor);
        Assert.assertTrue(hasModerate);
        Assert.assertTrue(hasMajor);
        Assert.assertTrue(hasRecord);

        Set<ThresholdOuter> okfg1Thresholds = readThresholds.get(OKFG1);

        Assert.assertEquals(4, okfg1Thresholds.size());

        hasLow = false;
        hasBankfull = false;
        hasAction = false;
        hasMinor = false;
        hasModerate = false;
        hasMajor = false;
        hasRecord = false;

        properThresholds = List.of(
                "bankfull",
                "action",
                "minor",
                "record"
        );

        for (ThresholdOuter thresholdOuter : okfg1Thresholds) {
            String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

            Assert.assertTrue(properThresholds.contains(thresholdName));

            switch (thresholdName) {
                case "bankfull":
                    hasBankfull = true;
                    Assert.assertEquals(
                            0.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "action":
                    hasAction = true;
                    Assert.assertEquals(
                            18.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "minor":
                    hasMinor = true;
                    Assert.assertEquals(
                            23.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "record":
                    hasRecord = true;
                    Assert.assertEquals(
                            40.1,
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
        Assert.assertFalse(hasModerate);
        Assert.assertFalse(hasMajor);
        Assert.assertTrue(hasRecord);

        Set<ThresholdOuter> tlpt2Thresholds = readThresholds.get(TLPT2);

        Assert.assertEquals(3, tlpt2Thresholds.size());

        hasLow = false;
        hasBankfull = false;
        hasAction = false;
        hasMinor = false;
        hasModerate = false;
        hasMajor = false;
        hasRecord = false;

        properThresholds = List.of(
                "bankfull",
                "minor",
                "record"
        );

        for (ThresholdOuter thresholdOuter : tlpt2Thresholds) {
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
                case "minor":
                    hasMinor = true;
                    Assert.assertEquals(
                            15.0,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
                case "record":
                    hasRecord = true;
                    Assert.assertEquals(
                            16.02,
                            thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                            EPSILON
                    );
                    break;
            }
        }

        Assert.assertFalse(hasLow);
        Assert.assertTrue(hasBankfull);
        Assert.assertFalse(hasAction);
        Assert.assertTrue(hasMinor);
        Assert.assertFalse(hasModerate);
        Assert.assertFalse(hasMajor);
        Assert.assertTrue(hasRecord);
    }
}
