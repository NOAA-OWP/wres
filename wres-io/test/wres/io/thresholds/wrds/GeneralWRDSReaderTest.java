package wres.io.thresholds.wrds;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import wres.config.generated.*;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.wrds.v3.GeneralThresholdDefinition;
import wres.io.thresholds.wrds.v3.GeneralThresholdResponse;
import wres.system.SystemSettings;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class GeneralWRDSReaderTest
{
    //The file used is for the old API.  URL unknown.
    private static final URI oldPath = URI.create( "testinput/thresholds/wrds/thresholds.json" );
    
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

    private static WrdsLocation createFeature( final String featureId, final String usgsSiteCode, final String lid )
    {
        return new WrdsLocation( featureId, usgsSiteCode, lid );
    }

    private static final WrdsLocation PTSA1 = GeneralWRDSReaderTest.createFeature( "2323396", "02372250", "PTSA1" );
    private static final WrdsLocation MNTG1 = GeneralWRDSReaderTest.createFeature( "6444276", "02349605", "MNTG1" );
    private static final WrdsLocation BLOF1 = GeneralWRDSReaderTest.createFeature( "2297254", "02358700", "BLOF1" );
    private static final WrdsLocation CEDG1 = GeneralWRDSReaderTest.createFeature( "2310009", "02343940", "CEDG1" );
    private static final WrdsLocation SMAF1 = GeneralWRDSReaderTest.createFeature( "2298964", "02359170", "SMAF1" );
    private static final WrdsLocation CHAF1 = GeneralWRDSReaderTest.createFeature( "2293124", "02358000", "CHAF1" );
    private static final WrdsLocation OKFG1 = GeneralWRDSReaderTest.createFeature( "6447636", "02350512", "OKFG1" );
    private static final WrdsLocation TLPT2 = GeneralWRDSReaderTest.createFeature( "13525368", "07311630", "TLPT2" );
    private static final WrdsLocation NUTF1 = GeneralWRDSReaderTest.createFeature( null, null, "NUTF1" );
    private static final WrdsLocation CDRA1 = GeneralWRDSReaderTest.createFeature( null, null, "CDRA1" );
    private static final WrdsLocation MUCG1 = GeneralWRDSReaderTest.createFeature( null, null, "MUCG1" );
    private static final WrdsLocation PRSG1 = GeneralWRDSReaderTest.createFeature( null, null, "PRSG1" );
    private static final WrdsLocation LSNO2 = GeneralWRDSReaderTest.createFeature( null, null, "LSNO2" );
    private static final WrdsLocation HDGA4 = GeneralWRDSReaderTest.createFeature( null, null, "HDGA4" );
    private static final WrdsLocation FAKE3 = GeneralWRDSReaderTest.createFeature( null, null, "FAKE3" );
    private static final WrdsLocation CNMP1 = GeneralWRDSReaderTest.createFeature( null, null, "CNMP1" );
    private static final WrdsLocation WLLM2 = GeneralWRDSReaderTest.createFeature( null, null, "WLLM2" );
    private static final WrdsLocation RCJD2 = GeneralWRDSReaderTest.createFeature( null, null, "RCJD2" );
    private static final WrdsLocation MUSM5 = GeneralWRDSReaderTest.createFeature( null, null, "MUSM5" );
    private static final WrdsLocation DUMM5 = GeneralWRDSReaderTest.createFeature( null, null, "DUMM5" );
    private static final WrdsLocation DMTM5 = GeneralWRDSReaderTest.createFeature( null, null, "DMTM5" );
    private static final WrdsLocation PONS2 = GeneralWRDSReaderTest.createFeature( null, null, "PONS2" );
    private static final WrdsLocation MCKG1 = GeneralWRDSReaderTest.createFeature( null, null, "MCKG1" );
    private static final WrdsLocation DSNG1 = GeneralWRDSReaderTest.createFeature( null, null, "DSNG1" );
    private static final WrdsLocation BVAW2 = GeneralWRDSReaderTest.createFeature( null, null, "BVAW2" );
    private static final WrdsLocation CNEO2 = GeneralWRDSReaderTest.createFeature( null, null, "CNEO2" );
    private static final WrdsLocation CMKT2 = GeneralWRDSReaderTest.createFeature( null, null, "CMKT2" );
    private static final WrdsLocation BDWN6 = GeneralWRDSReaderTest.createFeature( null, null, "BDWN6" );
    private static final WrdsLocation CFBN6 = GeneralWRDSReaderTest.createFeature( null, null, "CFBN6" );
    private static final WrdsLocation CCSA1 = GeneralWRDSReaderTest.createFeature( null, null, "CCSA1" );
    private static final WrdsLocation LGNN8 = GeneralWRDSReaderTest.createFeature( null, null, "LGNN8" );
    private static final WrdsLocation BCLN7 = GeneralWRDSReaderTest.createFeature( null, null, "BCLN7" );
    private static final WrdsLocation KERV2 = GeneralWRDSReaderTest.createFeature( null, null, "KERV2" );
    private static final WrdsLocation ARDS1 = GeneralWRDSReaderTest.createFeature( null, null, "ARDS1" );
    private static final WrdsLocation WINW2 = GeneralWRDSReaderTest.createFeature( null, null, "WINW2" );
    private static final WrdsLocation SRDN5 = GeneralWRDSReaderTest.createFeature( null, null, "SRDN5" );
    private static final WrdsLocation MNTN1 = GeneralWRDSReaderTest.createFeature( null, null, "MNTN1" );
    private static final WrdsLocation GNSW4 = GeneralWRDSReaderTest.createFeature( null, null, "GNSW4" );
    private static final WrdsLocation JAIO1 = GeneralWRDSReaderTest.createFeature( null, null, "JAIO1" );
    private static final WrdsLocation INCO1 = GeneralWRDSReaderTest.createFeature( null, null, "INCO1" );
    private static final WrdsLocation PRMO1 = GeneralWRDSReaderTest.createFeature( null, null, "PRMO1" );
    private static final WrdsLocation PARO1 = GeneralWRDSReaderTest.createFeature( null, null, "PARO1" );
    private static final WrdsLocation BRCO1 = GeneralWRDSReaderTest.createFeature( null, null, "BRCO1" );
    private static final WrdsLocation WRNO1 = GeneralWRDSReaderTest.createFeature( null, null, "WRNO1" );
    private static final WrdsLocation BLEO1 = GeneralWRDSReaderTest.createFeature( null, null, "BLEO1" );

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
                                                                        BLEO1 );

    private UnitMapper unitMapper;


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
                                                                                                                      null, //null ratings provider.
                                                                                                                      null,
                                                                                                                      LeftOrRightOrBaseline.LEFT ),
                                                                                         ThresholdOperator.GREATER_THAN );

    private static final ThresholdsConfig normalThresholdConfig = new ThresholdsConfig(
                                                                                        ThresholdType.VALUE,
                                                                                        ThresholdDataType.LEFT,
                                                                                        new ThresholdsConfig.Source(
                                                                                                                     path,
                                                                                                                     ThresholdFormat.WRDS,
                                                                                                                     null,
                                                                                                                     null,
                                                                                                                     "NWS-NRLDB",
                                                                                                                     null, //null ratings provider.
                                                                                                                     "stage",
                                                                                                                     LeftOrRightOrBaseline.LEFT ),
                                                                                        ThresholdOperator.GREATER_THAN );


    private static final ThresholdsConfig oldNormalThresholdConfig = new ThresholdsConfig(
            ThresholdType.VALUE,
            ThresholdDataType.LEFT,
            new ThresholdsConfig.Source(
                    oldPath,
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

    private SystemSettings systemSettings;

    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    @Before
    public void runBeforeEachTest()
    {
        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.systemSettings = SystemSettings.withDefaults();
        Mockito.when( this.unitMapper.getUnitMapper( "FT" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "CFS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "MM" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( units.toString() );
    }

    @Test
    public void testGroupLocations()
    {
        Set<String> desiredFeatures =
                DESIRED_FEATURES.stream().map( WrdsLocation::getNwsLid ).collect( Collectors.toSet() );
        Set<String> groupedLocations = WRDSReader.groupLocations( desiredFeatures );
        Assert.assertEquals( groupedLocations.size(), 3 );

        StringJoiner firstGroupBuilder = new StringJoiner( "," );
        StringJoiner secondGroupBuilder = new StringJoiner( "," );
        StringJoiner thirdGroupBuilder = new StringJoiner( "," );


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
    public void testGetResponse() throws IOException
    {
        GeneralWRDSReader reader = new GeneralWRDSReader( systemSettings );

        byte[] responseBytes = reader.getResponse( path );
        GeneralThresholdResponse response =
                JSON_OBJECT_MAPPER.readValue( responseBytes, GeneralThresholdResponse.class );

        Assert.assertEquals( 10, response.getThresholds().size() );
        Iterator<GeneralThresholdDefinition> iterator = response.getThresholds().iterator();

        GeneralThresholdDefinition activeCheckedThresholds = iterator.next();

        //==== PTSA1 is first.  This is a mostly empty set of thresholds.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source() );
        Assert.assertEquals( "National Weather Service - National River Location Database",
                             activeCheckedThresholds.getMetadata().getThreshold_source_description() );
        Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStage_units() );
        Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlow_units() );
        Assert.assertEquals( "NRLDB", activeCheckedThresholds.getRatingProvider() );
        Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getThresholdProvider() );
        Assert.assertEquals( "PTSA1",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getLocation_id() );
        Assert.assertEquals( "NWS Station",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getId_type() );
        Assert.assertEquals( "National Weather Service - National River Location Database",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getDescription() );
        Assert.assertEquals( "NRLDB", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getSource() );

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
        expectedThresholdValues.put( "low", 0.0 );

        for ( ThresholdOuter outerThreshold : thresholds )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        activeCheckedThresholds = iterator.next();

        //==== PTSA1 is second.  On difference: rating info.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source() );
        Assert.assertEquals( "National Weather Service - National River Location Database",
                             activeCheckedThresholds.getMetadata().getThreshold_source_description() );
        Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStage_units() );
        Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlow_units() );
        Assert.assertEquals( "USGS Rating Depot", activeCheckedThresholds.getRatingProvider() );
        Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getThresholdProvider() );
        Assert.assertEquals( "02372250",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getLocation_id() );
        Assert.assertEquals( "USGS Gage",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getId_type() );
        Assert.assertEquals( "The EXSA rating curves provided by USGS",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getDescription() );
        Assert.assertEquals( "USGS Rating Depot",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getSource() );

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
        expectedThresholdValues.put( "low", 0.0 );

        for ( ThresholdOuter outerThreshold : thresholds )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        activeCheckedThresholds = iterator.next();

        //==== MTNG1 is third.  On difference: rating info.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source() );
        Assert.assertEquals( "National Weather Service - National River Location Database",
                             activeCheckedThresholds.getMetadata().getThreshold_source_description() );
        Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStage_units() );
        Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlow_units() );
        Assert.assertEquals( "NRLDB", activeCheckedThresholds.getRatingProvider() );

        //Check the values with calculated flow included.
        results = activeCheckedThresholds.getThresholds(
                                                         WRDSThresholdType.FLOW,
                                                         Operator.GREATER,
                                                         ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                         true,
                                                         this.unitMapper );

        thresholds = results.values().iterator().next();
        expectedThresholdValues = new HashMap<>();
        expectedThresholdValues.put( "low", 557.8 );
        expectedThresholdValues.put( "bankfull", 9379.0 );
        expectedThresholdValues.put( "action", 9379.0 );
        expectedThresholdValues.put( "flood", 35331.0 );
        expectedThresholdValues.put( "minor", 35331.0 );
        expectedThresholdValues.put( "moderate", 102042.0 );
        expectedThresholdValues.put( "major", 142870.0 );
        expectedThresholdValues.put( "record", 136000.0 );

        for ( ThresholdOuter outerThreshold : thresholds )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        activeCheckedThresholds = iterator.next(); //Skip the 4th. 
        activeCheckedThresholds = iterator.next(); //Frankly, I'm not even sure we need to check the 5th.

        //==== BLOF1 is fifth.  On difference: rating info.
        //Check the PTSA1: NWS-CMS metadata
        Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThreshold_source() );
        Assert.assertEquals( "National Weather Service - National River Location Database",
                             activeCheckedThresholds.getMetadata().getThreshold_source_description() );
        Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStage_units() );
        Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlow_units() );
        Assert.assertEquals( "NRLDB", activeCheckedThresholds.getRatingProvider() );
        Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getThresholdProvider() );
        Assert.assertEquals( "BLOF1", activeCheckedThresholds.getMetadata().getNws_lid() );
        Assert.assertEquals( "all (stage,flow)", activeCheckedThresholds.getMetadata().getThreshold_type() );
        Assert.assertEquals( "BLOF1",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getLocation_id() );
        Assert.assertEquals( "NWS Station",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getId_type() );
        Assert.assertEquals( "National Weather Service - National River Location Database",
                             activeCheckedThresholds.getCalc_flow_values().getRating_curve().getDescription() );
        Assert.assertEquals( "NRLDB", activeCheckedThresholds.getCalc_flow_values().getRating_curve().getSource() );

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
        expectedThresholdValues.put( "bankfull", 15.0 );
        expectedThresholdValues.put( "action", 13.0 );
        expectedThresholdValues.put( "flood", 17.0 );
        expectedThresholdValues.put( "minor", 17.0 );
        expectedThresholdValues.put( "moderate", 23.5 );
        expectedThresholdValues.put( "major", 26.0 );
        expectedThresholdValues.put( "record", 28.6 );

        for ( ThresholdOuter outerThreshold : thresholds )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
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
        expectedThresholdValues.put( "bankfull", 38633.0 );
        expectedThresholdValues.put( "action", 31313.0 );
        expectedThresholdValues.put( "flood", 48628.0 );
        expectedThresholdValues.put( "minor", 48628.0 );
        expectedThresholdValues.put( "moderate", 144077.0 );
        expectedThresholdValues.put( "major", 216266.0 );
        expectedThresholdValues.put( "record", 209000.0 );

        for ( ThresholdOuter outerThreshold : thresholds )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
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
        expectedThresholdValues.put( "flood", 36900.0 );
        ;
        expectedThresholdValues.put( "record", 209000.0 );

        for ( ThresholdOuter outerThreshold : thresholds )
        {
            Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                 outerThreshold.getDataType() );
            Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
            Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

            Assert.assertEquals(
                                 expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

        //I believe additional testing of the remaining thresholds is unnecessary.
    }

    @Test
    public void testReadThresholds() throws IOException
    {
        Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds(
                                                                                                  systemSettings,
                                                                                                  normalThresholdConfig,
                                                                                                  this.unitMapper,
                                                                                                  DESIRED_FEATURES.stream()
                                                                                                                  .map( WrdsLocation::getNwsLid )
                                                                                                                  .collect( Collectors.toSet() ) );

        Assert.assertTrue( readThresholds.containsKey( PTSA1 ) );
        Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
        Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
        Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
        Assert.assertTrue( readThresholds.containsKey( CEDG1 ) );

        //The two low thresholds available are identical in both label and value, so only one is included.
        Set<ThresholdOuter> ptsa1Thresholds = readThresholds.get( PTSA1 );
        Assert.assertEquals( 1, ptsa1Thresholds.size() );

        Set<ThresholdOuter> blof1Thresholds = readThresholds.get( BLOF1 );
        Assert.assertEquals( 7, blof1Thresholds.size() );

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
                                                 "record" );

        for ( ThresholdOuter thresholdOuter : blof1Thresholds )
        {
            String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

            Assert.assertTrue( properThresholds.contains( thresholdName ) );

            switch ( thresholdName )
            {
                case "bankfull":
                    hasBankfull = true;
                    Assert.assertEquals(
                                         15.0,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "action":
                    hasAction = true;
                    Assert.assertEquals(
                                         13.0,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "flood":
                    hasMinor = true;
                    Assert.assertEquals(
                                         17.0,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "minor":
                    hasMinor = true;
                    Assert.assertEquals(
                                         17.0,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "moderate":
                    hasModerate = true;
                    Assert.assertEquals(
                                         23.5,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "major":
                    hasMajor = true;
                    Assert.assertEquals(
                                         26.0,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "record":
                    hasRecord = true;
                    Assert.assertEquals(
                                         28.6,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
            }
        }

        Assert.assertFalse( hasLow );
        Assert.assertTrue( hasBankfull );
        Assert.assertTrue( hasAction );
        Assert.assertTrue( hasMinor );
        Assert.assertTrue( hasModerate );
        Assert.assertTrue( hasMajor );
        Assert.assertTrue( hasRecord );
    }

    @Test
    public void testReadRecurrenceFlows() throws IOException
    {
        Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds(
                                                                                                  systemSettings,
                                                                                                  normalRecurrenceConfig,
                                                                                                  this.unitMapper,
                                                                                                  DESIRED_FEATURES.stream()
                                                                                                                  .map( WrdsLocation::getNwsLid )
                                                                                                                  .collect( Collectors.toSet() ) );

        Assert.assertTrue( readThresholds.containsKey( PTSA1 ) );
        Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
        Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
        Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
        Assert.assertTrue( readThresholds.containsKey( CEDG1 ) );


        Set<ThresholdOuter> blof1Thresholds = readThresholds.get( BLOF1 );
        Assert.assertEquals( 6, blof1Thresholds.size() );

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
                                                 "year_10_0" );

        for ( ThresholdOuter thresholdOuter : blof1Thresholds )
        {
            String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

            Assert.assertTrue( properThresholds.contains( thresholdName ) );

            switch ( thresholdName )
            {
                case "year_1_5":
                    has1_5 = true;
                    Assert.assertEquals(
                                         58864.26,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "year_2_0":
                    has2_0 = true;
                    Assert.assertEquals(
                                         87362.48,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "year_3_0":
                    has3_0 = true;
                    Assert.assertEquals(
                                         109539.05,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "year_4_0":
                    has4_0 = true;
                    Assert.assertEquals(
                                         128454.64,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "year_5_0":
                    has5_0 = true;
                    Assert.assertEquals(
                                         176406.6,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
                case "year_10_0":
                    has10_0 = true;
                    Assert.assertEquals(
                                         216831.58000000002,
                                         thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                         EPSILON );
                    break;
            }
        }

        Assert.assertTrue( has1_5 );
        Assert.assertTrue( has2_0 );
        Assert.assertTrue( has3_0 );
        Assert.assertTrue( has4_0 );
        Assert.assertTrue( has5_0 );
        Assert.assertTrue( has10_0 );
    }
    
    @Test
    public void testReadOldThresholds() throws IOException {
        
        Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds(
                                                                                                 systemSettings,
                                                                                                 oldNormalThresholdConfig,
                                                                                                 this.unitMapper,
                                                                                                 DESIRED_FEATURES.stream()
                                                                                                                 .map( WrdsLocation::getNwsLid )
                                                                                                                 .collect( Collectors.toSet() ) );

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
