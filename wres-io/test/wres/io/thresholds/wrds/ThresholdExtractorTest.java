package wres.io.thresholds.wrds;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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
import wres.system.SystemSettings;

public class ThresholdExtractorTest
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

    @Before
    public void runBeforeEachTest()
    {
        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.oldResponse = this.createOldThresholdResponse();
        Mockito.when( this.unitMapper.getUnitMapper( "FT" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "CFS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "MM" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( units.toString() );
    }

    private ThresholdResponse createOldThresholdResponse()
    {
        ThresholdMetadata ptsa1NWSMetadata = new ThresholdMetadata();
        ptsa1NWSMetadata.setLocation_id( "PTSA1" );
        ptsa1NWSMetadata.setNws_lid( "PTSA1" );
        ptsa1NWSMetadata.setUsgs_site_code( "02372250" );
        ptsa1NWSMetadata.setNwm_feature_id( "2323396" );
        ptsa1NWSMetadata.setThreshold_source( "NWS-CMS" );
        ptsa1NWSMetadata.setStage_unit( "FT" );

        OriginalThresholdValues ptsa1NWSOriginalValues = new OriginalThresholdValues();
        ptsa1NWSOriginalValues.setLow_stage( "None" );
        ptsa1NWSOriginalValues.setBankfull_stage( "None" );
        ptsa1NWSOriginalValues.setAction_stage( "0.0" );
        ptsa1NWSOriginalValues.setMinor_stage( "0.0" );
        ptsa1NWSOriginalValues.setModerate_stage( "0.0" );
        ptsa1NWSOriginalValues.setMajor_stage( "0.0" );
        ptsa1NWSOriginalValues.setRecord_stage( "None" );

        ThresholdDefinition ptsa1NWS = new ThresholdDefinition();
        ptsa1NWS.setMetadata( ptsa1NWSMetadata );
        ptsa1NWS.setOriginal_values( ptsa1NWSOriginalValues );
        ptsa1NWS.setCalculated_values( new CalculatedThresholdValues() );

        ThresholdMetadata mntg1NWSMetadata = new ThresholdMetadata();
        mntg1NWSMetadata.setLocation_id( "MNTG1" );
        mntg1NWSMetadata.setNws_lid( "MNTG1" );
        mntg1NWSMetadata.setUsgs_site_code( "02349605" );
        mntg1NWSMetadata.setNwm_feature_id( "6444276" );
        mntg1NWSMetadata.setThreshold_source( "NWS-CMS" );
        mntg1NWSMetadata.setStage_unit( "FT" );

        OriginalThresholdValues mntg1NWSOriginalValues = new OriginalThresholdValues();
        mntg1NWSOriginalValues.setLow_stage( "None" );
        mntg1NWSOriginalValues.setBankfull_stage( "None" );
        mntg1NWSOriginalValues.setAction_stage( "11.0" );
        mntg1NWSOriginalValues.setMinor_stage( "20.0" );
        mntg1NWSOriginalValues.setModerate_stage( "28.0" );
        mntg1NWSOriginalValues.setMajor_stage( "31.0" );
        mntg1NWSOriginalValues.setRecord_stage( "None" );

        ThresholdDefinition mntg1NWS = new ThresholdDefinition();
        mntg1NWS.setMetadata( mntg1NWSMetadata );
        mntg1NWS.setOriginal_values( mntg1NWSOriginalValues );
        mntg1NWS.setCalculated_values( new CalculatedThresholdValues() );

        ThresholdMetadata mntg1NRLDBMetadata = new ThresholdMetadata();
        mntg1NRLDBMetadata.setLocation_id( "MNTG1" );
        mntg1NRLDBMetadata.setNws_lid( "MNTG1" );
        mntg1NRLDBMetadata.setUsgs_site_code( "02349605" );
        mntg1NRLDBMetadata.setNwm_feature_id( "6444276" );
        mntg1NRLDBMetadata.setThreshold_source( "NWS-NRLDB" );
        mntg1NRLDBMetadata.setStage_unit( "FT" );

        OriginalThresholdValues mntg1NRLDBOriginalValues = new OriginalThresholdValues();
        mntg1NRLDBOriginalValues.setLow_stage( "None" );
        mntg1NRLDBOriginalValues.setBankfull_stage( "11.0" );
        mntg1NRLDBOriginalValues.setAction_stage( "11.0" );
        mntg1NRLDBOriginalValues.setMinor_stage( "20.0" );
        mntg1NRLDBOriginalValues.setModerate_stage( "28.0" );
        mntg1NRLDBOriginalValues.setMajor_stage( "31.0" );
        mntg1NRLDBOriginalValues.setRecord_stage( "34.11" );

        ThresholdDefinition mntg1NRLDB = new ThresholdDefinition();
        mntg1NRLDB.setMetadata( mntg1NRLDBMetadata );
        mntg1NRLDB.setOriginal_values( mntg1NRLDBOriginalValues );
        mntg1NRLDB.setCalculated_values( new CalculatedThresholdValues() );

        ThresholdResponse response = new ThresholdResponse();
        response.setThresholds( List.of( ptsa1NWS, mntg1NWS, mntg1NRLDB ) );

        return response;
    }


    @Test
    public void testExtractOld()
    {
        ThresholdExtractor extractor =
                new ThresholdExtractor( oldResponse ).readStage()
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
            Assert.assertEquals( outerThreshold.getOperator(), Operator.GREATER );
            Assert.assertEquals( outerThreshold.getDataType(), ThresholdConstants.ThresholdDataType.LEFT );

            Assert.assertTrue( thresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );
            Assert.assertEquals(
                                 thresholdValues.get( outerThreshold.getThreshold().getName() ),
                                 outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                 EPSILON );
        }

    }
}
