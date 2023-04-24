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
import wres.io.reading.wrds.thresholds.v2.CalculatedThresholdValues;
import wres.io.reading.wrds.thresholds.v2.OriginalThresholdValues;
import wres.io.reading.wrds.thresholds.v2.ThresholdDefinition;
import wres.io.reading.wrds.thresholds.v2.ThresholdExtractor;
import wres.io.reading.wrds.thresholds.v2.ThresholdMetadata;
import wres.io.reading.wrds.thresholds.v2.ThresholdResponse;
import wres.statistics.generated.Threshold;

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
        ptsa1NWSMetadata.setLocationId( "PTSA1" );
        ptsa1NWSMetadata.setNwsLid( "PTSA1" );
        ptsa1NWSMetadata.setUsgsSiteCode( "02372250" );
        ptsa1NWSMetadata.setNwmFeatureId( "2323396" );
        ptsa1NWSMetadata.setThresholdSource( "NWS-CMS" );
        ptsa1NWSMetadata.setStageUnit( "FT" );

        OriginalThresholdValues ptsa1NWSOriginalValues = new OriginalThresholdValues();
        ptsa1NWSOriginalValues.setLowStage( "None" );
        ptsa1NWSOriginalValues.setBankfullStage( "None" );
        ptsa1NWSOriginalValues.setActionStage( "0.0" );
        ptsa1NWSOriginalValues.setMinorStage( "0.0" );
        ptsa1NWSOriginalValues.setModerateStage( "0.0" );
        ptsa1NWSOriginalValues.setMajorStage( "0.0" );
        ptsa1NWSOriginalValues.setRecordStage( "None" );

        ThresholdDefinition ptsa1NWS = new ThresholdDefinition();
        ptsa1NWS.setMetadata( ptsa1NWSMetadata );
        ptsa1NWS.setOriginalValues( ptsa1NWSOriginalValues );
        ptsa1NWS.setCalculatedValues( new CalculatedThresholdValues() );

        ThresholdMetadata mntg1NWSMetadata = new ThresholdMetadata();
        mntg1NWSMetadata.setLocationId( "MNTG1" );
        mntg1NWSMetadata.setNwsLid( "MNTG1" );
        mntg1NWSMetadata.setUsgsSiteCode( "02349605" );
        mntg1NWSMetadata.setNwmFeatureId( "6444276" );
        mntg1NWSMetadata.setThresholdSource( "NWS-CMS" );
        mntg1NWSMetadata.setStageUnit( "FT" );

        OriginalThresholdValues mntg1NWSOriginalValues = new OriginalThresholdValues();
        mntg1NWSOriginalValues.setLowStage( "None" );
        mntg1NWSOriginalValues.setBankfullStage( "None" );
        mntg1NWSOriginalValues.setActionStage( "11.0" );
        mntg1NWSOriginalValues.setMinorStage( "20.0" );
        mntg1NWSOriginalValues.setModerateStage( "28.0" );
        mntg1NWSOriginalValues.setMajorStage( "31.0" );
        mntg1NWSOriginalValues.setRecordStage( "None" );

        ThresholdDefinition mntg1NWS = new ThresholdDefinition();
        mntg1NWS.setMetadata( mntg1NWSMetadata );
        mntg1NWS.setOriginalValues( mntg1NWSOriginalValues );
        mntg1NWS.setCalculatedValues( new CalculatedThresholdValues() );

        ThresholdMetadata mntg1NRLDBMetadata = new ThresholdMetadata();
        mntg1NRLDBMetadata.setLocationId( "MNTG1" );
        mntg1NRLDBMetadata.setNwsLid( "MNTG1" );
        mntg1NRLDBMetadata.setUsgsSiteCode( "02349605" );
        mntg1NRLDBMetadata.setNwmFeatureId( "6444276" );
        mntg1NRLDBMetadata.setThresholdSource( "NWS-NRLDB" );
        mntg1NRLDBMetadata.setStageUnit( "FT" );

        OriginalThresholdValues mntg1NRLDBOriginalValues = new OriginalThresholdValues();
        mntg1NRLDBOriginalValues.setLowStage( "None" );
        mntg1NRLDBOriginalValues.setBankfullStage( "11.0" );
        mntg1NRLDBOriginalValues.setActionStage( "11.0" );
        mntg1NRLDBOriginalValues.setMinorStage( "20.0" );
        mntg1NRLDBOriginalValues.setModerateStage( "28.0" );
        mntg1NRLDBOriginalValues.setMajorStage( "31.0" );
        mntg1NRLDBOriginalValues.setRecordStage( "34.11" );

        ThresholdDefinition mntg1NRLDB = new ThresholdDefinition();
        mntg1NRLDB.setMetadata( mntg1NRLDBMetadata );
        mntg1NRLDB.setOriginalValues( mntg1NRLDBOriginalValues );
        mntg1NRLDB.setCalculatedValues( new CalculatedThresholdValues() );

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

    }
}
