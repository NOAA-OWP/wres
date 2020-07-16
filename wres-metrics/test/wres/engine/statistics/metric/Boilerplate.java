package wres.engine.statistics.metric;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;

public class Boilerplate
{
    private static final FeatureKey NWS_FEATURE = new FeatureKey( "DRRC2", null, null, null );
    private static final FeatureKey USGS_FEATURE = new FeatureKey( "09165000", "DOLORES RIVER BELOW RICO, CO.", 4326, "POINT ( -108.0603517 37.63888428 )");
    private static final FeatureKey NWM_FEATURE = new FeatureKey( "18384141", null, null, null );
    private static final FeatureTuple FEATURE_TUPLE = new FeatureTuple( USGS_FEATURE, NWS_FEATURE, NWM_FEATURE );

    public static FeatureTuple getFeatureTuple()
    {
        return FEATURE_TUPLE;
    }

    public static SampleMetadata getSampleMetadata()
    {
        return SampleMetadata.of( MeasurementUnit.of(),
                                  DatasetIdentifier.of( FEATURE_TUPLE,
                                                        "SQIN",
                                                        "HEFS" ) );
    }
}
