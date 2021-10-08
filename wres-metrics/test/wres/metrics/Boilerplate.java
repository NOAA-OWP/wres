package wres.metrics;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

public class Boilerplate
{
    private static final FeatureKey NWS_FEATURE = new FeatureKey( "DRRC2", null, null, null );
    private static final FeatureKey USGS_FEATURE = new FeatureKey( "09165000",
                                                                   "DOLORES RIVER BELOW RICO, CO.",
                                                                   4326,
                                                                   "POINT ( -108.0603517 37.63888428 )" );
    private static final FeatureKey NWM_FEATURE = new FeatureKey( "18384141", null, null, null );
    private static final FeatureTuple FEATURE_TUPLE = new FeatureTuple( USGS_FEATURE, NWS_FEATURE, NWM_FEATURE );
    private static final FeatureGroup FEATURE_GROUP = FeatureGroup.of( FEATURE_TUPLE );

    /**
     * @return a feature group
     */

    public static FeatureGroup getFeatureGroup()
    {
        return FEATURE_GROUP;
    }

    /**
     * @return a feature tuple
     */

    public static FeatureTuple getFeatureTuple()
    {
        return FEATURE_TUPLE;
    }

    /**
     * @return some pool metadata
     */

    public static PoolMetadata getPoolMetadata()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( Boilerplate.FEATURE_GROUP,
                                          null,
                                          null,
                                          null,
                                          false );

        return PoolMetadata.of( evaluation, pool );
    }

}
