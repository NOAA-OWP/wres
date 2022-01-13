package wres.metrics;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Pool;

public class Boilerplate
{
    private static final Geometry NWS_FEATURE = MessageFactory.getGeometry( "DRRC2" );
    private static final Geometry USGS_FEATURE = MessageFactory.getGeometry( "09165000",
                                                                             "DOLORES RIVER BELOW RICO, CO.",
                                                                             4326,
                                                                             "POINT ( -108.0603517 37.63888428 )" );
    private static final Geometry NWM_FEATURE = MessageFactory.getGeometry( "18384141" );
    private static final FeatureTuple FEATURE_TUPLE =
            FeatureTuple.of( MessageFactory.getGeometryTuple( USGS_FEATURE, NWS_FEATURE, NWM_FEATURE ) );
    private static final FeatureGroup FEATURE_GROUP =
            FeatureGroup.of( MessageFactory.getGeometryGroup( FEATURE_TUPLE ) );

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

        Pool pool = MessageFactory.getPool( Boilerplate.FEATURE_GROUP,
                                          null,
                                          null,
                                          null,
                                          false );

        return PoolMetadata.of( evaluation, pool );
    }

}
