package wres.pipeline;

import java.util.List;

import io.soabase.recordbuilder.core.RecordBuilder;

import wres.datamodel.pools.PoolRequest;
import wres.pipeline.pooling.PoolFactory;
import wres.pipeline.pooling.PoolGroupTracker;
import wres.pipeline.pooling.PoolReporter;

/**
 * Small value class to collect together variables needed to drive pool creation and execution.
 * @param poolFactory
 * @param poolRequests
 * @param poolReporter
 * @param poolGroupTracker
 *
 * @author James Brown
 */

@RecordBuilder
record PoolDetails( PoolFactory poolFactory,
                    List<PoolRequest> poolRequests,
                    PoolReporter poolReporter,
                    PoolGroupTracker poolGroupTracker )
{
}
