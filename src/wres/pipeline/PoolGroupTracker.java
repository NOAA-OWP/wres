package wres.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.events.Evaluation;
import wres.events.EvaluationEventUtilities;

/**
 * Tracks the publication of statistics messages that belong to groups of pools and marks the publication of a group
 * complete when all statistics messages have been published for that group of pools. A group must be formally completed 
 * so that consumers can be notified that all expected statistics are ready for some grouped operation, such as the 
 * creation of a graphic from a group of pools (e.g., all lead durations). The grouping logic is embedded within this
 * class and is currently based on feature groups. The API itself is more general and allows for this implementation 
 * logic to change by adding a new static constructor, akin to {@link #ofFeatureGroupTracker(List)}.
 *  
 * @author James Brown
 */

class PoolGroupTracker
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolGroupTracker.class );

    /** The message groups and their completion state. */
    private final Map<String, CompletionTracker> groups;

    /** The evaluation whose message groups should be marked complete. */
    private final Evaluation evaluation;

    /** The pool group identity associated with each {@link PoolRequest}. */
    private final Map<PoolRequest, String> groupIdentities;

    /**
     * Builds a message group tracker that tracks messages by feature group.
     * @param evaluation the evaluation
     * @param poolRequests the pool requests
     * @return the tracker instance
     * @throws NullPointerException if either input is null
     */

    static PoolGroupTracker ofFeatureGroupTracker( Evaluation evaluation, List<PoolRequest> poolRequests )
    {
        // Group the requests by feature group and then identify each group
        Map<FeatureGroup, List<PoolRequest>> groups =
                poolRequests.stream()
                            .collect( Collectors.groupingBy( e -> e.getMetadata().getFeatureGroup() ) );

        Map<PoolRequest, String> groupIdentities = new HashMap<>();

        for ( Map.Entry<FeatureGroup, List<PoolRequest>> nextEntry : groups.entrySet() )
        {
            List<PoolRequest> nextRequests = nextEntry.getValue();
            String identity = EvaluationEventUtilities.getId();
            nextRequests.forEach( next -> groupIdentities.put( next, identity ) );
        }

        return new PoolGroupTracker( evaluation, groupIdentities );
    }

    /**
     * @param poolRequest the pool request
     * @return the group identity for the pool request
     * @throws IllegalArgumentException if the pool request does not exist in this context
     */

    String getGroupId( PoolRequest poolRequest )
    {
        String identity = groupIdentities.get( poolRequest );

        if ( Objects.isNull( identity ) )
        {
            throw new IllegalArgumentException( "Could not identify a message group for the specified pool "
                                                + "request: "
                                                + poolRequest );
        }

        return identity;
    }

    /**
     * Registers a message that belongs to a group as published or otherwise complete (no data to publish).
     * @param groupId the group identifier
     * @param actuallyPublished is true if statistics were actually published
     * @throws IllegalArgumentException if the group identifier does not exist in this context
     * @throws NullPointerException if the group identifier is null
     */

    void registerPublication( String groupId, boolean actuallyPublished )
    {
        Objects.requireNonNull( groupId, "The group identifier cannot be null." );

        CompletionTracker tracker = this.groups.get( groupId );

        if ( Objects.isNull( tracker ) )
        {
            throw new IllegalArgumentException( "The group identifier '"
                                                + groupId
                                                + "' does not exist in this context." );
        }

        // Register
        tracker.registerPublication( actuallyPublished );

        LOGGER.debug( "Received a publication notice for message group '{}'.", groupId );
    }

    /**
     * @param builder the builder
     */

    private PoolGroupTracker( Evaluation evaluation, Map<PoolRequest, String> groupIdentities )
    {
        this.evaluation = evaluation;
        this.groups = new HashMap<>();
        this.groupIdentities = new HashMap<>( groupIdentities );

        // Count the number of pools per group identity
        Map<String, Long> counts = groupIdentities.values()
                                                  .stream()
                                                  .collect( Collectors.groupingBy( Function.identity(),
                                                                                   Collectors.counting() ) );

        LOGGER.debug( "Discovered the following number of pools per group identity: {}.", counts );

        for ( Map.Entry<String, Long> nextGroup : counts.entrySet() )
        {
            String nextGroupId = nextGroup.getKey();
            Long nextGroupSize = nextGroup.getValue();
            CompletionTracker nextTracker = new CompletionTracker( this.evaluation, nextGroupId, nextGroupSize );
            this.groups.put( nextGroupId, nextTracker );
        }
    }

    /**
     * Small class that monitors the completion state of a group and triggers its completion with the registered
     * evaluation when complete.
     */

    private static class CompletionTracker
    {
        /** Logger.*/
        private static final Logger LOGGER = LoggerFactory.getLogger( CompletionTracker.class );

        /** The evaluation whose group should be completed. */
        private final Evaluation evaluation;

        /** The group identifier. */
        private final String groupId;

        /** 
         * The number of messages published and whether statistics were actually published for one or more groups or
         * all groups contained no statistics. These two things must be updated atomically. When the count reaches zero
         * and the flag is true, group completion occurs.
         */

        private final AtomicReference<Pair<Long, Boolean>> publicationState;

        /**
         * Creates an instance.
         * @param groupId the group identifier
         * @param groupSize the number of messages in the group
         */

        private CompletionTracker( Evaluation evaluation,
                                   String groupId,
                                   long groupSize )
        {
            Objects.requireNonNull( groupId );

            if ( groupSize <= 0 )
            {
                throw new IllegalArgumentException( "The message group '" + groupId + "' cannot have zero members." );
            }

            this.groupId = groupId;
            this.publicationState = new AtomicReference<>( Pair.of( groupSize, false ) );
            this.evaluation = evaluation;
        }

        /**
         * Registers a message that belongs to a group as published or otherwise complete (no data to publish).
         * @param actuallyPublished is true if statistics were actually published
         */

        private void registerPublication( boolean actuallyPublished )
        {
            UnaryOperator<Pair<Long, Boolean>> updater = next -> {
                long newCount = next.getKey() - 1;
                boolean published = next.getRight() || actuallyPublished;
                return Pair.of( newCount, published );
            };

            Pair<Long, Boolean> result = this.publicationState.updateAndGet( updater );

            if ( result.getLeft() == 0 && result.getRight().booleanValue() )
            {
                this.evaluation.markGroupPublicationCompleteReportedSuccess( this.groupId );

                LOGGER.debug( "Marked publication complete for message group {}.", this.groupId );
            }
        }

    }

}
