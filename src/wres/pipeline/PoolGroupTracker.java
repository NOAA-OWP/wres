package wres.pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.events.Evaluation;

/**
 * Tracks the publication of statistics messages that belong to groups of pools and marks the publication of a group
 * complete when all statistics messages have been published for that group of pools. A group must be formally completed 
 * so that consumers can be notified that all expected statistics are ready for some grouped operation, such as the 
 * creation of a graphic from a group of pools (e.g., all lead durations).
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
     * Incrementally build an instance by adding groups.
     */

    static class Builder
    {

        /** The evaluation. */
        private Evaluation evaluation;

        /** The groups. */
        private final Map<String, Integer> groups = new HashMap<>();

        /**
         * @param groupId the group identifier
         * @param groupSize the group size
         * @return this builder
         */

        Builder addGroup( String groupId, int groupSize )
        {
            this.groups.put( groupId, groupSize );
            return this;
        }

        /**
         * @param evaluation the evaluation
         * @return this builder
         */
        Builder setEvaluation( Evaluation evaluation )
        {
            this.evaluation = evaluation;
            return this;
        }

        /**
         * @return an instance
         */

        PoolGroupTracker build()
        {
            return new PoolGroupTracker( this );
        }
        
        /**
         * Package private constructor.
         */
        Builder()
        {
        }
    }

    /**
     * @param builder the builder
     */

    private PoolGroupTracker( Builder builder )
    {
        this.evaluation = builder.evaluation;
        this.groups = new HashMap<>();
        Map<String, Integer> innerGroups = Map.copyOf( builder.groups );

        for ( Map.Entry<String, Integer> nextGroup : innerGroups.entrySet() )
        {
            String nextGroupId = nextGroup.getKey();
            Integer nextGroupSize = nextGroup.getValue();
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

        private final AtomicReference<Pair<Integer, Boolean>> publicationState;

        /**
         * Creates an instance.
         * @param groupId the group identifier
         * @param groupSize the number of messages in the group
         */

        private CompletionTracker( Evaluation evaluation,
                                   String groupId,
                                   int groupSize )
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
            UnaryOperator<Pair<Integer, Boolean>> updater = next -> {
                int newCount = next.getKey() - 1;
                boolean published = next.getRight() || actuallyPublished;
                return Pair.of( newCount, published );
            };

            Pair<Integer, Boolean> result = this.publicationState.updateAndGet( updater );

            if ( result.getLeft() == 0 && result.getRight().booleanValue() )
            {
                this.evaluation.markGroupPublicationCompleteReportedSuccess( this.groupId );

                LOGGER.debug( "Marked publication complete for message group {}.", this.groupId );
            }
        }

    }

}
