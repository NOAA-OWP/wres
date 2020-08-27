package wres.events;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * <p>Used to track the completion of message groups in order to notify grouped consumers.
 * 
 * <p>The expected pattern is one instance of a {@link GroupCompletionTracker} per evaluation.
 * 
 * @author james.brown@hydrosolved.com
 */

public class GroupCompletionTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger( GroupCompletionTracker.class );

    /**
     * The expected number of messages per message group.
     */

    private final Map<String, Integer> expectedMessagesPerGroup;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static GroupCompletionTracker of()
    {
        return new GroupCompletionTracker();
    }

    /**
     * Registers the completion of a message group.
     * 
     * @param completionState a message indicating that a group has completed.
     * @param groupId the group identifier.
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the message is missing expected content
     */

    public void registerGroupComplete( EvaluationStatus completionState, String groupId )
    {
        Objects.requireNonNull( completionState );
        Objects.requireNonNull( groupId );

        if ( completionState.getCompletionStatus() != CompletionStatus.GROUP_PUBLICATION_COMPLETE_REPORTED_SUCCESS )
        {
            throw new IllegalArgumentException( "While registered the completion of group " + groupId
                                                + ", received an unexpected completion "
                                                + "status  "
                                                + completionState.getCompletionStatus()
                                                + ". Expected "
                                                + CompletionStatus.GROUP_PUBLICATION_COMPLETE_REPORTED_SUCCESS );
        }

        if ( completionState.getMessageCount() == 0 )
        {
            throw new IllegalArgumentException( "The completion status message for group " + groupId
                                                + " is missing an expected count of messages." );
        }

        int groupCount = completionState.getMessageCount();
        Integer key = this.expectedMessagesPerGroup.putIfAbsent( groupId, groupCount );

        // Register receipt of the group message
        if ( Objects.isNull( key ) )
        {
            LOGGER.debug( "Registered completion of group {}, which contained {} messages.",
                          groupId,
                          groupCount );
        }
    }

    /**
     * Returns the expected number of messages per group or null if no mapping exists.
     * 
     * @param groupId the group identifier
     * @return the expected number of messages per group
     * @throws NullPointerException if the input is null
     */

    public Integer getExpectedMessagesPerGroup( String groupId )
    {
        Objects.requireNonNull( groupId );

        return this.expectedMessagesPerGroup.get( groupId );
    }

    /**
     * Hidden constructor.
     * 
     * @param evaluationConsumerCount the number of evaluation description consumers
     * @param statisticsConsumerCount the number of statistics consumers
     * @param evaluationStatusConsumerCount the number of evaluation status consumers
     * @param statisticsGroupConsumerCount the number of consumers of grouped statistics messages
     * @param pairsConsumerCount the number of consumers of pairs messages
     * @throws IllegalArgumentException if any count is <= 0, except for grouped messages or pairs messages
     */

    private GroupCompletionTracker()
    {
        this.expectedMessagesPerGroup = new ConcurrentHashMap<>();
    }

}
