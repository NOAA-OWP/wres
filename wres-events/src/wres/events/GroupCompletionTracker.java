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
     * @throws NullPointerException if the input is null
     */

    public static GroupCompletionTracker of()
    {
        return new GroupCompletionTracker();
    }

    /**
     * Registers the publication of a message group complete.
     * 
     * @param completionState a message indicating that a group has completed.
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the message is missing expected content
     */

    public void registerPublicationComplete( EvaluationStatus completionState )
    {
        Objects.requireNonNull( completionState );

        String groupId = completionState.getGroupId();

        Objects.requireNonNull( groupId );

        if ( completionState.getCompletionStatus() != CompletionStatus.GROUP_PUBLICATION_COMPLETE )
        {
            throw new IllegalArgumentException( "While registered the completion of group " + groupId
                                                + ", received an unexpected completion "
                                                + "status  "
                                                + completionState.getCompletionStatus()
                                                + ". Expected "
                                                + CompletionStatus.GROUP_PUBLICATION_COMPLETE );
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
     * 
     * @param evaluation the evaluation to track
     */

    private GroupCompletionTracker()
    {
        this.expectedMessagesPerGroup = new ConcurrentHashMap<>();
    }

}
