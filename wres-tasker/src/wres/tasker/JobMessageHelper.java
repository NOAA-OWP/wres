package wres.tasker;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobMessageHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobMessageHelper.class );

    /** A magic value placeholder for job registered-but-not-done */
    static final Integer JOB_NOT_DONE_YET = Integer.MIN_VALUE + 1;

    private static final int JOB_WAIT_SECONDS = 10;
    private static final int MESSAGE_WAIT_SECONDS = 5;

    /**
     * Wait for messages from rabbitmq client related to a job
     * Caller's responsibility to match the type of Consumer with BlockingQueue
     * @param queueName the queue name to look in
     * @param messageQueue the synchronizer to use to talk to q client
     * @param messageSharer sink that saves to shared collection
     * @throws InterruptedException when talking to q client is interrupted
     */

    static void waitForAllMessages( String queueName,
                                    String jobId,
                                    BlockingQueue<? extends GeneratedMessageV3> messageQueue,
                                    Consumer<GeneratedMessageV3> messageSharer,
                                    String messageType )
            throws InterruptedException
    {
        boolean timedOut = false;

        while ( !timedOut )
        {
            LOGGER.debug( "Consuming from {}, waiting for result.", queueName );

            // One call to .basicConsume can result in many messages
            // being received by our jobOutputReceiver. Look for them.
            // This still seems uncertain and finnicky, but works?
            boolean mayBeMoreMessages = true;

            while ( mayBeMoreMessages )
            {
                GeneratedMessageV3 oneMoreLine
                        = messageQueue.poll( MESSAGE_WAIT_SECONDS, TimeUnit.SECONDS );

                if ( oneMoreLine != null )
                {
                    messageSharer.accept( oneMoreLine );
                }
                else
                {
                    mayBeMoreMessages = false;
                }
            }

            // Give up waiting if we don't get any output for some time
            // after the job has completed.
            GeneratedMessageV3 oneLastLine =
                    messageQueue.poll( JOB_WAIT_SECONDS, TimeUnit.SECONDS );

            if ( oneLastLine != null )
            {
                messageSharer.accept( oneLastLine );
            }
            else
            {
                // Has the job actually finished?
                Integer jobStatus = JobResults.getJobResultRaw( jobId );

                if ( Objects.isNull( jobStatus ) )
                {
                    timedOut = true;
                    LOGGER.warn( "Stopped waiting for not-found job {} {}",
                                 jobId, messageType );
                }
                else if ( !jobStatus.equals( JOB_NOT_DONE_YET ) )
                {
                    timedOut = true;
                    LOGGER.info( "Finished waiting for job {} {}",
                                 jobId, messageType );
                }
                else
                {
                    timedOut = false;
                    LOGGER.info( "Still waiting for job {} {}",
                                 jobId, messageType );
                }
            }
        }
    }
}
