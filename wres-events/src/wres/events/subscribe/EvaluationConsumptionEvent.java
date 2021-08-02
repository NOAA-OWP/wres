package wres.events.subscribe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.Statistics;

/**
 * A custom event for monitoring the consumption and writing of statistics by format writers and exposing this 
 * information to the Java Flight Recorder. When considering throughput (bytes divided by unit time), bear in mind that
 * a consumer can only consume as quickly as a producer can produce. Also, an {@link EvaluationConsumptionEvent} covers
 * the complete lifecycle of an {@link EvaluationConsumer}, which begins before the first statistics message arrives.
 * 
 * @author james.brown@hydrosolved.com
 */

@Name( "wres.events.subscribe.EvaluationConsumptionEvent" )
@Label( "Evaluation Consumption Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Writing", "Statistics" } )
class EvaluationConsumptionEvent extends Event
{
    @Label( "Evaluation Identifier" )
    @Description( "The unique identifier of the evaluation that wrote some statistics to formats." )
    private final String evaluationId;

    @Label( "Format Types" )
    @Description( "The data formats to which statistics were written." )
    private String formats;

    @Label( "Statistics Message Count" )
    @Description( "The number of statistics messages received." )
    private int statisticsMessageCount;

    @Label( "Statistics Message Size (Total Bytes)" )
    @Description( "The total size of all statistics messages received in bytes." )
    @DataAmount( DataAmount.BYTES )
    private long statisticsMessageBytes;

    @Label( "Resource Count" )
    @Description( "The number of resources created." )
    private int resourceCount;

    @Label( "Resource Size (Total Bytes, Estimated)" )
    @DataAmount( DataAmount.BYTES )
    @Description( "The estimated size in storage of all resources created by this evaluation." )
    private long resourceBytes;

    /** Internal count of statistics messages. **/
    private final AtomicInteger statisticsMessageCountInternal;

    /** Internal count of statistics message bytes. **/
    private final AtomicLong statisticsMessageBytesInternal;

    /**
     * @param evaluationId the unique evaluation identifier, not null
     * @return an instance
     * @throws NullPointerException if the input is null
     */

    static EvaluationConsumptionEvent of( String evaluationId )
    {
        return new EvaluationConsumptionEvent( evaluationId );
    }

    /**
     * Registers a new statistics message.
     * @param statistics the statistics
     */

    void addStatistics( Statistics statistics )
    {
        if ( Objects.nonNull( statistics ) )
        {
            this.statisticsMessageCountInternal.incrementAndGet();
            this.statisticsMessageBytesInternal.addAndGet( statistics.getSerializedSize() );
        }
    }

    /**
     * Sets information about the resources created.
     * 
     * @param resources the resources
     */

    void setResources( Set<Path> resources )
    {
        if ( Objects.nonNull( resources ) )
        {
            this.resourceCount = resources.size();
            long resourceBytesLocal = 0;

            for ( Path resource : resources )
            {
                try
                {
                    resourceBytesLocal += Files.size( resource );
                }
                catch ( IOException e )
                {
                    // Do nothing.
                }
            }

            this.resourceBytes = resourceBytesLocal;
        }
    }

    /**
     * Sets the formats written.
     * @param formats the formats
     */

    void setFormats( Set<Format> formats )
    {
        if ( Objects.nonNull( formats ) )
        {
            StringJoiner joiner = new StringJoiner( ", " );
            formats.forEach( format -> joiner.add( format.toString() ) );
            this.formats = joiner.toString();
        }
    }

    /**
     * Copies the incremented state on completion to the final state, which is visible to the Java Flight Recorder.
     */
    void complete()
    {
        this.statisticsMessageCount = this.statisticsMessageCountInternal.get();
        this.statisticsMessageBytes = this.statisticsMessageBytesInternal.get();
    }

    /**
     * Hidden constructor.
     * @param evaluationId the unique evaluation identifier, not null
     * @throws NullPointerException if the input is null
     */

    private EvaluationConsumptionEvent( String evaluationId )
    {
        Objects.requireNonNull( evaluationId );

        this.evaluationId = evaluationId;
        this.statisticsMessageCountInternal = new AtomicInteger();
        this.statisticsMessageBytesInternal = new AtomicLong();
    }

}
