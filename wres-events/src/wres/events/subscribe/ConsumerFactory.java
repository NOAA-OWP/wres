package wres.events.subscribe;

import java.nio.file.Path;

import wres.statistics.generated.Consumer;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Statistics;

/**
 * <p>An interface that supplies consumers for evaluation statistics. The interface supplies two types of consumers:
 * 
 * <ol>
 * <li>Consumers that consume one pool of statistics at a time. In other words, consumption happens immediately.</li>
 * <li>Consumers that consume multiple pools of statistics associated with a message group, such as a geographic 
 * feature. In other words, consumption is deferred until all statistics are available.</li>
 * </ol>
 * 
 * <p>A consumer is supplied at evaluation time based on an {@link Evaluation} description. Consumers are supplied at 
 * evaluation time because the consumers may depend on the evaluation description, either to determine consumers 
 * required or to use the description of the evaluation to qualify the statistics consumed.
 * 
 * <p>Each consumer consumes a collection of {@link Statistics} and returns a set of {@link Path} mutated. The consumer
 * may contain one or more underlying consumers that each consume the same statistics.
 *
 * <p>Callers are responsible for closing {@link StatisticsConsumer} created by this factory.
 * 
 * <p><b>Implementation notes:</b>
 * 
 * <p>At least one of the methods of this interface should return a non-trivial consumer. Both methods should return a
 * non-trivial consumer if both styles of consumption are required (incremental and grouped). For example, if the 
 * consumption abstracts a single consumer and that consumer writes to a numerical format whereby each pool of 
 * statistics can be written as it arrives, then the {@link #getConsumer(Evaluation, Path)} may return a
 * trivial consumer as follows:
 * 
 * <p><code>return statistics {@literal ->} Set.of();</code>
 * 
 * @author James Brown
 */

public interface ConsumerFactory
{
    /**
     * Creates a consumer for a given evaluation description. An ordinary consumer writes a statistics message as soon 
     * as it arrives.
     * 
     * @param evaluation the evaluation description
     * @param path the path to which outputs should be written by a consumer
     * @return a consumer
     * @throws ConsumerException if the consumer could not be created for any reason
     */

    StatisticsConsumer getConsumer( Evaluation evaluation, Path path );

    /**
     * Creates a consumer of grouped statistics for a given evaluation description. A grouped consumer delays writing 
     * until all messages have been received for a message group, which contains a collection of statistics.
     * 
     * @param evaluation the evaluation description
     * @param path the path to which outputs should be written by a consumer
     * @return a grouped consumer
     * @throws ConsumerException if the consumer could not be created for any reason
     */

    StatisticsConsumer getGroupedConsumer( Evaluation evaluation, Path path );
    
    /**
     * Returns a basic description of the consumers that are created by this factory, including the formats they offer.
     * 
     * @return the consumer description
     */

    Consumer getConsumerDescription();
}
