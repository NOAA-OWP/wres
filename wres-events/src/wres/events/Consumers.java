package wres.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Evaluation;

/**
 * Bag of consumers to consume the messages associated with one evaluation.
 * 
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class Consumers
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Consumers.class );

    /**
     * Consumers of evaluation events, which contain descriptions of evaluations to conduct.
     */

    private final List<Consumer<Evaluation>> evaluationConsumers;

    /**
     * Consumers of evaluation status events, which contain information about the status of an evaluation and 
     * associated user-facing messages (as distinct from developer-facing, which are logged).
     */

    private final List<Consumer<EvaluationStatus>> statusConsumers;


    /**
     * Consumers of evaluation statistics.
     */

    private final List<Consumer<Statistics>> statisticsConsumers;

    /**
     * Consumers of groups of evaluation statistics where consumption is triggered by group.
     */

    private final List<OneGroupConsumer<Statistics>> groupedStatisticsConsumers;

    /**
     * @return the evaluation consumers
     */
    List<Consumer<Evaluation>> getEvaluationConsumers()
    {
        return this.evaluationConsumers; //Immutable on construction
    }

    /**
     * @return the evaluation status consumers
     */
    List<Consumer<EvaluationStatus>> getEvaluationStatusConsumers()
    {
        return this.statusConsumers; //Immutable on construction
    }

    /**
     * @return the statistics consumers
     */
    List<Consumer<Statistics>> getStatisticsConsumers()
    {
        return this.statisticsConsumers; //Immutable on construction
    }

    /**
     * @return the consumers of statistics groups
     */
    List<OneGroupConsumer<Statistics>> getGroupedStatisticsConsumers()
    {
        return this.groupedStatisticsConsumers; //Immutable on construction
    }

    /**
     * Builder.
     */

    public static class Builder
    {

        /**
         * Consumers of evaluation events, which contain descriptions of evaluations to conduct.
         */

        private List<Consumer<Evaluation>> evaluationConsumers = new ArrayList<>();

        /**
         * Consumers of evaluation status events, which contain information about the status of an evaluation and 
         * associated user-facing messages (as distinct from developer-facing, which are logged).
         */

        private List<Consumer<EvaluationStatus>> statusConsumers = new ArrayList<>();

        /**
         * Consumers of evaluation statistics.
         */

        private List<Consumer<Statistics>> statisticsConsumers = new ArrayList<>();

        /**
         * Consumers of groups of evaluation statistics.
         */

        private List<OneGroupConsumer<Statistics>> groupedStatisticsConsumers = new ArrayList<>();

        /**
         * @param evaluationConsumers the evaluation consumers to set
         * @return this builder
         */
        public Builder setEvaluationConsumers( List<Consumer<Evaluation>> evaluationConsumers )
        {
            this.evaluationConsumers = evaluationConsumers;
            return this;
        }

        /**
         * @param statusConsumers the status consumers to set
         * @return this builder
         */
        public Builder setStatusConsumers( List<Consumer<EvaluationStatus>> statusConsumers )
        {
            this.statusConsumers = statusConsumers;
            return this;
        }

        /**
         * @param statisticsConsumers the statistics consumers to set#
         * @return this builder
         */
        public Builder setStatisticsConsumers( List<Consumer<Statistics>> statisticsConsumers )
        {
            this.statisticsConsumers = statisticsConsumers;
            return this;
        }

        /**
         * @param groupedStatisticsConsumers the statistics consumers to set
         * @return this builder
         */
        public Builder setGroupedStatisticsConsumers( List<OneGroupConsumer<Statistics>> groupedStatisticsConsumers )
        {
            this.groupedStatisticsConsumers = groupedStatisticsConsumers;
            return this;
        }

        /**
         * @param evaluationConsumer the evaluation consumer to add
         * @return this builder
         */
        public Builder addEvaluationConsumer( Consumer<Evaluation> evaluationConsumer )
        {
            this.evaluationConsumers.add( evaluationConsumer );
            return this;
        }

        /**
         * @param statusConsumer the status consumer to add
         * @return this builder
         */
        public Builder addStatusConsumer( Consumer<EvaluationStatus> statusConsumer )
        {
            this.statusConsumers.add( statusConsumer );
            return this;
        }

        /**
         * @param statisticsConsumer the statistics consumer to add
         * @return this builder
         */
        public Builder addStatisticsConsumer( Consumer<Statistics> statisticsConsumer )
        {
            this.statisticsConsumers.add( statisticsConsumer );
            return this;
        }

        /**
         * @param statisticsConsumer the statistics consumer to add
         * @return this builder
         */
        public Builder addGroupedStatisticsConsumer( OneGroupConsumer<Statistics> statisticsConsumer )
        {
            this.groupedStatisticsConsumers.add( statisticsConsumer );
            return this;
        }

        /**
         * Builds a consumer group.
         * 
         * @return a consumer group
         */

        public Consumers build()
        {
            return new Consumers( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */
    private Consumers( Builder builder )
    {
        this.evaluationConsumers = Collections.unmodifiableList( builder.evaluationConsumers );
        this.statisticsConsumers = Collections.unmodifiableList( builder.statisticsConsumers );
        this.statusConsumers = Collections.unmodifiableList( builder.statusConsumers );
        this.groupedStatisticsConsumers = Collections.unmodifiableList( builder.groupedStatisticsConsumers );

        Objects.requireNonNull( this.evaluationConsumers );
        Objects.requireNonNull( this.statisticsConsumers );
        Objects.requireNonNull( this.statusConsumers );

        LOGGER.debug( "Successfully constructed a consumer group with {} evaluation consumers, {} evaluation status "
                      + "consumers and {} statistics consumers.",
                      this.getEvaluationConsumers().size(),
                      this.getEvaluationStatusConsumers().size(),
                      this.getStatisticsConsumers().size() );
    }

}
