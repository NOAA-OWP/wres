package wres.control;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.engine.statistics.metric.MetricProcessorByTime;

    /**
     * Task that computes a set of metric results for a particular time window.
     */
    class PairsByTimeWindowProcessor implements Supplier<MetricOutputForProjectByTimeAndThreshold>
    {
        private static final Logger LOGGER =
                LoggerFactory.getLogger( PairsByTimeWindowProcessor.class );

        /**
         * The future metric input.
         */
        private final Future<MetricInput<?>> futureInput;

        /**
         * Processor for single-valued pairs.
         */

        private final MetricProcessorByTime<SingleValuedPairs> singleValuedProcessor;

        /**
         * Processor for ensemble pairs.
         */

        private final MetricProcessorByTime<EnsemblePairs> ensembleProcessor;

        /**
         * Construct.
         * 
         * @param futureInput the future metric input
         * @param singleValuedProcessor a processor for {@link SingleValuedPairs}
         * @param ensembleProcessor a processor for {@link EnsemblePairs}
         */

        PairsByTimeWindowProcessor( final Future<MetricInput<?>> futureInput,
                                    MetricProcessorByTime<SingleValuedPairs> singleValuedProcessor,
                                    MetricProcessorByTime<EnsemblePairs> ensembleProcessor )
        {
            Objects.requireNonNull( futureInput, "Specify a non-null input for the processor." );
            this.futureInput = futureInput;
            this.singleValuedProcessor = singleValuedProcessor;
            this.ensembleProcessor = ensembleProcessor;
        }

        @Override
        public MetricOutputForProjectByTimeAndThreshold get()
        {
            MetricInput<?> input = null;
            try
            {
                input = futureInput.get();
                LOGGER.debug( "Completed processing of pairs for feature '{}' and time window {}.",
                              input.getMetadata().getIdentifier().getGeospatialID(),
                              input.getMetadata().getTimeWindow() );
            }
            catch(final InterruptedException e)
            {
                LOGGER.warn( "Interrupted while processing pairs:", e );
                Thread.currentThread().interrupt();
            }
            catch( ExecutionException e )
            {
                throw new WresProcessingException( "While processing pairs:", e );
            }
            // Process the pairs
            if ( input instanceof SingleValuedPairs )
            {
                return singleValuedProcessor.apply( (SingleValuedPairs) input );
            }
            else if ( input instanceof EnsemblePairs )
            {
                return ensembleProcessor.apply( (EnsemblePairs) input );
            }
            else
            {
                throw new WresProcessingException( "While processing pairs: encountered an unexpected type of pairs." );
            }
        }
    }
