package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Abstract base class for timing error metrics.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class TimingError implements Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
{

    /**
     * The data factory.
     */

    private final DataFactory dataFactory;
    
    @Override
    public DataFactory getDataFactory()
    {
        return dataFactory;
    }

    @Override
    public String toString()
    {
        return getID().toString();
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public abstract static class TimingErrorBuilder
            implements MetricBuilder<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
    {

        /**
         * The data factory.
         */

        private DataFactory dataFactory;

        /**
         * Sets the {@link DataFactory} for constructing a {@link MetricOutput}.
         * 
         * @param dataFactory the {@link DataFactory}
         * @return the builder
         */

        @Override
        public TimingErrorBuilder setOutputFactory( final DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    protected TimingError( final TimingErrorBuilder builder ) throws MetricParameterException
    {
        if ( Objects.isNull( builder ) )
        {
            throw new MetricParameterException( "Cannot construct the metric with a null builder." );
        }
        
        this.dataFactory = builder.dataFactory;
        
        if ( Objects.isNull( this.dataFactory ) )
        {
            throw new MetricParameterException( "Specify a data factory with which to build the metric." );
        }

    }      

}
