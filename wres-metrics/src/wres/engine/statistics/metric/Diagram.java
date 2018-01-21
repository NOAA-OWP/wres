package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MetricOutput;

/**
 * An abstract diagram.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class Diagram<S extends MetricInput<?>, T extends MetricOutput<?>> implements Metric<S, T>
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
    
    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static abstract class DiagramBuilder<S extends MetricInput<?>, T extends MetricOutput<?>>
            implements
            MetricBuilder<S, T>
    {

        /**
         * The data factory.
         */
        
        protected DataFactory dataFactory;

        /**
         * Sets the {@link DataFactory} for constructing a {@link MetricOutput}.
         * 
         * @param dataFactory the {@link DataFactory}
         * @return the builder
         */

        @Override
        public DiagramBuilder<S,T> setOutputFactory( final DataFactory dataFactory )
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

    protected Diagram( final DiagramBuilder<S,T> builder ) throws MetricParameterException
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
