package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.function.Function;

import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputMetadata;

/**
 * <p>
 * Abstract base class for an immutable metric. The metric calculation is implemented in {@link #apply(MetricInput)}.
 * The metric may operate on paired or unpaired inputs, and inputs that comprise one or more individual datasets. The
 * structure of the input and output is prescribed by a particular subclass.
 * </p>
 * <p>
 * A metric cannot conduct any pre-processing of the input, such as rescaling, changing measurement units, conditioning,
 * or removing missing values. However, for metrics that rely on ordered input, such as position within a time-series 
 * or spatial coordinates, missing values may be used to retain (relative) position. Such metrics are uniquely aware 
 * of missing values. In other cases, missing values should be removed upfront.
 * </p>
 * <p>
 * In order to build a metric, implement the inner class {@link MetricBuilder} and validate the parameters in a hidden
 * constructor.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class Metric<S extends MetricInput<?>, T extends MetricOutput<?>> implements Function<S, T>
{

    /**
     * Instance of a {@link DataFactory} for constructing a {@link MetricOutput}.
     */

    private final DataFactory dataFactory;

    /**
     * Applies the function to the input and throws a {@link MetricCalculationException} if the calculation fails.
     * 
     * @param s the input
     * @return the output
     * @throws MetricInputException if the metric input is unexpected
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Override
    public abstract T apply( S s );

    /**
     * Returns a unique identifier for the metric from {@link MetricConstants}.
     * 
     * @return a unique identifier
     */

    public abstract MetricConstants getID();

    /**
     * Returns true if the metric generates outputs that are dimensioned in real units, false if the outputs are in
     * statistical or probabilistic units.
     * 
     * @return true if the outputs are dimensioned in real units, false otherwise
     */

    public abstract boolean hasRealUnits();

    /**
     * Returns the unique name of the metric, namely the string representation of {@link #getID()}.
     * 
     * @return the unique metric name
     */

    public String getName()
    {
        return getID().toString();
    }

    /**
     * Returns {@link #getName()}
     * 
     * @return a string representation of the metric.
     */

    @Override
    public String toString()
    {
        return getName();
    }

    /**
     * Returns true when the input is a {@link Metric} and the metrics have equivalent names, i.e. {@link #getName()}
     * returns an equivalent string.
     * 
     * @param o the object to test for equality with the current object
     * @return true if the input is a metric and has an equivalent name to the current metric, false otherwise
     */
    public boolean nameEquals( final Object o )
    {
        return o != null && o instanceof Metric && ( (Metric<?, ?>) o ).getName().equals( getName() );
    }

    /**
     * <p>
     * An abstract builder to build a {@link Metric}. Implement this interface when building a {@link Metric}, and hide
     * the constructor. Add setters to set the parameters of the metric, as required, prior to building. For thread
     * safety, validate the parameters using the hidden constructor of the {@link Metric}, i.e. do not validate the
     * parameters in the {@link MetricBuilder} before construction.
     * </p>
     * <p>
     * TODO: support construction with parameters by defining an abstract method, setParameters(EnumMap mapping).
     * </p>
     */

    public static abstract class MetricBuilder<P extends MetricInput<?>, Q extends MetricOutput<?>>
    {

        DataFactory dataFactory;

        /**
         * Build the {@link Metric}.
         * 
         * @return a {@link Metric}
         * @throws MetricParameterException if one or more parameters is incorrect
         */

        protected abstract Metric<P, Q> build() throws MetricParameterException;

        /**
         * Sets the {@link DataFactory} for constructing a {@link MetricOutput}.
         * 
         * @param dataFactory the {@link DataFactory}
         * @return the builder
         */

        public MetricBuilder<P, Q> setOutputFactory( final DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

    }

    /**
     * Returns a {@link DataFactory} for constructing a {@link MetricOutput}.
     * 
     * @return a {@link DataFactory}
     */

    protected DataFactory getDataFactory()
    {
        return dataFactory;
    }

    /**
     * Returns the {@link MetricOutputMetadata} using a prescribed {@link MetricInput} and the current {@link Metric} to
     * compose, along with an explicit component identifier or decomposition template and an identifier for the
     * baseline, where applicable. This helper method is not intended for implementations of {@link Collectable}, whose
     * {@link Collectable#getCollectionInput(MetricInput)} should return the {@link MetricConstants} identifier
     * associated with the implementing class and not the caller. This method identifies the metric by calling
     * {@link #getID()}.
     * 
     * @param input the metric input
     * @param sampleSize the sample size
     * @param componentID the component identifier or metric decomposition template
     * @param baselineID the baseline identifier or null
     * @return the metadata
     */

    protected MetricOutputMetadata getMetadata( final MetricInput<?> input,
                                      final int sampleSize,
                                      final MetricConstants componentID,
                                      final DatasetIdentifier baselineID )
    {
        if ( this instanceof Collectable )
        {
            throw new UnsupportedOperationException( "Cannot safely obtain the metadata for the collectable "
                                                     + "implementation of '"
                                                     + getID()
                                                     + "': build the metadata in the implementing class." );
        }
        final Metadata metIn = input.getMetadata();
        Dimension outputDim = null;
        //Dimensioned?
        if ( hasRealUnits() )
        {
            outputDim = metIn.getDimension();
        }
        else
        {
            outputDim = dataFactory.getMetadataFactory().getDimension();
        }
        DatasetIdentifier identifier = metIn.getIdentifier();
        //Add the scenario ID associated with the baseline input
        if ( Objects.nonNull( baselineID ) )
        {
            identifier =
                    dataFactory.getMetadataFactory().getDatasetIdentifier( identifier, baselineID.getScenarioID() );
        }
        return dataFactory.getMetadataFactory()
                          .getOutputMetadata( sampleSize,
                                              outputDim,
                                              metIn.getDimension(),
                                              getID(),
                                              componentID,
                                              identifier,
                                              metIn.getTimeWindow() );
    }

    /**
     * Construct a {@link Metric} with a {@link MetricBuilder}.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    protected Metric( final MetricBuilder<S, T> builder ) throws MetricParameterException
    {
        if ( Objects.isNull( builder ) )
        {
            throw new MetricParameterException( "Cannot construct the metric with a null builder." );
        }
        if ( Objects.isNull( builder.dataFactory ) )
        {
            throw new MetricParameterException( "Specify a data factory with which to build the metric." );
        }
        this.dataFactory = builder.dataFactory;
    }

}
