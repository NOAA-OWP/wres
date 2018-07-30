package wres.engine.statistics.metric.ensemble;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * An abstract base class for plotting (the errors associated with) ensemble forecasts as a box. Each pair generates
 * one box. The domain and range axis associated with the metric function is dictated by concrete implementations. For
 * example, the domain axis may show the observed value and the range axis may show forecast value. Alternatively, the 
 * domain axis may show a function of the ensemble forecast value (e.g. the ensemble mean) and the range axis may show
 * forecast error. 
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class BoxPlot
        extends
        Diagram<EnsemblePairs, BoxPlotOutput>
{

    /**
     * Default probabilities.
     */

    static final VectorOfDoubles DEFAULT_PROBABILITIES =
            VectorOfDoubles.of( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } );

    /**
     * A vector of probabilities that define the quantiles to plot.
     */

    final VectorOfDoubles probabilities;

    /**
     * Creates a box from a {@link EnsemblePair}.
     * 
     * @param pair the pair
     * @return a box
     * @throws MetricCalculationException if the box cannot be constructed
     */

    abstract EnsemblePair getBox( EnsemblePair pair );

    /**
     * Returns the dimension associated with the left side of the pairing, i.e. the value against which each box is
     * plotted on the domain axis. 
     * 
     * @return the domain axis dimension
     */

    abstract MetricDimension getDomainAxisDimension();

    /**
     * Returns the dimension associated with the right side of the pairing, i.e. the values associated with the 
     * whiskers of each box. 
     * 
     * @return the range axis dimension
     */

    abstract MetricDimension getRangeAxisDimension();

    @Override
    public BoxPlotOutput apply( final EnsemblePairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        List<EnsemblePair> boxes = new ArrayList<>();
        //Create each box
        for ( EnsemblePair next : s )
        {
            boxes.add( getBox( next ) );
        }
        MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );
        return DataFactory.ofBoxPlotOutput( boxes,
                                            probabilities,
                                            metOut,
                                            getDomainAxisDimension(),
                                            getRangeAxisDimension() );
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    BoxPlot()
    {
        super();

        this.probabilities = DEFAULT_PROBABILITIES;
    }
    
    /**
     * Hidden constructor.
     * 
     * @param probabilities the probabilities
     * @throws MetricParameterException if one or more parameters are invalid
     */

    BoxPlot( VectorOfDoubles probabilities ) throws MetricParameterException
    {
        super();

        //Validate the probabilities
        if ( probabilities.size() < 2 )
        {
            throw new MetricParameterException( "Specify at least two probabilities for the verification box plot." );
        }
        if ( probabilities.size() > 2 && probabilities.size() % 2 == 0 )
        {
            throw new MetricParameterException( "Specify an odd number of probabilities for the verification box plot." );
        }

        //Check for invalid or duplicate values
        Set<Double> check = new HashSet<>();
        for ( double next : probabilities.getDoubles() )
        {
            if ( check.contains( next ) )
            {
                throw new MetricParameterException( "Specify only non-unique probabilities from which to construct "
                                                    + "the box plot." );
            }
            if ( next < 0.0 || next > 1.0 )
            {
                throw new MetricParameterException( "Specify only valid probabilities within [0,1] from which to "
                                                    + "construct the box plot." );
            }
            check.add( next );
        }

        this.probabilities = probabilities;

    }
}
