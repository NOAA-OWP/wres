package wres.engine.statistics.metric.ensemble;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.StatisticMetadata;
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

abstract class EnsembleBoxPlot extends Diagram<SampleData<Pair<Double, Ensemble>>, BoxPlotStatistics>
{

    /**
     * Default probabilities.
     */

    static final VectorOfDoubles DEFAULT_PROBABILITIES =
            VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

    /**
     * A vector of probabilities that define the quantiles to plot.
     */

    final VectorOfDoubles probabilities;

    /**
     * Creates a box from an ensemble pair.
     * 
     * @param pair the pair
     * @param metadata the box metadata
     * @return a box
     * @throws MetricCalculationException if the box cannot be constructed
     */

    abstract BoxPlotStatistic getBox( Pair<Double,Ensemble> pair, StatisticMetadata metadata );

    @Override
    public BoxPlotStatistics apply( final SampleData<Pair<Double, Ensemble>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        List<BoxPlotStatistic> boxes = new ArrayList<>();

        StatisticMetadata metOut = StatisticMetadata.of( s.getMetadata(),
                                                         this.getID(),
                                                         MetricConstants.MAIN,
                                                         this.hasRealUnits(),
                                                         s.getRawData().size(),
                                                         null );

        //Create each box
        for ( Pair<Double,Ensemble> next : s.getRawData() )
        {
            boxes.add( this.getBox( next, metOut ) );
        }

        return BoxPlotStatistics.of( boxes, metOut );
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    EnsembleBoxPlot()
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

    EnsembleBoxPlot( VectorOfDoubles probabilities ) throws MetricParameterException
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
