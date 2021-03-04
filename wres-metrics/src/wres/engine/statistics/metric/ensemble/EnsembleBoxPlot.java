package wres.engine.statistics.metric.ensemble;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * An abstract base class for plotting (the errors associated with) ensemble forecasts as a box. Each pair generates
 * one box. The domain and range axis associated with the metric function is dictated by concrete implementations. For
 * example, the domain axis may show the observed value and the range axis may show forecast value. Alternatively, the 
 * domain axis may show a function of the ensemble forecast value (e.g. the ensemble mean) and the range axis may show
 * forecast error. 
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class EnsembleBoxPlot extends Diagram<SampleData<Pair<Double, Ensemble>>, BoxplotStatisticOuter>
{

    /**
     * Default probabilities.
     */

    static final List<Double> DEFAULT_PROBABILITIES = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

    /**
     * Function that orders boxes.
     */

    private static final Comparator<? super Box> BOX_COMPARATOR = EnsembleBoxPlot.getBoxComparator();

    /**
     * Creates a box from an ensemble pair.
     * 
     * @param pair the pair
     * @return a box
     * @throws MetricCalculationException if the box cannot be constructed
     */

    abstract Box getBox( Pair<Double, Ensemble> pair );

    /**
     * Returns the metric definition.
     * 
     * @return the metric definition
     */

    abstract BoxplotMetric getMetric();

    @Override
    public BoxplotStatisticOuter apply( final SampleData<Pair<Double, Ensemble>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        List<Box> boxes = new ArrayList<>();

        // Create each box
        for ( Pair<Double, Ensemble> next : s.getRawData() )
        {
            boxes.add( this.getBox( next ) );
        }

        // Sort the boxes by value: #70986
        boxes.sort( BOX_COMPARATOR );

        BoxplotStatistic statistic = BoxplotStatistic.newBuilder()
                                                     .setMetric( this.getMetric()
                                                                     .toBuilder()
                                                                     .setUnits( s.getMetadata()
                                                                                 .getMeasurementUnit()
                                                                                 .toString() ) )
                                                     .addAllStatistics( boxes )
                                                     .build();

        return BoxplotStatisticOuter.of( statistic, s.getMetadata() );
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
    }

    /**
     * Validates the probabilities
     * 
     * @param probabilities the probabilities
     * @throws MetricParameterException if one or more parameters are invalid
     */

    void validateProbabilities( VectorOfDoubles probabilities ) throws MetricParameterException
    {
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
    }

    /**
     * Returns a function that orders boxes.
     * 
     * @return a function that orders boxes
     */

    private static Comparator<? super Box> getBoxComparator()
    {
        return ( first, second ) -> {
            int returnMe = Double.compare( first.getLinkedValue(), second.getLinkedValue() );

            if ( returnMe != 0 )
            {
                return returnMe;
            }

            List<Double> one = first.getQuantilesList();
            List<Double> two = second.getQuantilesList();

            return Arrays.compare( one.toArray( new Double[one.size()] ),
                                   two.toArray( new Double[two.size()] ),
                                   Double::compare );
        };
    }

}
