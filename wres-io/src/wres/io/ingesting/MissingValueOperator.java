package wres.io.ingesting;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import wres.config.generated.DataSourceConfig.Source;
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;

/**
 * A class that applies a consistent missing value identifier for any missing values that are identified within the 
 * project declaration, rather than handled in-band to readers.
 * 
 * @author James Brown
 */

class MissingValueOperator implements UnaryOperator<Stream<TimeSeriesTuple>>
{
    /** The missing value transformer. */
    private static final UnaryOperator<TimeSeriesTuple> MISSING_VALUE_TRANSFORMER =
            MissingValueOperator.getMissingValueTransformer();

    /**
     * Creates an instance.
     * 
     * @return an instance of this class
     */

    static MissingValueOperator of()
    {
        return new MissingValueOperator();
    }

    @Override
    public Stream<TimeSeriesTuple> apply( Stream<TimeSeriesTuple> timeSeries )
    {
        Objects.requireNonNull( timeSeries );

        return timeSeries.map( MISSING_VALUE_TRANSFORMER );
    }

    /**
     * Creates a missing value transformer.
     * 
     * @return a missing value transformer
     */
    private static UnaryOperator<TimeSeriesTuple> getMissingValueTransformer()
    {
        return tuple -> {
            UnaryOperator<Double> missingMapper =
                    MissingValueOperator.getMissingValueTransformer( tuple.getDataSource() );

            if ( tuple.hasSingleValuedTimeSeries() )
            {
                TimeSeries<Double> singleValued = tuple.getSingleValuedTimeSeries();
                TimeSeries<Double> transformed = TimeSeriesSlicer.transform( singleValued, missingMapper, null );
                return TimeSeriesTuple.ofSingleValued( transformed, tuple.getDataSource() );
            }
            else
            {
                TimeSeries<Ensemble> ensemble = tuple.getEnsembleTimeSeries();
                UnaryOperator<Ensemble> ensembleTransformer = TimeSeriesSlicer.getEnsembleTransformer( missingMapper );
                TimeSeries<Ensemble> transformed = TimeSeriesSlicer.transform( ensemble, ensembleTransformer, null );
                return TimeSeriesTuple.ofEnsemble( transformed, tuple.getDataSource() );
            }
        };
    }

    /**
     * Returns an operator that replaces a missing value declared alongside the data source with the 
     * {@link MissingValues#DOUBLE}.
     * 
     * @param dataSource the data source
     * @return the missing value transformer
     */

    private static UnaryOperator<Double> getMissingValueTransformer( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Parse the missings once per source, not once per call of the transformer
        double[] missingDoubles = new double[0];
        Source source = dataSource.getSource();
        if ( Objects.nonNull( source ) && Objects.nonNull( source.getMissingValue() ) )
        {
            String missingValueString = source.getMissingValue();
            String[] missingValueStrings = missingValueString.split( "," );
            missingDoubles = Arrays.stream( missingValueStrings )
                                   .mapToDouble( Double::parseDouble )
                                   .sorted()
                                   .toArray();
        }

        // No missings to check
        if ( missingDoubles.length == 0 )
        {
            return in -> in;
        }

        // Some missings to check
        double[] finalMissings = missingDoubles;

        return doubleValue -> {
            if ( Arrays.binarySearch( finalMissings, doubleValue ) >= 0 )
            {
                return MissingValues.DOUBLE;
            }

            return doubleValue;
        };
    }

    /**
     * Hidden constructor.
     */

    private MissingValueOperator()
    {
        // Do nothing
    }
}