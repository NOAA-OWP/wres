package wres.io.ingesting;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.components.DataType;
import wres.config.components.DatasetOrientation;
import wres.datamodel.MissingValues;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.TestData;
import wres.io.ingesting.memory.InMemoryTimeSeriesIngester;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;

/**
 * Tests the {@link InMemoryTimeSeriesIngesterTest}.
 *
 * @author James Brown
 */
class InMemoryTimeSeriesIngesterTest
{
    @Test
    void testIngest()
    {
        TimeSeriesStore.Builder builder = new TimeSeriesStore.Builder();
        TimeSeriesIngester ingester = InMemoryTimeSeriesIngester.of( builder );

        TimeSeries<Double> timeSeries = TestData.generateTimeSeriesDoubleWithNoReferenceTimes();
        DataSource dataSource = TestData.generateDataSource( DatasetOrientation.LEFT, DataType.SIMULATIONS );
        TimeSeriesTuple testTuple = TimeSeriesTuple.ofSingleValued( timeSeries, dataSource );
        List<IngestResult> results = ingester.ingest( Stream.of( testTuple ), dataSource );

        TimeSeriesStore store = builder.build();
        List<TimeSeries<Double>> actual = store.getSingleValuedSeries( DatasetOrientation.LEFT )
                                               .toList();
        List<TimeSeries<Double>> expected = List.of( timeSeries );

        assertAll( () -> assertEquals( expected, actual ),
                   () -> assertTrue( results.get( 0 ).hasNonMissingData() ) );
    }

    @Test
    void testIngestWithNoData()
    {
        TimeSeriesStore.Builder builder = new TimeSeriesStore.Builder();
        TimeSeriesIngester ingester = InMemoryTimeSeriesIngester.of( builder );

        TimeSeries<Double> source = TestData.generateTimeSeriesDoubleWithNoReferenceTimes();
        TimeSeries<Double> timeSeries = TimeSeriesSlicer.transform( source, e -> MissingValues.DOUBLE, m -> m );
        DataSource dataSource = TestData.generateDataSource( DatasetOrientation.LEFT, DataType.SIMULATIONS );
        TimeSeriesTuple testTuple = TimeSeriesTuple.ofSingleValued( timeSeries, dataSource );
        List<IngestResult> results = ingester.ingest( Stream.of( testTuple ), dataSource );

        TimeSeriesStore store = builder.build();
        List<TimeSeries<Double>> actual = store.getSingleValuedSeries( DatasetOrientation.LEFT )
                                               .toList();
        List<TimeSeries<Double>> expected = List.of( timeSeries );

        assertAll( () -> assertEquals( expected, actual ),
                   () -> assertFalse( results.get( 0 ).hasNonMissingData() ) );
    }
}
