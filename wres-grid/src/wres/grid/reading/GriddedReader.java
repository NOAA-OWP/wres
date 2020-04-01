package wres.grid.reading;

import thredds.client.catalog.ServiceType;
import ucar.nc2.NetcdfFile;
import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;
import wres.util.NetCDF;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.Array;

import ucar.nc2.dataset.DatasetUrl;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

public class GriddedReader
{
    /**
     * Stands-up resources per JVM.
     */

    private static final Object READER_LOCK = new Object();

    private static final Map<String, GridFileReader> FILE_READERS = new ConcurrentHashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger( GriddedReader.class );

    private static GridFileReader getReader( final String filePath, final boolean isForecast )
    {
        synchronized ( READER_LOCK )
        {
            GridFileReader reader;

            // If the reader isn't in the collection, add it
            if ( !GriddedReader.FILE_READERS.containsKey( filePath ) )
            {
                reader = new GridFileReader( filePath, isForecast );
                GriddedReader.FILE_READERS.put( filePath, reader );
            }
            else
            {
                reader = GriddedReader.FILE_READERS.get( filePath );
            }

            return reader;
        }
    }

    /**
     * Returns a single-valued time-series response for the input request.
     * 
     * @param request the request
     * @return the time-series response
     * @throws IOException if the gridded values cannot be read for any reason
     */

    public static SingleValuedTimeSeriesResponse getSingleValuedResponse( Request request ) throws IOException
    {
        Objects.requireNonNull( request );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Processing the following request for gridded data {}.", request );
        }

        GridValue griddedValue;

        Queue<String> paths = new LinkedList<>( request.getPaths() );

        // Events per feature, where each event is indexed by reference time in a pair
        Map<FeaturePlus, List<Pair<Instant, Event<Double>>>> eventsPerFeature = new HashMap<>();

        String measurementUnit = "UNKNOWN";

        while ( !paths.isEmpty() )
        {
            String path = paths.remove();
            GridFileReader reader = GriddedReader.getReader( path, request.isForecast() );

            if ( reader.isLocked() )
            {
                paths.add( path );
            }
            else
            {
                for ( Feature feature : request.getFeatures() )
                {
                    FeaturePlus featurePlus = FeaturePlus.of( feature );

                    List<Pair<Instant, Event<Double>>> events = eventsPerFeature.get( featurePlus );
                    if ( Objects.isNull( events ) )
                    {
                        events = new ArrayList<>();
                        eventsPerFeature.put( featurePlus, events );
                    }

                    // We'll need to eventually iterate through time, but now is
                    // not the... time!
                    griddedValue = reader.read(
                                                null,
                                                request.getVariableName(),
                                                feature.getCoordinate().getLatitude(),
                                                feature.getCoordinate().getLongitude() );

                    events.add( Pair.of( griddedValue.getIssueTime(),
                                         Event.of( griddedValue.getValidTime(), griddedValue.getValue() ) ) );
                    measurementUnit = griddedValue.getMeasurementUnit();
                }
            }
        }

        Map<FeaturePlus, Stream<TimeSeries<Double>>> seriesPerFeature = new HashMap<>();

        for ( Map.Entry<FeaturePlus, List<Pair<Instant, Event<Double>>>> nextPair : eventsPerFeature.entrySet() )
        {
            Stream<TimeSeries<Double>> timeSeries =
                    GriddedReader.getTimeSeriesFromListOfEvents( nextPair.getValue(), 
                                                                 request.getTimeScale(),
                                                                 request.isForecast(),
                                                                 request.getVariableName(),
                                                                 nextPair.getKey(),
                                                                 measurementUnit )
                                 .stream();
            seriesPerFeature.put( nextPair.getKey(), timeSeries );
        }

        seriesPerFeature = Collections.unmodifiableMap( seriesPerFeature );

        return SingleValuedTimeSeriesResponse.of( seriesPerFeature, request.getVariableName(), measurementUnit );
    }

    /**
     * Attempts to compose a list of {@link TimeSeries} from a list of events.
     * 
     * TODO: replace with retrieval based around uniquely identified time-series. In the presence of duplicate events
     * whose values are different, it is impossible, by definition, to know the time-series to which a duplicate 
     * belongs; rather time-series must be composed with reference to a time-series identifier.
     *  
     * @param <T> the type of event
     * @param events the events
     * @param timeScale optional time scale information
     * @return a best guess about the time-series composed by the events
     * @throws NullPointerException if the input is null
     */

    static <T> List<TimeSeries<T>> getTimeSeriesFromListOfEvents( List<Pair<Instant, Event<T>>> events,
                                                                  TimeScale timeScale,
                                                                  boolean isForecast,
                                                                  String variableName,
                                                                  FeaturePlus featurePlus,
                                                                  String unit )
    {
        Objects.requireNonNull( events );

        // Map the events by reference datetime
        // Place any duplicates by valid time in a separate list 
        // and call recursively until no duplicates exist
        List<Pair<Instant, Event<T>>> duplicates = new ArrayList<>();
        Map<Instant, SortedSet<Event<T>>> eventsByReferenceTime = new TreeMap<>();
        List<TimeSeries<T>> returnMe = new ArrayList<>();

        // Iterate the events
        for ( Pair<Instant, Event<T>> nextPair : events )
        {
            Event<T> nextEvent = nextPair.getRight();

            Instant referenceTime = nextPair.getLeft();

            // Existing series
            if ( eventsByReferenceTime.containsKey( referenceTime ) )
            {
                SortedSet<Event<T>> nextSeries = eventsByReferenceTime.get( referenceTime );

                // Duplicate?
                if ( nextSeries.contains( nextEvent ) )
                {
                    duplicates.add( nextPair );
                }
                else
                {
                    nextSeries.add( nextEvent );
                }
            }
            // New series
            else
            {
                // Sorted set that checks for times only, not values
                // In other words, a duplicate is a coincident measurement by time, not value
                SortedSet<Event<T>> container = new TreeSet<>( ( e1, e2 ) -> e1.getTime().compareTo( e2.getTime() ) );

                //Add the first value
                container.add( nextEvent );
                eventsByReferenceTime.put( referenceTime, container );
            }
        }

        // Add the time-series
        for ( Map.Entry<Instant, SortedSet<Event<T>>> nextEntry : eventsByReferenceTime.entrySet() )
        {
            TimeSeriesBuilder<T> builder =
                    new TimeSeriesBuilder<T>().addEvents( nextEntry.getValue() );
            Map<ReferenceTimeType,Instant> referenceTimes = Collections.emptyMap();

            // Add the reference time for forecasts
            if( isForecast )
            {
                referenceTimes = Map.of( ReferenceTimeType.T0, nextEntry.getKey() );
            }

            TimeSeriesMetadata metadata =
                    TimeSeriesMetadata.of( referenceTimes,
                                           timeScale,
                                           variableName,
                                           String.valueOf( featurePlus.getFeature()
                                                                      .getComid() ),
                                           unit );
            builder.setMetadata( metadata );
            returnMe.add( builder.build() );
        }

        // Add duplicates: this will be called recursively
        // until no duplicates are left
        if ( !duplicates.isEmpty() )
        {
            returnMe.addAll( GriddedReader.getTimeSeriesFromListOfEvents( duplicates,
                                                                          timeScale,
                                                                          isForecast,
                                                                          variableName,
                                                                          featurePlus,
                                                                          unit ) );
        }

        return Collections.unmodifiableList( returnMe );
    }

    private static class GridValue
    {
        GridValue(
                   final double value,
                   final String measurementUnit,
                   final Instant issueTime,
                   final Instant validTime )
        {
            this.value = value;
            this.measurementUnit = measurementUnit;
            this.issueTime = issueTime;
            this.validTime = validTime;
        }

        double getValue()
        {
            return value;
        }

        Instant getIssueTime()
        {
            return this.issueTime;
        }

        Instant getValidTime()
        {
            return this.validTime;
        }

        String getMeasurementUnit()
        {
            return this.measurementUnit;
        }

        private final String measurementUnit;
        private final double value;
        private final Instant issueTime;
        private final Instant validTime;
    }

    private static class GridFileReader
    {
        GridFileReader( final String path, final boolean isForecast )
        {
            this.path = path;
            this.isForecast = isForecast;
            this.readLock = new ReentrantLock();
        }

        GridValue read( Integer time, String variableName, final double latitude, final double longitude )
                throws IOException
        {
            if ( time == null )
            {
                time = 0;
            }

            this.readLock.lock();

            // This is underlying THREDDS code. It generally expects some semi-remote location for its data, but we're local, so we're using
            DatasetUrl url = new DatasetUrl( ServiceType.File, this.path );

            try ( NetcdfDataset dataset = NetcdfDatasets.acquireDataset( url, null );
                  GridDataset gridDataset = new GridDataset( dataset ) )
            {
                GridDatatype variable = gridDataset.findGridDatatype( variableName );

                // Returns XY from YX parameters
                int[] xIndexYIndex = variable.getCoordinateSystem().findXYindexFromLatLon( latitude, longitude, null );

                // readDataSlice takes (time, z, y, x) as parameters. Since the previous call was XY, we need to flip
                // the two, yielding indexes 1 then 0
                Array data = variable.readDataSlice( time, 0, xIndexYIndex[1], xIndexYIndex[0] );

                double value = data.getDouble( 0 );
                return new GridValue( value, variable.getUnitsString(), this.getIssueTime(), this.getValidTime() );
            }
            finally
            {
                this.readLock.unlock();
            }
        }

        boolean isLocked()
        {
            return this.readLock.isLocked();
        }

        private Instant getValidTime() throws IOException
        {
            if ( this.validTime == null )
            {
                DatasetUrl url = new DatasetUrl( ServiceType.File, this.path );
                try ( NetcdfFile file = NetcdfDataset.acquireFile( url, null ) )
                {
                    this.validTime = NetCDF.getTime( file );
                }
            }

            return this.validTime;
        }

        private Instant getIssueTime() throws IOException
        {
            if ( this.issueTime == null && this.isForecast )
            {
                DatasetUrl url = new DatasetUrl( ServiceType.File, this.path );
                try ( NetcdfFile file = NetcdfDatasets.acquireFile( url, null ) )
                {
                    this.issueTime = NetCDF.getReferenceTime( file );
                }
            }
            else if ( this.issueTime == null )
            {
                this.issueTime = this.getValidTime();
            }

            return this.issueTime;
        }

        private final String path;
        private Instant issueTime = null;
        private Instant validTime = null;
        private final ReentrantLock readLock;
        private final boolean isForecast;
    }

    /**
     * Do not construct.
     */

    private GriddedReader()
    {
    }
}
