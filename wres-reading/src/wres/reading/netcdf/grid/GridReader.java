package wres.reading.netcdf.grid;

import org.locationtech.jts.geom.Coordinate;
import thredds.client.catalog.ServiceType;
import ucar.nc2.NetcdfFile;

import wres.datamodel.DataUtilities;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.DoubleEvent;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.netcdf.Netcdf;

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

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Reads gridded data in NetCDF format.
 */
public class GridReader
{
    private static final Object READER_LOCK = new Object();
    private static final Map<String, GridFileReader> FILE_READERS = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger( GridReader.class );

    /**
     * Returns a single-valued time-series response for the input request.
     *
     * @param request the request
     * @return the time-series response
     * @throws IOException if the gridded values cannot be read for any reason
     * @throws InvalidGridRequestException if the request is invalid
     */

    public static Map<Feature, Stream<TimeSeries<Double>>> getSingleValuedTimeSeries( GridRequest request )
            throws IOException
    {
        Objects.requireNonNull( request );

        // #90061-117
        if ( request.paths()
                    .isEmpty() )
        {
            throw new InvalidGridRequestException( "A request for gridded data must contain at least one path to read. "
                                                   + "The request was: "
                                                   + request );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Processing the following request for gridded data {}.", request );
        }

        GridValue griddedValue;

        Queue<String> paths = new LinkedList<>( request.paths() );

        // Events per feature, where each event is indexed by reference time in a pair
        Map<Feature, List<Pair<Instant, Event<Double>>>> eventsPerFeature = new HashMap<>();

        String measurementUnit = "UNKNOWN";

        while ( !paths.isEmpty() )
        {
            String path = paths.remove();
            GridFileReader reader = GridReader.getReader( path, request.isForecast() );

            if ( reader.isLocked() )
            {
                paths.add( path );
            }
            else
            {
                for ( Feature feature : request.features() )
                {
                    List<Pair<Instant, Event<Double>>> events = eventsPerFeature.get( feature );
                    if ( Objects.isNull( events ) )
                    {
                        events = new ArrayList<>();
                        eventsPerFeature.put( feature, events );
                    }

                    // We'll need to eventually iterate through time, but now is
                    // not the... time!
                    griddedValue = reader.read( request.variableName(),
                                                feature.getWkt() );

                    Event<Double> event = DoubleEvent.of( griddedValue.validTime(), griddedValue.value() );
                    Pair<Instant, Event<Double>> eventPlusIssueTime = Pair.of( griddedValue.issueTime(), event );
                    events.add( eventPlusIssueTime );
                    measurementUnit = griddedValue.measurementUnit();
                }
            }
        }

        Map<Feature, Stream<TimeSeries<Double>>> seriesPerFeature = new HashMap<>();

        for ( Map.Entry<Feature, List<Pair<Instant, Event<Double>>>> nextPair : eventsPerFeature.entrySet() )
        {
            Stream<TimeSeries<Double>> timeSeries =
                    GridReader.getTimeSeriesFromListOfEvents( nextPair.getValue(),
                                                              request.timeScale(),
                                                              request.isForecast(),
                                                              request.variableName(),
                                                              nextPair.getKey(),
                                                              measurementUnit )
                              .stream();
            seriesPerFeature.put( nextPair.getKey(), timeSeries );
        }

        return Collections.unmodifiableMap( seriesPerFeature );
    }

    /**
     * Attempts to compose a list of {@link TimeSeries} from a list of events.
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

    private static <T> List<TimeSeries<T>> getTimeSeriesFromListOfEvents( List<Pair<Instant, Event<T>>> events,
                                                                          TimeScaleOuter timeScale,
                                                                          boolean isForecast,
                                                                          String variableName,
                                                                          Feature feature,
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

            // Use a dummy reference time for non-forecasts so that the values are composed as a single series
            if ( !isForecast )
            {
                referenceTime = Instant.MIN;
            }

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
            Builder<T> builder = new Builder<T>().setEvents( nextEntry.getValue() );
            Map<ReferenceTimeType, Instant> referenceTimes = Collections.emptyMap();

            // Add the reference time for forecasts
            if ( isForecast )
            {
                referenceTimes = Map.of( ReferenceTimeType.T0, nextEntry.getKey() );
            }

            TimeSeriesMetadata metadata =
                    TimeSeriesMetadata.of( referenceTimes,
                                           timeScale,
                                           variableName,
                                           feature,
                                           unit );
            builder.setMetadata( metadata );
            returnMe.add( builder.build() );
        }

        // Add duplicates: this will be called recursively
        // until no duplicates are left
        if ( !duplicates.isEmpty() )
        {
            returnMe.addAll( GridReader.getTimeSeriesFromListOfEvents( duplicates,
                                                                       timeScale,
                                                                       isForecast,
                                                                       variableName,
                                                                       feature,
                                                                       unit ) );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Creates a reader.
     * @param filePath the file path
     * @param isForecast whether the data associated with the path is a forecast
     * @return a reader
     */
    private static GridFileReader getReader( final String filePath, final boolean isForecast )
    {
        synchronized ( READER_LOCK )
        {
            GridFileReader reader;

            // If the reader isn't in the collection, add it
            if ( !GridReader.FILE_READERS.containsKey( filePath ) )
            {
                reader = new GridFileReader( filePath, isForecast );
                GridReader.FILE_READERS.put( filePath, reader );
            }
            else
            {
                reader = GridReader.FILE_READERS.get( filePath );
            }

            return reader;
        }
    }

    /**
     * A gridded value.
     * @param value the value
     * @param measurementUnit the measurement unit
     * @param issueTime the issue time
     * @param validTime the valid time
     */
    private record GridValue( double value, String measurementUnit, Instant issueTime, Instant validTime )
    {
    }

    /**
     * Reader.
     */
    private static class GridFileReader
    {
        GridFileReader( final String path, final boolean isForecast )
        {
            this.path = path;
            this.isForecast = isForecast;
            this.readLock = new ReentrantLock();
        }

        GridValue read( String variableName, String wkt )
                throws IOException
        {
            // Time always 0 for now
            int time = 0;
            this.readLock.lock();

            Coordinate point = getLatLonCoordFromSridWkt( wkt );

            // This is underlying THREDDS code. It generally expects some semi-remote location for its data, but we're
            // local, so we're using
            DatasetUrl url = DatasetUrl.create( ServiceType.File, this.path );

            try ( NetcdfDataset dataset = NetcdfDatasets.acquireDataset( url, null );
                  GridDataset gridDataset = new GridDataset( dataset ) )
            {
                GridDatatype variable = gridDataset.findGridDatatype( variableName );

                // #95028, couldn't find the declared variable
                if ( Objects.isNull( variable ) )
                {
                    throw new IOException( "Unable to read the gridded dataset from " + this.path
                                           + " because the "
                                           + "declared variable name "
                                           + variableName
                                           + " was not discovered inside the dataset. Please "
                                           + "correct the declared variable name or the source and try again." );
                }

                // Returns XY from YX parameters
                int[] xIndexYIndex = variable.getCoordinateSystem()
                                             .findXYindexFromLatLon( point.getY(), point.getX(), null );

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

        /**
         * Parse a point from a point WKT, ignoring srid.
         * TODO: do an affine transform, not just parse a point.
         */
        private static Coordinate getLatLonCoordFromSridWkt( String wkt )
        {
            return DataUtilities.getLonLatFromPointWkt( wkt );
        }

        private Instant getValidTime() throws IOException
        {
            if ( this.validTime == null )
            {
                DatasetUrl url = DatasetUrl.create( ServiceType.File, this.path );
                try ( NetcdfFile file = NetcdfDatasets.acquireFile( url, null ) )
                {
                    this.validTime = Netcdf.getTime( file );
                }
            }

            return this.validTime;
        }

        private Instant getIssueTime() throws IOException
        {
            if ( this.issueTime == null && this.isForecast )
            {
                DatasetUrl url = DatasetUrl.create( ServiceType.File, this.path );
                try ( NetcdfFile file = NetcdfDatasets.acquireFile( url, null ) )
                {
                    this.issueTime = Netcdf.getReferenceTime( file );
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

    private GridReader()
    {
    }
}
