package wres.io.retrieval.left;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.project.Project;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.NoDataException;
import wres.util.Collections;
import wres.util.TimeHelper;

class GridCache implements LeftHandCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger( GridCache.class);

    /**
     * @throws NoDataException when ?
     */
    GridCache(final Project project ) throws SQLException
    {
        this.project = project;
        this.cachedSources = new TreeMap<>(  );
        this.loadSourceCache();
    }

    @Override
    public Collection<Double> getLeftValues( Feature feature,
                                             LocalDateTime earliestTime,
                                             LocalDateTime latestTime )
            throws IOException
    {
        List<Double> leftValues = new ArrayList<>();

        Instant earliestValidTime = earliestTime.toInstant( ZoneOffset.of( "Z" ) );
        Instant latestValidTime = latestTime.toInstant( ZoneOffset.of( "Z" ) );        
        TimeWindow timeWindow = TimeWindow.of( earliestValidTime, latestValidTime );
        boolean isForecast = ConfigHelper.isForecast( this.project.getLeft() );
        List<Feature> features = List.of( feature );
        String variableName = this.project.getLeft().getVariable().getValue();

        TimeScale timeScale = null;
        if ( Objects.nonNull( this.project.getLeft().getExistingTimeScale() ) )
        {
            timeScale = TimeScale.of( this.project.getLeft().getExistingTimeScale() );
        }

        Collection<String> paths = Collections.getValuesInRange( this.cachedSources, earliestTime, latestTime );
        if ( paths.size() == 0 )
        {
            LOGGER.debug( "There are no gridded data files for left hand "
                          + "comparison for this project between the dates of "
                          + "'{}' and '{}'",
                          TimeHelper.convertDateToString( earliestTime ),
                          TimeHelper.convertDateToString( latestTime ) );
            return leftValues;
        }

        Request griddedRequest = Fetcher.prepareRequest( List.copyOf( paths ),
                                                         features,
                                                         variableName,
                                                         timeWindow,
                                                         isForecast,
                                                         timeScale );

        SingleValuedTimeSeriesResponse gridResponse = Fetcher.getSingleValuedTimeSeries( griddedRequest );

        Map<FeaturePlus,Stream<TimeSeries<Double>>> timeSeries = gridResponse.getTimeSeries();
        
        // Until we support many locations per retrieval, we don't need special handling for features        
        for ( Stream<TimeSeries<Double>> series : timeSeries.values() )
        {
            leftValues.addAll( this.getFeatureValues( series, gridResponse.getMeasuremenUnits() ) );
        }

        return leftValues;
    }

    private List<Double> getFeatureValues(Stream<TimeSeries<Double>> featureSeries, String measurementUnits)
            throws IOException
    {
        List<Double> leftValues = new ArrayList<>();
        List<TimeSeries<Double>> nextList = featureSeries.collect( Collectors.toList() );
        
        for ( TimeSeries<Double> series : nextList )
        {
            leftValues.addAll( this.addTimeSeriesValues( series, measurementUnits ) );
        }

        return leftValues;
    }

    private List<Double> addTimeSeriesValues( TimeSeries<Double> series, String measurementUnits )
            throws IOException
    {
        List<Double> leftValues = new ArrayList<>();
        for ( Event<Double> entry : series.getEvents() )
        {
            try
            {
                leftValues.add(
                                UnitConversions.convert(
                                                         entry.getValue(),
                                                         measurementUnits,
                                                         this.project.getDesiredMeasurementUnit() ) );
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not convert control value '" +
                                       String.valueOf( entry.getValue() )
                                       +
                                       "' because a valid conversion from '"
                                       +
                                       measurementUnits
                                       +
                                       "' to '"
                                       +
                                       this.project.getDesiredMeasurementUnit()
                                       +
                                       "' could not be determined.",
                                       e );
            }
        }

        return leftValues;
    }


    /**
     * @throws NoDataException when no data could be loaded
     */

    private void loadSourceCache() throws SQLException
    {
        DataScripter script = new DataScripter();
        script.addLine("SELECT S.path, S.output_time + INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * S.lead AS output_time");
        script.addLine("FROM wres.Source S");
        script.addLine("WHERE S.is_point_data = FALSE");

        if ( project.getEarliestDate() != null)
        {
            script.addTab().addLine( "AND S.output_time >= '", project.getEarliestDate(), "'");
        }

        if ( project.getLatestDate() != null)
        {
            script.addTab().addLine( "AND S.output_time <= '", project.getLatestDate(), "'");
        }

        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine( "WHERE PS.project_id = ", project.getId());
        script.addTab(   3   ).addLine( "AND PS.member = ", Project.LEFT_MEMBER);
        script.addTab(   3   ).addLine("AND PS.source_id = S.source_id");
        script.addTab().addLine(");");

        script.consume( this::addSourceToCache );

        if (this.cachedSources.size() == 0)
        {
            throw new NoDataException( "No gridded sources data could be found "
                                       + "to compare data against." );
        }
    }

    private void addSourceToCache(DataProvider data) throws SQLException
    {
        LocalDateTime leftTime = data.getLocalDateTime( "output_time" );
        this.cachedSources.put( leftTime, data.getString( "path" ) );
    }

    private final NavigableMap<LocalDateTime, String> cachedSources;
    private final Project project;
}
