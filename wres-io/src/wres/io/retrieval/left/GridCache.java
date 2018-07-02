package wres.io.retrieval.left;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.Response;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.util.Collections;
import wres.util.TimeHelper;

class GridCache implements LeftHandCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger( GridCache.class);

    GridCache(final ProjectDetails projectDetails) throws SQLException, NoDataException
    {
        this.projectDetails = projectDetails;
        this.cachedSources = new TreeMap<>(  );
        this.loadSourceCache();
    }

    @Override
    public List<Double> getLeftValues( Feature feature,
                                       LocalDateTime earliestDate,
                                       LocalDateTime latestDateTime )
            throws IOException
    {
        Request gridRequest;

        List<Double> leftValues = new ArrayList<>();

        try
        {
            gridRequest = ConfigHelper.getGridDataRequest(
                    this.projectDetails,
                    this.projectDetails.getLeft(),
                    feature
            );
        }
        catch ( SQLException e )
        {
            throw new IOException( "File paths to investigate could not be determined.", e );
        }

        gridRequest.setEarliestValidTime( earliestDate.toInstant( ZoneOffset.UTC ) );
        gridRequest.setLatestValidTime( latestDateTime.toInstant( ZoneOffset.UTC ) );

        List<String> paths = Collections.getValuesInRange( this.cachedSources, earliestDate, latestDateTime );
        if (paths.size() == 0)
        {
            LOGGER.debug("There are no gridded data files for left hand "
                         + "comparison for this project between the dates of "
                         + "'{}' and '{}'",
                         TimeHelper.convertDateToString( earliestDate ),
                         TimeHelper.convertDateToString( latestDateTime ));
            return leftValues;
        }

        paths.forEach( gridRequest::addPath );

        Response gridResponse = Fetcher.getData(gridRequest);

        for (List<Response.Series> seriesPerFeature : gridResponse)
        {
            leftValues.addAll( this.addFeatureValues( seriesPerFeature ) );
        }

        return leftValues;
    }

    private List<Double> addFeatureValues(List<Response.Series> featureSeries)
            throws IOException
    {
        List<Double> leftValues = new ArrayList<>();

        for (Response.Series series : featureSeries )
        {
            leftValues.addAll( this.addTimeSeriesValues( series ) );
        }

        return leftValues;
    }

    private List<Double> addTimeSeriesValues(Response.Series series)
            throws IOException
    {
        List<Double> leftValues = new ArrayList<>();
        for (Response.Entry entry : series)
        {
            for (double value : entry)
            {
                try
                {
                    leftValues.add(
                            UnitConversions.convert(
                                    value,
                                    entry.getMeasurementUnit(),
                                    this.projectDetails.getDesiredMeasurementUnit()
                            )
                    );
                }
                catch ( SQLException e )
                {
                    throw new IOException( "Could not convert control value '" +
                                           String.valueOf( value ) +
                                           "' because a valid conversion from '" +
                                           entry.getMeasurementUnit() +
                                           "' to '" +
                                           this.projectDetails.getDesiredMeasurementUnit() +
                                           "' could not be determined.",
                                           e);
                }
            }
        }
        return leftValues;
    }

    private void loadSourceCache() throws SQLException, NoDataException
    {
        ScriptBuilder script = new ScriptBuilder();
        script.addLine("SELECT path, output_time");
        script.addLine("FROM wres.Source S");
        script.addLine("WHERE S.is_point_data = FALSE");

        if (projectDetails.getEarliestDate() != null)
        {
            script.addTab().addLine("AND S.output_time >= '", projectDetails.getEarliestDate(), "'");
        }

        if (projectDetails.getLatestDate() != null)
        {
            script.addTab().addLine("AND S.output_time <= '", projectDetails.getLatestDate(), "'");
        }

        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ", projectDetails.getId());
        script.addTab(   3   ).addLine("AND PS.member = ", ProjectDetails.LEFT_MEMBER);
        script.addTab().addLine(")");
        script.addTab().addLine("AND is_point_data = FALSE;");

        script.consume( this::addSourceToCache );

        if (this.cachedSources.size() == 0)
        {
            throw new NoDataException( "No gridded sources data could be found "
                                       + "to compare data against." );
        }
    }

    private void addSourceToCache(ResultSet sourceRow) throws SQLException
    {
        LocalDateTime leftTime = Database.getLocalDateTime( sourceRow, "output_time" );
        this.cachedSources.put( leftTime, sourceRow.getString( "path" ) );
    }

    private final NavigableMap<LocalDateTime, String> cachedSources;
    private final ProjectDetails projectDetails;
}
