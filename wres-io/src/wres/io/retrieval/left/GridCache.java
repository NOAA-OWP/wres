package wres.io.retrieval.left;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
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
        this.sources = new ArrayList<>();
        this.values = new HashMap<>();
        this.loadSourceCache();
    }

    @Override
    public List<Double> getLeftValues( Feature feature,
                                       LocalDateTime earliestDate,
                                       LocalDateTime latestDateTime )
            throws IOException
    {
        if (!this.values.containsKey( feature ))
        {
            this.generateLeftHandData( feature );
        }

        return Collections.getValuesInRange( this.values.get(feature), earliestDate, latestDateTime );
    }

    private void generateLeftHandData(final Feature feature) throws IOException
    {
        Request gridRequest;

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

        this.sources.forEach( gridRequest::addPath );

        Response gridResponse = Fetcher.getData(gridRequest);
        NavigableMap<LocalDateTime, Double> featureValues = new TreeMap<>(  );

        for (List<Response.Series> featureSeries : gridResponse)
        {
            for (Response.Series series : featureSeries)
            {
                for (Response.Entry entry : series)
                {
                    if (entry.getMeasurements() != null && entry.getMeasurements().length > 0)
                    {
                        LocalDateTime key = LocalDateTime.ofInstant( entry.getValidDate(), ZoneId.of("UTC" ) );
                        Double value = entry.getMeasurements()[0];

                        try
                        {
                            value = UnitConversions.convert(
                                    value,
                                    entry.getMeasurementUnit(),
                                    this.projectDetails.getDesiredMeasurementUnit()
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

                        featureValues.put(key, value);
                    }
                }
            }
        }

        this.values.put( feature, featureValues );
    }

    private void loadSourceCache() throws SQLException, NoDataException
    {
        ScriptBuilder script = new ScriptBuilder();
        script.addLine("SELECT path");
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

        script.consume( sourceRow -> this.sources.add(sourceRow.getString( "path" )) );

        if (this.sources.isEmpty())
        {
            throw new NoDataException( "No gridded sources data could be found "
                                       + "to compare data against." );
        }
    }

    private final List<String> sources;
    private final ProjectDetails projectDetails;
    private final Map<Feature, NavigableMap<LocalDateTime, Double>> values;
}
