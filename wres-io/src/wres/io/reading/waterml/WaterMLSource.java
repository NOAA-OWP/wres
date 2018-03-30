package wres.io.reading.waterml;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.StatementRunner;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.utilities.Database;
import wres.io.utilities.IOExceptionalConsumer;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.util.Collections;
import wres.util.TimeHelper;

/**
 * Saves WaterML Response objects to the database
 *
 * TODO: Fix database deadlocking issues
 */
public class WaterMLSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WaterMLSource.class);

    private static final String OBSERVATION_UPSERT = WaterMLSource.writeObservationUpsert();

    private class UpsertValue
    {
        UpsertValue(String gageID, String observationTime, Double value)
        {
            this.gageID = gageID;
            this.observationTime = TimeHelper.standardize( observationTime );
            this.value = value;
        }

        Object[] getParameters() throws SQLException
        {
            return new Object[] {
                    this.value,
                    WaterMLSource.this.getVariablePositionID(this.gageID),
                    this.observationTime,
                    WaterMLSource.this.getMeasurementunitId(),
                    WaterMLSource.this.getSourceId(),
                    WaterMLSource.this.getVariablePositionID(this.gageID),
                    this.observationTime,
                    this.value,
                    WaterMLSource.this.getMeasurementunitId(),
                    WaterMLSource.this.getSourceId(),
                    WaterMLSource.this.getVariablePositionID(this.gageID),
                    this.observationTime,
                    WaterMLSource.this.getMeasurementunitId(),
                    WaterMLSource.this.getSourceId()
            };
        }

        private final String observationTime;
        private final Double value;
        private final String gageID;
    }

    private final int sourceId;
    private final Response waterML;
    private final SortedMap<String, Integer> variablePositionIDs;
    private final Stack<UpsertValue> upsertValues = new Stack<>();
    private IOExceptionalConsumer<TimeSeries> invalidSeriesHandler;
    private final int waterMLMeasurementId;


    public WaterMLSource( ProjectConfig projectConfig,
                          Response waterML,
                          int sourceId,
                          int waterMLMeasurementId )
    {
        super( projectConfig );
        this.waterML = waterML;
        this.variablePositionIDs = new TreeMap<>();
        this.sourceId = sourceId;
        this.waterMLMeasurementId = waterMLMeasurementId;
    }

    public void setInvalidSeriesHandler(IOExceptionalConsumer<TimeSeries> handler)
    {
        this.invalidSeriesHandler = handler;
    }

    private void handleInvalidSeries(TimeSeries invalidSeries)
            throws IOException
    {
        if (this.invalidSeriesHandler != null)
        {
            this.invalidSeriesHandler.accept( invalidSeries );
        }
    }

    @Override
    public List<IngestResult> saveObservation() throws IOException
    {
        if ( this.waterML == null ||
             Collections.exists(this.waterML.getValue().getTimeSeries(),
                                series -> series.getValues() == null || series.getValues().length == 0))
        {
            throw new NoDataException( "No WaterML data could be loaded with "
                                       + "the given configuration." );
        }

        for (TimeSeries series : this.waterML.getValue().getTimeSeries())
        {
            this.readObservationSeries( series );
        }

        try
        {
            this.performObservationUpserts();
        }
        catch ( SQLException e )
        {
            throw new IOException( "WaterML observations could not be saved.", e );
        }

        String hash = this.getHash();

        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                this.getDataSourceConfig(),
                                                hash,
                                                false );
    }

    private void readObservationSeries( TimeSeries series )
            throws IOException
    {
        String gageID = series.getSourceInfo().getSiteCode()[0].getValue();

        if (series.getValues().length == 0)
        {
            this.handleInvalidSeries( series );
            System.out.println( "" );
            System.out.println( "" );
            System.out.println(gageID + " won't be parsed.");
            System.out.println("");
            System.out.println("");
            return;
        }

        for (TimeSeriesValues valueSet : series.getValues())
        {
            if (valueSet.getValue().length == 0)
            {
                this.handleInvalidSeries( series );

                System.out.println( "" );
                System.out.println( "" );
                System.out.println("A set of values in " + gageID + " won't be parsed.");
                System.out.println("");
                System.out.println("");
                continue;
            }

            for (TimeSeriesValue value : valueSet.getValue())
            {
                try
                {
                    Double readValue = value.getValue();

                    if ( series.getVariable().getNoDataValue() != null &&
                         Precision.equals( readValue,
                                           series.getVariable()
                                                 .getNoDataValue(),
                                           EPSILON ) )
                    {
                        readValue = null;
                    }

                    this.addObservationValue( gageID,
                                              value.getDateTime(),
                                              readValue );
                }
                catch (SQLException e)
                {
                    throw new IOException( e );
                }
            }
        }

        LOGGER.info("A WaterML time series has been parsed for '{}'",
                    series.getSourceInfo().getSiteName());
    }

    private void addObservationValue( String gageID, String observationTime, Double value)
            throws SQLException
    {
        this.upsertValues.add(new UpsertValue( gageID, observationTime, value ));

        if ( this.upsertValues.size() >= SystemSettings.maximumDatabaseInsertStatements())
        {
            this.performObservationUpserts();
        }
    }

    private void performObservationUpserts() throws SQLException
    {
        if (this.upsertValues.size() > 0)
        {
            List<Object[]> values = new ArrayList<>();

            while (!upsertValues.empty())
            {
                values.add( upsertValues.pop().getParameters() );
            }

            StatementRunner statementRunner = new StatementRunner(
                    WaterMLSource.OBSERVATION_UPSERT,
                    values
            );

            Database.ingest(statementRunner);

            LOGGER.trace("WaterML data has been submitted to the database queue");
        }
    }

    private int getVariablePositionID(String gageId) throws SQLException
    {
        if (!this.variablePositionIDs.containsKey( gageId ))
        {
            FeatureDetails feature = Features.getDetailsByGageID( gageId );
            this.variablePositionIDs.put(
                    gageId,
                    feature.getVariablePositionID( this.getVariableId() )
            );
        }
        return this.variablePositionIDs.get(gageId);
    }

    private int getSourceId()
    {
        return this.sourceId;
    }

    @Override
    public int getMeasurementunitId()
    {
        return this.waterMLMeasurementId;
    }

    @Override
    protected String getHash() throws IOException
    {
        return DataSources.getHash( this.getSourceId() );
    }

    private static String writeObservationUpsert()
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("WITH upsert AS");
        script.addLine("(");
        script.addTab().addLine("UPDATE wres.Observation");
        script.addTab(  2  ).addLine("SET observed_value = ?");
        script.addTab().addLine("WHERE variableposition_id = ?");
        script.addTab(  2  ).addLine("AND observation_time = (?)::timestamp without time zone");
        script.addTab(  2  ).addLine("AND measurementunit_id = ?");
        script.addTab(  2  ).addLine("AND source_id = ?");
        script.addTab().addLine("RETURNING *");
        script.addLine(")");
        script.addLine("INSERT INTO wres.Observation (");
        script.addTab().addLine("variableposition_id,");
        script.addTab().addLine("observation_time,");
        script.addTab().addLine("observed_value,");
        script.addTab().addLine("measurementunit_id,");
        script.addTab().addLine("source_id");
        script.addLine(")");
        script.addLine("SELECT ?, (?)::timestamp without time zone, ?, ?, ?");
        script.addLine("WHERE NOT EXISTS (");
        script.addTab().addLine("SELECT 1");
        script.addTab().addLine("FROM upsert U");
        script.addTab().addLine("WHERE U.variableposition_id = ?");
        script.addTab(  2  ).addLine("AND U.observation_time = (?)::timestamp without time zone");
        script.addTab(  2  ).addLine("AND U.measurementunit_id = ?");
        script.addTab(  2  ).addLine("AND U.source_id = ?");
        script.add(");");

        return script.toString();
    }

    @Override
    protected Logger getLogger()
    {
        return WaterMLSource.LOGGER;
    }
}
