package wres.io.reading.waterml;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.util.Collections;
import wres.util.TimeHelper;

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
                    getVariablePositionID(gageID),
                    this.observationTime,
                    getMeasurementunitId(),
                    sourceId,
                    getVariablePositionID(gageID),
                    this.observationTime,
                    this.value,
                    getMeasurementunitId(),
                    sourceId,
                    getVariablePositionID(gageID),
                    this.observationTime,
                    getMeasurementunitId(),
                    sourceId
            };
        }

        private final String observationTime;
        private final Double value;
        private final String gageID;
    }

    private String operationStartTime;
    private final int sourceId;
    private final Response waterML;
    private final SortedMap<String, Integer> variablePositionIDs;


    protected WaterMLSource( ProjectConfig projectConfig, Response waterML, int sourceId )
    {
        super( projectConfig );
        this.waterML = waterML;
        this.variablePositionIDs = new TreeMap<>();
        this.sourceId = sourceId;
    }

    @Override
    public List<IngestResult> saveObservation() throws IOException
    {
        this.operationStartTime = TimeHelper.convertDateToString( LocalDateTime.now() );

        if ( this.waterML == null ||
             Collections.exists(this.waterML.getValue().getTimeSeries(), series -> series.getValues() == null || series.getValues().length == 0))
        {
            throw new NoDataException( "No WaterML data could be loaded with the given configuration." );
        }

        for (TimeSeries series : this.waterML.getValue().getTimeSeries())
        {
            this.readSeries( series );
        }

        return super.save();
    }

    private void readSeries( TimeSeries series )
    {

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
