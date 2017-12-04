package wres.io.reading.waterml;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

import wres.io.concurrency.StatementRunner;
import wres.io.config.SystemSettings;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.details.FeatureDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.Time;

public class WaterMLSource extends BasicSource
{

    private static final String UPSERT =
            "WITH upsert AS" + System.lineSeparator() +
            "(" + System.lineSeparator() +
            "    UPDATE wres.Observation" + System.lineSeparator() +
            "        SET observed_value = ?" + System.lineSeparator() +
            "    WHERE variableposition_id = ?" + System.lineSeparator() +
            "        AND observation_time = (?)::timestamp without time zone" + System.lineSeparator() +
            "        AND measurementunit_id = ?" + System.lineSeparator() +
            "        AND source_id = ?" + System.lineSeparator() +
            "    RETURNING *" + System.lineSeparator() +
            ")" + System.lineSeparator() +
            "INSERT INTO wres.Observation (" + System.lineSeparator() +
            "    variableposition_id," + System.lineSeparator() +
            "    observation_time," + System.lineSeparator() +
            "    observed_value," + System.lineSeparator() +
            "    measurementunit_id," + System.lineSeparator() +
            "    source_id" + System.lineSeparator() +
            ")" + System.lineSeparator() +
            "SELECT ?, (?)::timestamp without time zone, ?, ?, ?" + System.lineSeparator() +
            "WHERE NOT EXISTS (" + System.lineSeparator() +
            "    SELECT 1" + System.lineSeparator() +
            "    FROM upsert U" + System.lineSeparator() +
            "    WHERE U.variableposition_id = ?" + System.lineSeparator() +
            "        AND U.observation_time = (?)::timestamp without time zone" + System.lineSeparator() +
            "        AND U.measurementunit_id = ?" + System.lineSeparator() +
            "        AND U.source_id = ?" + System.lineSeparator() +
            ");" + System.lineSeparator();

    private class UpsertValue
    {
        public UpsertValue(String gageID, String observationTime, Double value)
        {
            this.gageID = gageID;
            this.observationTime = observationTime;
            this.value = value;
        }

        public Object[] getParameters() throws SQLException
        {
            return new Object[] {
                    this.value,
                    getVariablePositionID(gageID),
                    this.observationTime,
                    getMeasurementUnitID(),
                    sourceID,
                    getVariablePositionID(gageID),
                    this.observationTime,
                    this.value,
                    getMeasurementUnitID(),
                    sourceID,
                    getVariablePositionID(gageID),
                    this.observationTime,
                    getMeasurementUnitID(),
                    sourceID
            };
        }

        private final String observationTime;
        private final Double value;
        private final String gageID;
    }

    public WaterMLSource(Response waterML, Integer sourceID, String measurementUnit)
    {
        this.waterML = waterML;
        this.sourceID = sourceID;
        this.measurementUnit = measurementUnit;
    }

    @Override
    public List<String> saveObservation() throws IOException
    {

        List<String> foundLocations = new ArrayList<>(  );

        for (TimeSeries series : this.waterML.getValue().getTimeSeries())
        {
            foundLocations.add(series.getSourceInfo().getSiteCode()[0].getValue());
            this.readSeries( series );
        }

        try
        {
            enforceValidFeatures( foundLocations );
        }
        catch ( SQLException e )
        {
            throw new IOException( "Locations without official data from the " +
                                   "USGS could not be removed from the total " +
                                   "list of locations to evaluate.", e );
        }

        try
        {
            this.performUpserts();
        }
        catch ( SQLException e )
        {
            throw new IOException( "USGS observations could not be saved.", e );
        }
        return foundLocations;
    }

    private void enforceValidFeatures(List<String> foundLocations)
            throws SQLException
    {
        List<FeatureDetails> invalidFeatures = new ArrayList<>();

        for (FeatureDetails details : this.getProjectDetails().getFeatures())
        {
            if (!foundLocations.contains( details.getGageID() ))
            {
                invalidFeatures.add( details );
            }
        }

        if (invalidFeatures.size() > 0)
        {
            this.getProjectDetails().getFeatures().removeAll( invalidFeatures );
        }
    }

    private Integer getVariablePositionID(String gageID) throws SQLException
    {
        if (this.variablePositionIDs == null)
        {
            this.variablePositionIDs = new TreeMap<>(  );
        }

        if (!this.variablePositionIDs.containsKey( gageID ))
        {
            FeatureDetails details =
                    Collections.find( this.getProjectDetails().getFeatures(),
                                      feature -> feature.getGageID() != null &&
                                                 feature.getGageID().equalsIgnoreCase( gageID ) );

            this.variablePositionIDs.put(gageID,
                                         details.getVariablePositionID( this.getVariableID() ));
        }

        return this.variablePositionIDs.get( gageID );
    }

    private Integer getVariableID() throws SQLException
    {
        if (this.variableID == null)
        {
            if (this.getProjectDetails().getLeft().equals( this.getDataSourceConfig() ))
            {
                this.variableID = this.getProjectDetails().getLeftVariableID();
            }
            else if (this.getProjectDetails().getRight().equals( this.getDataSourceConfig() ))
            {
                this.variableID = this.getProjectDetails().getRightVariableID();
            }
            else
            {
                this.variableID = this.getProjectDetails().getBaselineVariableID();
            }
        }

        return this.variableID;
    }

    private void addValue(String gageID, String observationTime, Double value)
            throws SQLException
    {
        this.upsertValues.add(
                new WaterMLSource.UpsertValue( gageID,
                                            Time.normalize( observationTime ),
                                            value )
        );

        if ( this.upsertValues.size() >= SystemSettings.maximumDatabaseInsertStatements())
        {
            this.performUpserts();
        }
    }

    private void readSeries(TimeSeries series) throws IOException
    {
        String gageID = series.getSourceInfo().getSiteCode()[0].getValue();

        if (series.getValues().length == 0)
        {
            FeatureDetails invalidFeature = null;

            try
            {
                for (FeatureDetails feature : this.getProjectDetails().getFeatures())
                {
                    if ( Strings.hasValue( feature.getGageID() ) && feature.getGageID().equalsIgnoreCase( gageID ))
                    {
                        invalidFeature = feature;
                        break;
                    }
                }

                this.getProjectDetails().getFeatures().remove( invalidFeature );
                return;
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not iterate through available " +
                                       "features in order to avoid processing " +
                                       "the invalid location '" + gageID + "'" ,
                                       e);
            }
        }

        for (TimeSeriesValues valueSet : series.getValues())
        {
            if (valueSet.getValue().length == 0)
            {
                FeatureDetails invalidFeature = null;

                try
                {
                    for (FeatureDetails feature : this.getProjectDetails().getFeatures())
                    {
                        if (Strings.hasValue( feature.getGageID() ) && feature.getGageID().equalsIgnoreCase( gageID ))
                        {
                            invalidFeature = feature;
                            break;
                        }
                    }

                    this.getProjectDetails().getFeatures().remove( invalidFeature );
                    return;
                }
                catch ( SQLException e )
                {
                    throw new IOException( "Could not iterate through available " +
                                           "features in order to avoid processing " +
                                           "the invalid location '" + gageID + "'" ,
                                           e);
                }
            }

            for (TimeSeriesValue value : valueSet.getValue())
            {
                try
                {
                    this.addValue( series.getSourceInfo().getSiteCode()[0].getValue(),
                                   value.getDateTime(),
                                   value.getValue() );
                }
                catch ( SQLException e )
                {
                    throw new IOException( e );
                }
            }
        }
    }

    private void performUpserts() throws SQLException
    {
        if (this.upsertValues.size() > 0)
        {
            List<Object[]> values = new ArrayList<>(  );

            while (!upsertValues.empty())
            {
                values.add( upsertValues.pop().getParameters() );
            }

            StatementRunner
                    statement = new StatementRunner( UPSERT, values );
            statement.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            Database.ingest( statement);

            ProgressMonitor.increment();
        }
    }

    private Integer getMeasurementUnitID() throws SQLException
    {
        if (this.measurementUnitID == null)
        {
            this.measurementUnitID = MeasurementUnits.getMeasurementUnitID( this.measurementUnit );
        }
        return this.measurementUnitID;
    }

    // TODO: Implement
    public void updateFeature(FeatureDetails feature, TimeSeries series)
    {
        // USGS data is relatively static, but changes every once in a while.
        // It's usually nothing huge; generally something like its coordinates.
        // If the coordinates DO change, we might need to update that information.
        // Similarly, we may also need to update the HUC.
        //
        // This should only be performed if the series information has information
        // that the feature doesn't.
    }

    private final Response waterML;
    private final Integer sourceID;
    private final String measurementUnit;

    private final Stack<UpsertValue> upsertValues = new Stack<>();
    private Integer variableID;
    private Map<String, Integer> variablePositionIDs;
    private Integer measurementUnitID;
}
