package wres.io.reading.commaseparated;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeSeries;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.TimeSeriesValues;
import wres.io.utilities.DataProvider;
import wres.util.Strings;
import wres.util.TimeHelper;

public class CSVSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CSVSource.class );

    /**
     * Constructor that sets the filename
     * @param projectConfig the ProjectConfig causing ingest
     * @param filename The name of the source file
     */
    public CSVSource( ProjectConfig projectConfig,
                       String filename )
    {
        super( projectConfig );
        this.setFilename(filename);
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        return super.saveObservation();
    }

    @Override
    protected List<IngestResult> saveForecast() throws IOException
    {
        try
        {
            sourceDetails = DataSources.get( this.getFilename(), Instant.now().toString(), null, this.getHash());
        }
        catch ( SQLException e )
        {
            throw new IOException( "Metadata about the file at '" + this.getFilename() + "' could not be created.", e );
        }

        if (sourceDetails.performedInsert())
        {
            DataProvider data;

            if (this.getSourceConfig().isHasHeader())
            {
                data = DataProvider.fromCSV( this.getFilename() );
            }
            else
            {
                data = DataProvider.fromCSV(
                        this.filename,
                        "start_date",
                        "value_date",
                        "variable_name",
                        "location",
                        "measurement_unit",
                        "value",
                        "ensemble_name",
                        "qualifier_id",
                        "ensemblemember_id"
                );
            }

            parseTimeSeries( data );
        }

        return IngestResult.singleItemListFrom(
                this.getProjectConfig(),
                this.getDataSourceConfig(),
                this.getHash(),
                this.getFilename(),
                !sourceDetails.performedInsert()
        );
    }

    private void parseTimeSeries(final DataProvider data) throws IOException
    {
        TimeSeries currentTimeSeries = null;

        while (data.next())
        {
            try
            {
                this.validateDataProvider( data );

                Instant start = data.getInstant( "start_date" );
                Instant valueDate = data.getInstant( "value_date" );
                Double value = data.getDouble( "value" );
                int lead = (int)TimeHelper.durationToLeadUnits( Duration.between( start, valueDate ) );

                if ( currentTimeSeries == null ||
                     !currentTimeSeries.getInitializationDate().equals( start.toString() ) )
                {
                    currentTimeSeries = formTimeSeries( data, lead );
                }


                TimeSeriesValues.add( currentTimeSeries.getTimeSeriesID(), lead, value );
            }
            catch (SQLException e)
            {
                throw new IOException( "Metadata needed to save time series values could not be loaded.", e );
            }
        }
    }

    private void validateDataProvider(final DataProvider dataProvider) throws IngestException
    {
        String prefix = "Validation error(s) on line " +
                        (dataProvider.getRowIndex() + 1) +
                        " in '" +
                        this.getFilename() +
                        "'" +
                        System.lineSeparator();
        String suffix = System.lineSeparator() + "'" + this.getFilename() + "' cannot be ingested.";
        StringJoiner errorJoiner = new StringJoiner(
                System.lineSeparator(),
                prefix,
                suffix
        );
        boolean valid = true;
        boolean hasColumn;
        if ( ConfigHelper.isForecast( this.getDataSourceConfig() ))
        {
            hasColumn = dataProvider.hasColumn( "start_date" );

            if (!hasColumn)
            {
                valid = false;
                errorJoiner.add( "The provided csv is missing a 'start_date' column." );
            }
            else if (!Strings.hasValue( dataProvider.getString( "start_date" )))
            {
                errorJoiner.add("The provided csv is missing valid 'start_date' data.");
                valid = false;
            }
            else
            {
                try
                {
                    dataProvider.getInstant( "start_date" );
                }
                catch ( DateTimeParseException | ClassCastException e )
                {
                    errorJoiner.add("The provided csv has invalid data within the 'start_date' column.");
                }
            }
        }

        hasColumn = dataProvider.hasColumn( "value_date" );

        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'value_date' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "value_date" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'value_date' data.");
            valid = false;
        }
        else
        {
            try
            {
                dataProvider.getInstant( "value_date" );
            }
            catch ( DateTimeParseException | ClassCastException e )
            {
                errorJoiner.add("The provided csv has invalid data within the 'value_date' column.");
            }
        }

        hasColumn = dataProvider.hasColumn( "variable_name" );


        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'variable_name' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "variable_name" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'variable_name' data.");
            valid = false;
        }
        else if (!dataProvider.getString( "variable_name" )
                              .equalsIgnoreCase( this.getDataSourceConfig().getVariable().getValue() ))
        {
            valid = false;
            String foundVariable = dataProvider.getString( "variable_name" );
            errorJoiner.add( "The variable in the provided csv ('" +
                             foundVariable +
                             "') doesn't match the configured variable ('" +
                             this.getDataSourceConfig().getVariable().getValue() +
                             "')" );
        }

        hasColumn = dataProvider.hasColumn( "location" );


        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'location' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "location" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'location' data.");
            valid = false;
        }

        hasColumn = dataProvider.hasColumn( "measurement_unit" );

        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'measurement_unit' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "measurement_unit" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'measurement_unit' data.");
            valid = false;
        }

        hasColumn = dataProvider.hasColumn( "value" );

        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'value' column." );
        }
        else
        {
            try
            {
                dataProvider.getDouble( "value" );
            }
            catch ( ClassCastException e )
            {
                errorJoiner.add("The provided csv has invalid data within the 'value' column.");
            }
        }

        if (!valid)
        {
            throw new IngestException( errorJoiner.toString() );
        }
    }

    private TimeSeries formTimeSeries(final DataProvider data, final int timeStep) throws SQLException
    {
        TimeSeries timeseries = new TimeSeries(
                this.sourceDetails.getId(),
                data.getInstant( "start_date" ).toString()
        );

        String variable = data.getString( "variable_name" );
        String location = data.getString( "location" );
        String measurementUnit = data.getString( "measurement_unit" );

        String ensembleName = "default";
        String qualifierID = null;
        String ensembleMemberID = null;

        if (data.hasColumn("ensemble_name"))
        {
            ensembleName = data.getString("ensemble_name");
        }

        if (data.hasColumn("qualifier_id"))
        {
            qualifierID = data.getString("qualifier_id");
        }

        if (data.hasColumn("ensemblemember_id"))
        {
            ensembleMemberID = data.getString("ensemblemember_id");
        }

        Integer ensembleID = Ensembles.getEnsembleID(ensembleName, ensembleMemberID, qualifierID);

        timeseries.setEnsembleID(ensembleID);

        timeseries.setMeasurementUnitID(
                MeasurementUnits.getMeasurementUnitID(measurementUnit)
        );

        Integer variableFeatureId = Features.getVariableFeatureIDByLID(
                location,
                Variables.getVariableID(variable)
        );

        timeseries.setVariableFeatureID( variableFeatureId );
        timeseries.setScalePeriod( 1 );
        timeseries.setTimeStep( timeStep );

        return timeseries;
    }

    private SourceDetails sourceDetails;

    @Override
    protected Logger getLogger()
    {
        return CSVSource.LOGGER;
    }
}
