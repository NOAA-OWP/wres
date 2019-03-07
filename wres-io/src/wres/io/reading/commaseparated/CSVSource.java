package wres.io.reading.commaseparated;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import wres.io.reading.IngestedValues;
import wres.io.utilities.DataProvider;
import wres.util.LRUContainer;
import wres.util.Strings;
import wres.util.TimeHelper;

public class CSVSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CSVSource.class );

    // It's probably worth making this configurable
    private static final String DELIMITER = ",";

    private static final int TIME_SERIES_LIMIT = 60;

    private SourceDetails sourceDetails;

    /**
     * A container holding referenced TimeSeries entries
     */
    private LRUContainer<TimeSeries> encounteredTimeSeries;

    /**
     * Constructor that sets the filename
     * @param projectConfig the ProjectConfig causing ingest
     * @param filename The name of the source file
     */
    public CSVSource( ProjectConfig projectConfig,
                      URI filename )
    {
        super( projectConfig );
        this.setFilename(filename);
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
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
                data = DataProvider.fromCSV( this.getFilename(), DELIMITER );
            }
            else
            {
                data = DataProvider.fromCSV(
                        this.getFilename(),
                        DELIMITER,
                        "value_date",
                        "variable_name",
                        "location",
                        "measurement_unit",
                        "value"
                );
            }

            this.parseObservations( data );
        }

        return IngestResult.singleItemListFrom(
                this.getProjectConfig(),
                this.getDataSourceConfig(),
                this.getHash(),
                this.getFilename(),
                !sourceDetails.performedInsert()
        );
    }

    @Override
    protected List<IngestResult> saveForecast() throws IOException
    {
        if (encounteredTimeSeries == null)
        {
            encounteredTimeSeries = new LRUContainer<>( TIME_SERIES_LIMIT );
        }

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
                data = DataProvider.fromCSV( this.getFilename(), DELIMITER );
            }
            else
            {
                data = DataProvider.fromCSV(
                        this.filename,
                        DELIMITER,
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
        TimeSeries currentTimeSeries;

        while (data.next())
        {
            try
            {
                this.validateDataProvider( data );

                Instant start = data.getInstant( "start_date" );
                Instant valueDate = data.getInstant( "value_date" );
                Double value = data.getDouble( "value" );
                int ensembleId = this.getEnsembleId( data );
                int lead = (int) TimeHelper.durationToLongUnits( Duration.between( start, valueDate ),
                                                                 TimeHelper.LEAD_RESOLUTION );

                currentTimeSeries = formTimeSeries( data, ensembleId);

                IngestedValues.addTimeSeriesValue( currentTimeSeries.getTimeSeriesID(), lead, value );
            }
            catch (SQLException e)
            {
                throw new IOException( "Metadata needed to save time series values could not be loaded.", e );
            }
        }
    }

    private int getEnsembleId(final DataProvider data) throws SQLException
    {
        String ensembleName = "default";
        String qualifierID = null;
        Integer ensembleMemberID = null;

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
            ensembleMemberID = data.getInt("ensemblemember_id");
        }

        return Ensembles.getEnsembleID(ensembleName, ensembleMemberID, qualifierID);
    }

    private void parseObservations(final DataProvider data) throws IOException
    {
        while (data.next())
        {
            this.validateDataProvider( data );

            Instant valueDate = data.getInstant( "value_date" );
            Double value = data.getDouble( "value" );

            String variable = data.getString( "variable_name" );
            String location = data.getString( "location" );
            String measurementUnit = data.getString( "measurement_unit" );
            Integer measurementUnitId;

            try
            {
                measurementUnitId = MeasurementUnits.getMeasurementUnitID( measurementUnit );
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not determine the ID for the measurement unit '" +
                                       measurementUnit + "'", e );
            }

            Integer variableFeatureId;

            try
            {
                variableFeatureId = Features.getVariableFeatureIDByLID(
                        location,
                        Variables.getVariableID(variable)
                );
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not determine the metadata about where '" +
                                       location + "' and the variable '" + variable +
                                       "' intersect that are needed to save observations from " +
                                       this.getFilename(), e );
            }

            IngestedValues.observed( value )
                          .at(valueDate)
                          .measuredIn( measurementUnitId )
                          .forVariableAndFeatureID( variableFeatureId )
                          .inSource( this.sourceDetails.getId() )
                          .add();
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

    private TimeSeries formTimeSeries(final DataProvider data, final int ensembleId) throws SQLException
    {
        final String variable = data.getString( "variable_name" );
        final String location = data.getString( "location" );
        final String measurementUnit = data.getString( "measurement_unit" );
        final String startDate = data.getInstant( "start_date" ).toString();

        final Integer variableFeatureId = Features.getVariableFeatureIDByLID(
                location,
                Variables.getVariableID(variable)
        );

        TimeSeries timeSeries = this.encounteredTimeSeries.get(
                series -> series.getEnsembleId() == ensembleId &&
                          series.getInitializationDate().equals( startDate ) &&
                          series.getVariableFeatureID().equals( variableFeatureId )
        );

        if (timeSeries != null)
        {
            return timeSeries;
        }

        timeSeries = new TimeSeries(
                this.sourceDetails.getId(),
                startDate
        );

        timeSeries.setEnsembleID(ensembleId);

        timeSeries.setMeasurementUnitID(
                MeasurementUnits.getMeasurementUnitID(measurementUnit)
        );

        timeSeries.setVariableFeatureID( variableFeatureId );
        
        // Time scale information is missing by default        
        //timeSeries.setScalePeriod( 1 );

        this.encounteredTimeSeries.add(timeSeries);

        return timeSeries;
    }

    @Override
    protected Logger getLogger()
    {
        return CSVSource.LOGGER;
    }
}
