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

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeSeries;
import wres.io.reading.BasicSource;
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
            DataProvider data = DataProvider.fromCSV(
                    this.filename,
                    true,
                    "start_date", "value_date", "variable_name", "location", "measurement_unit", "value"
            );

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
                Instant start = data.getInstant( "start_date" );
                Instant valueDate = data.getInstant( "value_date" );
                String variable = data.getString( "variable_name" );
                String location = data.getString( "location" );
                String measurementUnit = data.getString( "measurement_unit" );
                Double value = data.getDouble( "value" );
                int lead = (int)TimeHelper.durationToLeadUnits( Duration.between( start, valueDate ) );

                if ( currentTimeSeries == null ||
                     !currentTimeSeries.getInitializationDate().equals( start.toString() ) )
                {
                    currentTimeSeries = new TimeSeries( this.sourceDetails.getId(), start.toString() );

                    currentTimeSeries.setEnsembleID(
                            Ensembles.getEnsembleID( "default", null, null )
                    );
                    currentTimeSeries.setMeasurementUnitID(
                            MeasurementUnits.getMeasurementUnitID(measurementUnit)
                    );

                    Integer variableFeatureId = Features.getVariableFeatureIDByLID(
                            location,
                            Variables.getVariableID(variable)
                    );

                    currentTimeSeries.setVariableFeatureID( variableFeatureId );
                    currentTimeSeries.setScalePeriod( 1 );
                    currentTimeSeries.setTimeStep( lead );
                }


                TimeSeriesValues.add( currentTimeSeries.getTimeSeriesID(), lead, value );
            }
            catch (SQLException e)
            {
                throw new IOException( "", e );
            }
        }
    }

    private void validateDataProvider(final DataProvider dataProvider, final boolean isForecast)
    {
        StringJoiner errorJoiner = new StringJoiner( System.lineSeparator() );
        boolean valid = true;
        boolean hasColumn;
        if (isForecast)
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
                    Instant valueDate = dataProvider.getInstant( "start_date" );
                }
                catch ( DateTimeParseException e )
                {
                    errorJoiner.add("The provided csv has invalid data within the 'start_date' column.");
                }
            }
        }

        hasColumn = dataProvider.hasColumn( "value_date" );
    }

    private SourceDetails sourceDetails;

    @Override
    protected Logger getLogger()
    {
        return CSVSource.LOGGER;
    }
}
