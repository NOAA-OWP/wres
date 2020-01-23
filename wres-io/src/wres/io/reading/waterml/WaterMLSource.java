package wres.io.reading.waterml;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.math3.util.Precision.EPSILON;

import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScale;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.SourceCompleter;
import wres.io.reading.waterml.timeseries.Method;
import wres.io.reading.waterml.timeseries.SiteCode;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.system.DatabaseLockManager;

/**
 * Saves WaterML Response objects to the database
 */
public class WaterMLSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(
            WaterMLSource.class);

    private final Response waterML;
    private final String hash;
    private final SortedMap<Pair<String,String>, Integer> variableFeatureIDs = new TreeMap<>();
    private final Map<String,Integer> measurementIds = new HashMap<>( 1 );

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final Set<Pair<CountDownLatch,CountDownLatch>> latches = new HashSet<>();

    /**
     * @param projectConfig the project declaration
     * @param dataSource the data source information
     * @param lockManager the lock manager to use
     * @param waterML the deserialized waterML object
     * @param hash the identifier of the waterML object
     */

    public WaterMLSource( ProjectConfig projectConfig,
                          DataSource dataSource,
                          DatabaseLockManager lockManager,
                          Response waterML,
                          String hash )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );
        Objects.requireNonNull( waterML );
        Objects.requireNonNull( hash );
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.waterML = waterML;
        this.hash = hash;
    }


    IngestResult ingestObservationResponse() throws IOException
    {
        int readSeriesCount = 0;

        SourceDetails.SourceKey sourceKey =
                new SourceDetails.SourceKey( this.dataSource.getUri(),
                                             Instant.now().toString(),
                                             null,
                                             this.hash );

        SourceDetails sourceDetails = this.createSourceDetails( sourceKey );

        try
        {
            sourceDetails.save();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to ingest data source "
                                       + sourceKey.getSourcePath(),
                                       se );
        }

        SourceCompleter sourceCompleter =
                this.createSourceCompleter( sourceDetails.getId(),
                                            this.lockManager );
        boolean sourceCompleted;

        if ( sourceDetails.performedInsert() )
        {
            try
            {
                lockManager.lockSource( sourceDetails.getId() );
            }
            catch ( SQLException se )
            {
                throw new IngestException( "Unable to lock to ingest source id "
                                           + sourceDetails.getId() + " named "
                                           + this.dataSource.getUri(), se );
            }

            if ( this.waterML.getValue().getNumberOfPopulatedTimeSeries() > 0 )
            {
                for ( TimeSeries series : this.waterML.getValue()
                                                      .getTimeSeries() )
                {
                    boolean validSeries = this.readObservationSeries( series,
                                                                      sourceDetails );

                    if ( validSeries )
                    {
                        readSeriesCount++;
                    }
                }
            }

            sourceCompleter.complete( this.latches );
            sourceCompleted = true;
        }
        else
        {
            sourceCompleted = this.wasCompleted( sourceDetails );
        }

        return IngestResult.from( this.projectConfig,
                                  this.dataSource,
                                  this.hash,
                                  !sourceDetails.performedInsert(),
                                  !sourceCompleted );
    }

    private boolean readObservationSeries( TimeSeries series,
                                           SourceDetails sourceDetails )
            throws IOException
    {
        if (!series.isPopulated())
        {
            return false;
        }

        // Get the first variable name from the series in the actual data.
        if ( series.getVariable() == null
             || series.getVariable().getVariableCode() == null
             || series.getVariable().getVariableCode().length < 1
             || series.getVariable().getVariableCode()[0].getValue() == null )
        {
            LOGGER.debug( "No variable found for timeseries {} in source {}",
                          series, this );
            return false;
        }

        String variableName = series.getVariable()
                                    .getVariableCode()[0]
                                    .getValue();

        // Get the measurement unit from the series in the actual data.
        // (Assumes above check for variable has already happened as well)
        if ( series.getVariable().getUnit() == null
             || series.getVariable().getUnit().getUnitCode() == null )
        {
            LOGGER.warn( "No unit found for timeseries {} in source {}",
                          series, this );
            return false;
        }

        String unitCode = series.getVariable()
                                .getUnit()
                                .getUnitCode();

        if ( series.getSourceInfo() == null
             || series.getSourceInfo().getSiteCode() == null
             || series.getSourceInfo().getSiteCode().length < 1 )
        {
            LOGGER.debug( "No unit code found for timeseries {} in source {}",
                          series, this );
            return false;
        }

        List<String> usgsSiteCodesFound = new ArrayList<>( 1 );

        for ( SiteCode siteCode : series.getSourceInfo().getSiteCode() )
        {
            if ( siteCode.getAgencyCode() != null
                 && siteCode.getAgencyCode()
                            .toLowerCase()
                            .equals( "usgs" )
                 && siteCode.getValue() != null )
            {
                usgsSiteCodesFound.add( siteCode.getValue() );
            }
        }

        if ( usgsSiteCodesFound.size() != 1 )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Expected exactly one USGS site code, but found {} for timeseries {} in source {}",
                              usgsSiteCodesFound.size(),
                              series,
                              this );
            }
            return false;
        }

        String usgsSiteCode = usgsSiteCodesFound.get( 0 );
        Pair<String,String> gageAndVariable = Pair.of( usgsSiteCode, variableName );
        TimeScale.TimeScaleFunction function = TimeScale.TimeScaleFunction.UNKNOWN;
        Duration period = null;

        // Assume that USGS "IV" service implies "instantaneous" values, which
        // we model as having a period of 1 minute due to 1 minute being the
        // finest forecasted-value time-step granularity as of this commit.
        // Perhaps this could be refactored into a isNwisIvService() method if
        // it is needed more than once.
        if ( dataSource.getUri()
                       .toString()
                       .contains( "usgs.gov/nwis/iv" ) )
        {
            period = Duration.ofMinutes( 1 );
        }

        int countOfTracesFound = series.getValues().length;

        if ( countOfTracesFound > 1 && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Skipping site {} because multiple timeseries for variable {} from USGS NWIS URI {}",
                         usgsSiteCode, variableName, this.dataSource.getUri() );
            return false;
        }

        for (TimeSeriesValues valueSet : series.getValues())
        {
            if (valueSet.getValue().length == 0)
            {
                continue;
            }

            Method[] methods = valueSet.getMethod();

            if ( Objects.nonNull( methods ) && methods.length > 0 )
            {
                for ( Method method : methods )
                {
                    LOGGER.debug( "Found method id={} description='{}'",
                                  method.getMethodID(),
                                  method.getMethodDescription() );
                }
            }

            for (TimeSeriesValue value : valueSet.getValue())
            {
                try
                {
                    Double readValue = value.getValue();

                    if (series.getVariable().getNoDataValue() != null &&
                        Precision.equals( readValue, series.getVariable().getNoDataValue(), EPSILON))
                    {
                        readValue = null;
                    }

                    this.addObservationValue( gageAndVariable,
                                              value.getDateTime(),
                                              readValue,
                                              sourceDetails,
                                              unitCode,
                                              function,
                                              period );
                }
                catch ( SQLException e )
                {
                    throw new IOException( e );
                }
            }
        }

        LOGGER.info( "A USGS time series has been parsed for site number {}",
                     usgsSiteCode );

        return true;
    }

    private void addObservationValue( Pair<String,String> gageAndVariable,
                                      Instant observationTime,
                                      Double value,
                                      SourceDetails sourceDetails,
                                      String unitCode,
                                      TimeScale.TimeScaleFunction scaleFunction,
                                      Duration scalePeriod )
            throws SQLException, IngestException
    {
        int variableFeatureId = this.getVariableFeatureID( gageAndVariable );
        int measurementUnitId = this.getMeasurementUnitId( unitCode );
        Pair<CountDownLatch, CountDownLatch> synchronizer =
                IngestedValues.observed( value )
                              .measuredIn( measurementUnitId )
                              .at( observationTime )
                              .forVariableAndFeatureID( variableFeatureId )
                              .inSource( sourceDetails.getId() )
                              .scaledBy( scaleFunction )
                              .scaleOf( scalePeriod )
                              .add();
        this.latches.add( synchronizer );
    }


    private int getVariableFeatureID( Pair<String,String> gageAndVariable )
            throws SQLException
    {

        if ( !this.variableFeatureIDs.containsKey( gageAndVariable ) )
        {
            FeatureDetails feature = Features.getDetailsByGageID( gageAndVariable.getLeft() );
            int variableId = Variables.getVariableID( gageAndVariable.getRight() );
            this.variableFeatureIDs.put(
                    gageAndVariable,
                    Features.getVariableFeatureByFeature( feature, variableId )
            );
        }
        return this.variableFeatureIDs.get( gageAndVariable );
    }


    private int getMeasurementUnitId( String unitCode )
            throws SQLException
    {
        if ( !this.measurementIds.containsKey( unitCode ) )
        {
            Integer measurementId = MeasurementUnits.getMeasurementUnitID( unitCode );
            this.measurementIds.put( unitCode, measurementId );
        }

        return this.measurementIds.get( unitCode );
    }


    /**
     * Discover whether the source was completely ingested
     * @param sourceDetails the source to query
     * @throws IngestException when discovery fails due to SQLException
     * @return true if the source has been marked as completed, false otherwise
     */

    private boolean wasCompleted( SourceDetails sourceDetails )
            throws IngestException
    {
        SourceCompletedDetails completed =
                this.createSourceCompletedDetails( sourceDetails );

        try
        {
            return completed.wasCompleted();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed discover whether source "
                                       + sourceDetails + " was completed.",
                                       se );
        }
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceKey the first arg to SourceDetails
     * @return a SourceDetails
     */

    SourceDetails createSourceDetails( SourceDetails.SourceKey sourceKey )
    {
        return new SourceDetails( sourceKey );
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceId the first arg to SourceCompleter
     * @param lockManager the second arg to SourceCompleter
     * @return a SourceCompleter
     */
    SourceCompleter createSourceCompleter( int sourceId,
                                           DatabaseLockManager lockManager )
    {
        return new SourceCompleter( sourceId, lockManager );
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceDetails the first arg to SourceCompletedDetails
     * @return a SourceCompleter
     */
    SourceCompletedDetails createSourceCompletedDetails( SourceDetails sourceDetails )
    {
        return new SourceCompletedDetails( sourceDetails );
    }
}
