package wres.io.reading.waterml;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.math3.util.Precision.EPSILON;

import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.PreIngestException;
import wres.io.reading.waterml.timeseries.GeographicLocation;
import wres.io.reading.waterml.timeseries.Method;
import wres.io.reading.waterml.timeseries.SiteCode;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;

/**
 * Tranforms parsed WaterML data into WRES TimeSeries data.
 */
class WaterMLSource implements Callable<List<TimeSeries<Double>>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(
            WaterMLSource.class);

    private final Response waterML;
    private final DataSource dataSource;

    /**
     * Create a WaterMLSource, call transformWaterML to transform to TimeSeries.
     *
     * @param dataSource The data source information
     * @param waterML The deserialized waterML object
     */

    public WaterMLSource( DataSource dataSource,
                          Response waterML )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( waterML );
        this.dataSource = dataSource;
        this.waterML = waterML;
    }

    @Override
    public List<TimeSeries<Double>> call()
    {
        List<wres.datamodel.time.TimeSeries<Double>> allTimeSeries =
                new ArrayList<>();

        if ( this.waterML.getValue().getNumberOfPopulatedTimeSeries() > 0 )
        {
            for ( wres.io.reading.waterml.timeseries.TimeSeries series :
                    this.waterML.getValue()
                                .getTimeSeries() )
            {
                List<wres.datamodel.time.TimeSeries<Double>> doubleTimeSerieses
                        = this.readObservationSeries( series );
                allTimeSeries.addAll( doubleTimeSerieses );
            }
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "{} USGS time series parsed from URL {}",
                          allTimeSeries.size(), this.dataSource.getUri() );
        }

        return Collections.unmodifiableList( allTimeSeries );
    }


    private List<TimeSeries<Double>> readObservationSeries( wres.io.reading.waterml.timeseries.TimeSeries series )
    {
        if (!series.isPopulated())
        {
            return Collections.emptyList();
        }

        // Get the first variable name from the series in the actual data.
        if ( series.getVariable() == null
             || series.getVariable().getVariableCode() == null
             || series.getVariable().getVariableCode().length < 1
             || series.getVariable().getVariableCode()[0].getValue() == null )
        {
            LOGGER.debug( "No variable found for timeseries {} in source {}",
                          series, this );
            return Collections.emptyList();
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
            return Collections.emptyList();
        }

        String unitCode = series.getVariable()
                                .getUnit()
                                .getUnitCode();

        if ( series.getSourceInfo() == null
             || series.getSourceInfo().getSiteCode() == null
             || series.getSourceInfo().getSiteCode().length < 1 )
        {
            LOGGER.debug( "No site code found for timeseries {} in source {}",
                          series, this );
            return Collections.emptyList();
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

            return Collections.emptyList();
        }

        String usgsSiteCode = usgsSiteCodesFound.get( 0 );
        FeatureKey featureKey = translateGeographicFeature( usgsSiteCode,
                                                            series );
        List<wres.datamodel.time.TimeSeries<Double>> timeSerieses =
                new ArrayList<>();
        TimeScaleOuter period = null;

        // Assume that USGS "IV" service implies "instantaneous" values, which
        // we model as having a period of 1 minute due to 1 minute being the
        // finest forecasted-value time-step granularity as of this commit.
        // Perhaps this could be refactored into a isNwisIvService() method if
        // it is needed more than once.
        if ( dataSource.getUri()
                       .toString()
                       .contains( "usgs.gov/nwis/iv" ) )
        {
            // Short-hand for declaring "instantaneous scale used by WRES"
            period = TimeScaleOuter.of();
        }

        int countOfTracesFound = series.getValues().length;

        if ( countOfTracesFound > 1 && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Skipping site {} because multiple timeseries for variable {} from USGS NWIS URI {}",
                         usgsSiteCode, variableName, this.dataSource.getUri() );
            return Collections.emptyList();
        }

        for (TimeSeriesValues valueSet : series.getValues())
        {
            Instant latestObservation = Instant.MIN;
            SortedSet<Event<Double>> rawTimeSeries = new TreeSet<>();

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

            LOGGER.trace( "Jackson parsed these for site {} variable {}: {}",
                          usgsSiteCode, variableName, valueSet );

            for ( TimeSeriesValue value : valueSet.getValue() )
            {
                double readValue = value.getValue();
                Double noDataValue = series.getVariable()
                                           .getNoDataValue();

                if ( Objects.nonNull( noDataValue )
                     && Precision.equals( readValue, noDataValue, EPSILON) )
                {
                    readValue = MissingValues.DOUBLE;
                }

                Instant dateTime = value.getDateTime();
                Event<Double> event = Event.of( dateTime, readValue );
                rawTimeSeries.add( event );

                if ( dateTime.isAfter( latestObservation ) )
                {
                    latestObservation = dateTime;
                }
            }

            Map<ReferenceTimeType,Instant> referenceTimes = Map.of( ReferenceTimeType.LATEST_OBSERVATION,
                                                                    latestObservation );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( referenceTimes,
                                                                 period,
                                                                 variableName,
                                                                 featureKey,
                                                                 unitCode );

            LOGGER.trace( "TimeSeries parsed for {}: {}", metadata, rawTimeSeries );

            try
            {
                wres.datamodel.time.TimeSeries<Double> timeSeries =
                        wres.datamodel.time.TimeSeries.of( metadata,
                                                           rawTimeSeries );
                timeSerieses.add( timeSeries );
            }
            catch ( IllegalArgumentException iae )
            {
                throw new PreIngestException( "While creating timeseries for site "
                                              + usgsSiteCode + " from URI "
                                              + this.dataSource.getUri()
                                              + ": ", iae );
            }
        }

        return Collections.unmodifiableList( timeSerieses );
    }


    /**
     * Translate the USGS format of geographic feature into WRES format.
     * @param usgsSiteCode The site code already found and validated.
     * @param series The series to get geographic feature data from.
     * @return a WRES feature
     */

    private FeatureKey translateGeographicFeature( String usgsSiteCode,
                                                   wres.io.reading.waterml.timeseries.TimeSeries series )
    {
        String siteDescription = null;
        Integer siteSrid = null;
        String siteWkt = null;
        GeographicLocation geographicLocation = null;

        if ( Objects.nonNull( series.getSourceInfo()
                                    .getGeoLocation() ) )
        {
            siteDescription = series.getSourceInfo()
                                    .getSiteName();

            geographicLocation = series.getSourceInfo()
                                       .getGeoLocation()
                                       .getGeogLocation();
        }

        if ( Objects.nonNull( geographicLocation ) )
        {
            if ( Objects.nonNull( geographicLocation.getSrs() ) )
            {
                String rawSrs = geographicLocation.getSrs()
                                                  .strip();

                if ( rawSrs.startsWith( "EPSG:" ) )
                {
                    String srid = rawSrs.substring( 5 );

                    try
                    {
                        siteSrid = Integer.valueOf( srid );
                    }
                    catch ( NumberFormatException nfe )
                    {
                        LOGGER.warn( "Unable to extract SRID from SRS {} in timeseries for site code {} at url {}",
                                     rawSrs,
                                     usgsSiteCode,
                                     this.dataSource.getUri() );
                    }
                }
            }

            if ( Objects.nonNull( geographicLocation.getLatitude() )
                 && Objects.nonNull( geographicLocation.getLongitude() ) )
            {
                StringJoiner point = new StringJoiner( " " );
                point.add( "POINT (" );

                // WKT is x,y aka lon,lat
                point.add( geographicLocation.getLongitude()
                                             .toString() );
                point.add( geographicLocation.getLatitude()
                                             .toString() );
                point.add( ")" );
                siteWkt = point.toString();
            }
        }

        return new FeatureKey( usgsSiteCode, siteDescription, siteSrid, siteWkt );
    }
}
