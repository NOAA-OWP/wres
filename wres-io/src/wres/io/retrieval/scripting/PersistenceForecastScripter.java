package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;

class PersistenceForecastScripter extends Scripter
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PersistenceForecastScripter.class );
    private static final String NEWLINE = System.lineSeparator();

    private final Instant zeroDate;
    private final Duration timeScaleWidth;
    private final int progress;
    private final int sequenceStep;

    PersistenceForecastScripter( ProjectDetails projectDetails,
                                 DataSourceConfig dataSourceConfig,
                                 Feature feature,
                                 int progress,
                                 int sequenceStep )
            throws SQLException, IOException
    {
        super( projectDetails, dataSourceConfig, feature, progress, sequenceStep );

        String zeroDate = this.getProjectDetails().getZeroDate(
                this.getDataSourceConfig(),
                this.getFeature() );
        String isoZeroDate = zeroDate.replace(" ", "T" )
                                     .replace( "'", "" )
                             + "Z";
        Instant usualZeroDate = Instant.parse( isoZeroDate );
        this.timeScaleWidth = ConfigHelper.getDurationFromTimeScale( projectDetails.getScale() );
        this.zeroDate = usualZeroDate.minus( this.timeScaleWidth );
        this.progress = super.getProgress();
        this.sequenceStep = super.getSequenceStep();
    }

    @Override
    String formScript()
    {
        StringBuilder result = new StringBuilder();

        Pair<Instant,Instant> startAndEnd = getTimeWindow();

        // If our zero date is Jan 1 at midnight, and our time scale width is 3
        // hours, and we are on the first window, we have to shift back in time
        // and take the maximum(time) observed value from the previous window
        // May be faster to have the db do date math but it is super confusing.

        result.append( "SELECT o.observation_time AS persistence_time," ).append( NEWLINE );
        result.append( "       o.observed_value AS observed_value," ).append( NEWLINE );
        result.append( "       '");
        // The end of the time window is what we pair on. Pass-through for later
        // functions that expect this date to be coming from DB.
        result.append( startAndEnd.getRight()
                                  .toString()
                                  .replace( "T", " " )
                                  .replace( "Z", "" ) );
        result.append( "' AS pair_time" ).append( NEWLINE );
        result.append( "FROM wres.observation AS o" ).append( NEWLINE );
        result.append( "INNER JOIN wres.projectsource AS ps" ).append( NEWLINE );
        result.append( "    ON ps.source_id = o.source_id" ).append( NEWLINE );
        result.append( "WHERE o.observed_value IS NOT NULL" ).append( NEWLINE );
        result.append( "    AND ps.project_id = " );
        result.append( getProjectDetails().getId() ).append( NEWLINE );
        result.append( "    AND ps.member = 'baseline'" ).append( NEWLINE );
        result.append( "    AND o.observation_time >= '" );

        result.append( startAndEnd.getLeft() );
        result.append( "'" ).append( NEWLINE );

        // The next line is intentionally exclusive to avoid picking t0's value.
        result.append( "    AND o.observation_time < '" );
        result.append( startAndEnd.getRight() );
        result.append( "'" ).append( NEWLINE );
        result.append( "ORDER BY o.observation_time DESC" ).append( NEWLINE );
        result.append( "LIMIT 1" ).append( NEWLINE );

        // TODO: demote to debug when PersistenceForecastScripter is integrated
        LOGGER.info( "{}", result );

        return result.toString();
    }

    @Override
    String getBaseDateName()
    {
        return "persistence_time";
    }

    @Override
    String getValueDate()
    {
        return this.getBaseDateName();
    }


    private Instant getZeroDate()
    {
        return this.zeroDate;
    }

    private Duration getTimeScaleWidth()
    {
        return this.timeScaleWidth;
    }

    private Pair<Instant,Instant> getTimeWindow()
    {
        Instant startTime = this.getZeroDate()
                                .plus( progress * getTimeScaleWidth().toMillis(),
                                       ChronoUnit.MILLIS );
        Instant endTime = startTime.plus( getTimeScaleWidth() );
        return Pair.of( startTime, endTime );
    }
}
