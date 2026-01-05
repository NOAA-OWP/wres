package wres.config.components;

import java.time.MonthDay;
import java.util.Objects;

import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.SeasonDeserializer;
import wres.config.serializers.SeasonSerializer;

/**
 * A season filter. Provides a more convenient API for internal use than the {@link wres.statistics.generated.Season},
 * but the canonical form is available on request using {@link #canonical()}.
 * @param minimum the start of the season
 * @param maximum the end of the season
 */
@RecordBuilder
@JsonSerialize( using = SeasonSerializer.class )
@JsonDeserialize( using = SeasonDeserializer.class )
public record Season( MonthDay minimum, MonthDay maximum )
{
    /**
     * Returns a canonical description of the season.
     * @return the canonical season
     */

    public wres.statistics.generated.Season canonical()
    {
        wres.statistics.generated.Season.Builder builder = wres.statistics.generated.Season.newBuilder();

        // Earliest month-day?
        if ( Objects.nonNull( this.minimum() ) )
        {
            builder.setStartDay( this.minimum()
                                     .getDayOfMonth() )
                   .setStartMonth( this.minimum()
                                       .getMonthValue() );
        }

        // Latest month-day?
        if ( Objects.nonNull( this.maximum() ) )
        {
            builder.setEndDay( this.maximum()
                                   .getDayOfMonth() )
                   .setEndMonth( this.maximum()
                                     .getMonthValue() );
        }

        return builder.build();
    }
}
