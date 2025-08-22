package wres.config.components;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.serializers.EnsembleAverageTypeSerializer;
import wres.config.serializers.GeneratedBaselineSerializer;
import wres.statistics.generated.Pool;

/**
 * A class that stores the parameters for generating a baseline datasets from a time-series data source.
 * @param method the method, required
 * @param order the order of a persistence baseline
 * @param average the average type for a single-valued climatological baseline
 * @param minimumDate the minimum date for a climatological baseline
 * @param maximumDate the maximum date for a climatological baseline
 * @author James Brown
 */
@RecordBuilder
@JsonSerialize( using = GeneratedBaselineSerializer.class )
public record GeneratedBaseline( @JsonProperty( "name" ) GeneratedBaselines method,
                                 @JsonProperty( "order" ) Integer order,
                                 @JsonSerialize( using = EnsembleAverageTypeSerializer.class )
                                 @JsonProperty( "average" ) Pool.EnsembleAverageType average,
                                 @JsonProperty( "minimum_date" ) Instant minimumDate,
                                 @JsonProperty( "maximum_date" ) Instant maximumDate )
{
    /**
     * Set the defaults.
     * @param method the method, required
     * @param order the order of a persistence baseline
     * @param average the average type for a single-valued climatological baseline
     * @param minimumDate the minimum date for a climatological baseline
     * @param maximumDate the maximum date for a climatological baseline
     */
    public GeneratedBaseline
    {
        if ( Objects.isNull( order ) )
        {
            order = 1;
        }

        if ( Objects.isNull( average ) )
        {
            average = Pool.EnsembleAverageType.MEAN;
        }

        if ( Objects.isNull( minimumDate ) )
        {
            minimumDate = Instant.MIN;
        }

        if ( Objects.isNull( maximumDate ) )
        {
            maximumDate = Instant.MAX;
        }
    }
}
