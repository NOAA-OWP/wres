package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A unit alias.
 * @param alias the alias
 * @param unit the UCUM unit
 */
@RecordBuilder
public record UnitAlias( @JsonProperty( "alias" ) String alias, @JsonProperty( "unit" ) String unit ) {}
