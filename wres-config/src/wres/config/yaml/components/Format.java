package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An enumeration of statistics formats.
 * @author James Brown
 */
public enum Format
{
    /** Portable Network Graphics (PNG). */
    @JsonProperty( "png" ) PNG,
    /** Scalable Vector Graphics (SVG). */
    @JsonProperty( "svg" ) SVG,
    /** Comma Separated Values (CSV). */
    @JsonProperty( "csv" ) CSV,
    /** CSV Version 2.0. */
    @JsonProperty( "csv2" ) CSV2,
    /** Pairs in CSV format. */
    @JsonProperty( "pairs" ) PAIRS,
    /** Network Common Data Form (NetCDF). */
    @JsonProperty( "netcdf" ) NETCDF,
    /** NETCDF Version 2.0. */
    @JsonProperty( "netcdf2" ) NETCDF2,
    /** Protocol Buffers. */
    @JsonProperty( "protobuf" ) PROTOBUF
}
