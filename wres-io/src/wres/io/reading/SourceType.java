package wres.io.reading;

import wres.util.Internal;

/**
 * @author ctubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public enum SourceType {
	ASCII,
	ASCII_RASTER,
	DATACARD,
	NETCDF,
	PI_XML,
	ARCHIVE,
	UNDEFINED;
}
