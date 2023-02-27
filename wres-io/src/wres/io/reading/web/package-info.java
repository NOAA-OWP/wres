/**
 * This is where web-services meet format readers. Reading from a web service involves instantiating an adapter class,
 * which composes one or more format readers. The adapter class handles the web service interaction for a specific
 * service and the composed format reader(s) handle the format reading. Both the adapter class and the format reader
 * implements the {@link wres.io.reading.TimeSeriesReader} API. Thus, an appropriate reader may be instantiated for an
 * appropriate stream source (e.g., web, file etc.) and format(s) while advertising the same API and using the same
 * underlying implementation(s) for format reading.
 *
 * @author James Brown
 */

package wres.io.reading.web;

