package wres.io.reading;

/**
 * @author ctubbs
 *
 */
public class ReaderFactory {
    private ReaderFactory(){}

    /*private static final int MAX_PERMIT = 10;
    private static final Semaphore availableNonsense = new Semaphore(MAX_PERMIT);*/


	public static BasicSource getReader(String filename) throws Exception
	{
		SourceType type_of_file = getFiletype(filename);
		
		BasicSource source = null;

		//availableNonsense.acquire();
		switch (type_of_file) {
			case DATACARD:
				// TODO: Implement new Datacard reader that adheres to new schema
				//source = new wres.io.reading.nws.DatacardSource(filename);
				break;
			case ASCII:
				// TODO: Implement new ASCII reader that adheres to new schema
				//source = new wres.io.reading.misc.ASCIISource(filename);
				break;
			case NETCDF:
				source = new wres.io.reading.ucar.NetCDFSource(filename);
				break;
			case PI_XML:
				source = new wres.io.reading.fews.FEWSSource(filename);
				break;
			default:
				String message = "The file '%s' is not a valid data file.";
				throw new Exception(String.format(message, filename));
		}

		/*if (source != null) {
			source.setCloseHandler(basicSource -> {
				availableNonsense.release();
				System.err.println(System.lineSeparator() + "Reader released from finalizer." + System.lineSeparator());
			});
		}*/

		return source;
	}

	/*public static void releaseReader()
	{
		availableNonsense.release();
		System.err.println(System.lineSeparator() + "Reader released." + System.lineSeparator());
	}*/

	
	private static SourceType getFiletype(String filename)
	{
		SourceType type = SourceType.UNDEFINED;
		
		filename = filename.toLowerCase();
		if (filename.endsWith(".asc"))
		{
			type = SourceType.ASCII;
		}
		else if (filename.endsWith("map06"))
		{
			type = SourceType.DATACARD;
		}
		else if (filename.endsWith(".nc") || filename.endsWith(".gz"))
		{
			type = SourceType.NETCDF;
		}
		else if (filename.endsWith(".xml"))
		{
			type = SourceType.PI_XML;
		}

		return type;
	}
}
