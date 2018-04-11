package wres.io.griddedReader;

import java.nio.file.Path;
import java.time.Duration;

public class GriddedPath
{
    private String side;
    private Path path;
    private Duration leadTime;

    protected GriddedPath()
    {
        this.side=null;
        this.path=null;
        this.leadTime=null;
    }

    public GriddedPath ( String side, Path path, Duration leadTime )
    {
        this.side= side;
        this.path= path;
        this.leadTime= leadTime;
    }

    public String getSide()
    {
        return side;
    }

    public Path getPath()
    {
        return path;
    }

    public Duration getLeadTime()
    {
        return leadTime;
    }
}
