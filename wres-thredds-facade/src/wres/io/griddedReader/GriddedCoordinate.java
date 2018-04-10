package wres.io.griddedReader;

public class GriddedCoordinate
{
    private int srid;
    private String datum;
    private float x_coordinate;
    private float y_coordinate;

    protected GriddedCoordinate()
    {
        this.srid= 0;
        this.datum= null;
        this.x_coordinate= 0;
        this.y_coordinate= 0;
    }

    public GriddedCoordinate ( int srid, String datum, float x_coordinate, float y_coordinate )
    {
        this.srid= srid;
        this.datum= datum;
        this.x_coordinate= x_coordinate;
        this.y_coordinate= y_coordinate;
    }

    public int getSrid() {
        return srid;
    }

    public String getDatum() {
        return datum;
    }

    public float getX_coordinate() {
        return x_coordinate;
    }

    public float getY_coordinate() {
        return y_coordinate;
    }
}
