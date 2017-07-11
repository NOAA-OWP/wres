package wres.datamodel;

public class SafeMatrixOfDoubles implements MatrixOfDoubles
{
    private final double[][] doubles;

    private SafeMatrixOfDoubles(final double[][] doubles)
    {
        this.doubles = doubles.clone();
    }

    public static MatrixOfDoubles of(final double[][] doubles)
    {
        return new SafeMatrixOfDoubles(doubles);
    }

    @Override
    public double[][] getDoubles()
    {
        return doubles.clone();
    }

    @Override
    public int rows()
    {
        return doubles.length;
    }

    @Override
    public int columns()
    {
        return doubles[0].length;
    }

    @Override
    public boolean isSquare()
    {
        return rows() == columns();
    }
}
