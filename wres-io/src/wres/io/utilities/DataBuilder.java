package wres.io.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DataBuilder
{
    private final Map<String, Integer> columnNames;
    private final List<Object[]> rows;
    private int currentRow;

    private DataBuilder()
    {
        this.columnNames = new TreeMap<>(  );

        this.rows = new ArrayList<>();
        this.currentRow = -1;
    }

    public static DataBuilder with(String... columnNames)
    {
        DataBuilder builder = new DataBuilder();

        if (columnNames.length == 0)
        {
            throw new IllegalArgumentException(
                    "Column names must be added to create a new data set."
            );
        }

        for (int index = 0; index < columnNames.length; ++index)
        {
            builder.columnNames.put(columnNames[index], index);
        }

        return builder;
    }

    public DataBuilder addRow()
    {
        this.currentRow += 1;
        this.rows.add( new Object[columnNames.size()] );
        return this;
    }

    public DataBuilder addRow(final Object[] data)
    {
        if (data.length > this.columnNames.size())
        {
            throw new IndexOutOfBoundsException(
                    "Input array too large; the maximum number of elements is " +
                    this.columnNames.size() +
                    ", but " + data.length + " objects were passed." );
        }

        this.currentRow += 1;
        Object[] newValues = new Object[this.columnNames.size()];

        System.arraycopy( data, 0, newValues, 0, data.length );

        this.rows.add( newValues );
        return this;
    }

    public DataBuilder set(final String columnName, final Object value)
    {
        if (!this.columnNames.containsKey( columnName ))
        {
            throw new IllegalArgumentException(
                    "'" + columnName + "' is not a valid column for this dataset."
            );
        }

        if (this.currentRow < 0)
        {
            this.addRow();
        }

        int index = this.columnNames.get( columnName );
        this.rows.get(currentRow)[index] = value;
        return this;
    }

    public DataProvider build()
    {
        return DataSetProvider.from(this.columnNames, this.rows);
    }
}
