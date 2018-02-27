package wres.io.utilities;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class was originally used in development to separate a resultset
 * from its database components. A function could use a connection and
 * statement to retrieve data, but closing the database statement and
 * connection would render the resultset unreadable. As a result, you
 * could not retrieve data from the database in one thread and send it to
 * another. This was built as a possible solution.
 *
 * Despite being a workable solution, it was later removed as a return type
 * for Database.execute and SQLExecutor.execute. Large queries simply
 * return too much data and result in paging, slowing down the entire
 * application. This remains in the system in case it is needed down the line.
 * If not, it is safe to remove.
 */
public class DataSet
{
    private List<String> columnNames;
    private List<Object[]> rows;
    private int currentRow = -1;

    public DataSet(ResultSet resultSet ) throws SQLException
    {
        if (resultSet == null)
        {
            return;
        }

        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();

        // JDBC is ones indexed
        for (int columnIndex = 1; columnIndex <= columnCount; ++columnIndex)
        {
            if (this.columnNames == null)
            {
                this.columnNames = new ArrayList<>(  );
            }

            this.columnNames.add( metadata.getColumnLabel( columnIndex ) );
        }

        while (resultSet.next())
        {
            if (this.rows == null)
            {
                this.rows = new ArrayList<>();
            }

            Object[] row = new Object[columnCount];

            if (resultSet.getObject(1).toString().equals("1981-04-26 01:00:00.0"))
            {
                System.err.print( "An error prone date is being loaded." );
            }

            for ( int columnIndex = 0;
                  columnIndex < columnCount;
                  ++columnIndex )
            {
                row[columnIndex] = resultSet.getObject( columnIndex + 1 );
            }

            this.rows.add( row );
        }

    }

    public boolean next()
    {
        this.currentRow++;
        return this.currentRow < rows.size();
    }

    public boolean back()
    {
        this.currentRow--;
        return this.currentRow >= 0;
    }

    public void toEnd()
    {
        this.currentRow = this.rows.size() - 1;
    }

    public void reset()
    {
        if (this.rows.isEmpty())
        {
            this.currentRow = -1;
        }
        else
        {
            this.currentRow = 0;
        }
    }

    private Object getObject(int rowNumber, int index) throws NoDataException
    {
        // If "next" hasn't been called, go ahead and move on to the next row
        if (rowNumber < 0)
        {
            rowNumber++;
        }

        if (this.rows == null)
        {
            throw new NoDataException( "There is no data to retrieve from the data set." );
        }
        else if (rowNumber >= this.rows.size())
        {
            throw new IndexOutOfBoundsException( "There are no more rows to retrieve data from. Index = " +
                                                 String.valueOf(rowNumber) +
                                                 ", Row Count: " +
                                                 String.valueOf(this.rows.size()) );
        }
        else if (this.rows.get( rowNumber ).length <= index)
        {
            throw new IndexOutOfBoundsException( "The provided index exceeds the length of the row. Index = " +
                                                 String.valueOf(index) +
                                                 " Row Length: " +
                                                 String.valueOf(this.rows.get(rowNumber).length) );
        }

        return this.rows.get(rowNumber)[index];
    }

    private Integer getColumnIndex(String columnName) throws NoDataException
    {
        if (this.columnNames == null)
        {
            throw new NoDataException( "There is no data to retrieve from the data set" );
        }

        Integer index = this.columnNames.indexOf( columnName );

        if (index < 0)
        {
            throw new IndexOutOfBoundsException( "There is no column in the data set named " + columnName );
        }

        return index;
    }

    public Object getObject(int index) throws NoDataException
    {
        Object value = this.getObject( this.currentRow, index );

        if (value == null)
        {
            return null;
        }

        return this.getObject( this.currentRow, index );
    }

    public Object getObject(String columnName) throws NoDataException
    {
        return this.getObject(this.getColumnIndex( columnName ));
    }

    public Integer getInt(int index) throws NoDataException
    {
        Object value = this.getObject( index );

        if (value == null)
        {
            return null;
        }

        return (Integer)value;
    }

    public String getString(int index) throws NoDataException
    {
        Object value = this.getObject( index );

        if (value == null)
        {
            return null;
        }

        return value.toString();
    }

    public Float getFloat(int index) throws NoDataException
    {
        Object value = this.getObject( index );

        if (value == null)
        {
            return null;
        }

        return (Float)value;
    }

    public Double getDouble(int index) throws NoDataException
    {
        Object value = this.getObject( index );

        if (value == null)
        {
            return null;
        }

        return (Double)value;
    }

    public Long getLong(int index) throws NoDataException
    {
        Object value = this.getObject( index );

        if (value == null)
        {
            return null;
        }

        return (Long)value;
    }

    public Short getShort(int index) throws NoDataException
    {
        Object value = this.getObject( index );

        if (value == null)
        {
            return null;
        }

        return (Short)value;
    }

    public Integer getInt(String columnName) throws NoDataException
    {
        return this.getInt( this.getColumnIndex( columnName ) );
    }

    public Short getShort(String columnName) throws NoDataException
    {
        return this.getShort( this.getColumnIndex( columnName ) );
    }

    public Long getLong(String columnName) throws NoDataException
    {
        return this.getLong(this.getColumnIndex( columnName ));
    }

    public Float getFloat(String columnName) throws NoDataException
    {
        return this.getFloat(this.getColumnIndex( columnName ));
    }

    public Double getDouble(String columnName) throws NoDataException
    {
        return this.getDouble(this.getColumnIndex(columnName));
    }

    public String getString(String columnName) throws NoDataException
    {
        return this.getString(this.getColumnIndex( columnName ));
    }
}
