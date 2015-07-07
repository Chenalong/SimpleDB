package simpledb;

import java.io.IOException;
import java.util.*;

import javax.swing.text.AsyncBoxView.ChildLocator;

import org.omg.CORBA.TypeCodeHolder;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends AbstractDbIterator
{
	
	/**
	 * Constructor.
	 * 
	 * @param t
	 *            The transaction running the insert.
	 * @param child
	 *            The child operator from which to read tuples to be inserted.
	 * @param tableid
	 *            The table in which to insert tuples.
	 * @throws DbException
	 *             if TupleDesc of child differs from table into which we are to
	 *             insert.
	 */
	private DbIterator _child;
	private int _tableId;
	private TransactionId _t;
	private boolean _done;
	private Tuple tp;
	public Insert(TransactionId t, DbIterator child, int tableid)
			throws DbException
	{
		_child = child;
		_tableId = tableid;
		_t = t;
		_done = false;
		tp = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
		// some code goes here
	}
	
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return tp.getTupleDesc();
	}
	
	public void open() throws DbException, TransactionAbortedException
	{
		// some code goes here
		_done = true;
		_child.open();
	}
	
	public void close()
	{
		// some code goes here
		_done = false;
		_child.close();
	}
	
	public void rewind() throws DbException, TransactionAbortedException
	{
		// some code goes here
		_child.rewind();
	}
	
	/**
	 * Inserts tuples read from child into the tableid specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or
	 *         null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException
	{
		// some code goes here
		if(_done == false)
			return null;
		
		int count = 0;
		try
		{
			while(_child.hasNext())
			{
				Tuple t = _child.next();
				//System.out.println(t.toString());
				Database.getBufferPool().insertTuple(_t, _tableId, t);
				count++;
			}
		}
		catch (IOException e)
		{
			DbException de = new DbException("");
			de.initCause(e);
			throw de;
			// TODO: handle exception
		}
		_done = false;
		tp.setField(0, new IntField(count));
		return tp;
	}
}
