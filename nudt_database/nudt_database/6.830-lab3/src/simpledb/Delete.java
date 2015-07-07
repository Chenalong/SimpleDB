package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends AbstractDbIterator
{
	
	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 * 
	 * @param t
	 *            The transaction this delete runs in
	 * @param child
	 *            The child operator from which to read tuples for deletion
	 */
	private TransactionId _tid;
	private DbIterator _child;
	private boolean _done;
	private Tuple _tp;
	public Delete(TransactionId t, DbIterator child)
	{
		_tid = t;
		_child = child;
		_done = false;
		_tp = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
		// some code goes here
	}
	
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return _tp.getTupleDesc();
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
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException
	{
		// some code goes here
		if(_done == false)
			return null;
		
		int count = 0;
			while(_child.hasNext())
			{
				Tuple tp = _child.next();
				Database.getBufferPool().deleteTuple(_tid, tp);
				count++;
			}

		_done = false;
		_tp.setField(0, new IntField(count));
		return _tp;
	}
}
