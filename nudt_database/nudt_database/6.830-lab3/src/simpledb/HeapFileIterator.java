package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator
{
	private HeapFile		_hf			= null;
	private TransactionId	_tid;
	private int				_tableId;
	private int				_pageno;
	Iterator<Tuple>			_pageTuple	= null;
	
	public HeapFileIterator(HeapFile hf, TransactionId tid)
	{
		_hf = hf;
		_tid = tid;
		_tableId = hf.getId();
		_pageno = 0;
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException
	{
		_pageTuple = OpenInternal();
		// TODO Auto-generated method stub
		
	}
	
	public Iterator<Tuple> OpenInternal() throws DbException,
			TransactionAbortedException
	{
		if(_tid == null)
			System.out.println("in openInternal ------------");
		PageId pid = new HeapPageId(_tableId, _pageno);
		HeapPage p = (HeapPage) Database.getBufferPool().getPage(_tid, pid,
				Permissions.READ_ONLY);
		return p.iterator();
		
	}
	
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException
	{
		// TODO Auto-generated method stub
		if (_pageTuple == null)
			return false;
		if (_pageTuple.hasNext())
			return true;
		_pageno++;
		if (_pageno >= _hf.numPages())
			return false;
		_pageTuple = OpenInternal();
		return _pageTuple.hasNext();
	}
	
	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException
	{
		// TODO Auto-generated method stub
		if (hasNext())
			return _pageTuple.next();
		else
			throw new NoSuchElementException();
	}
	
	@Override
	public void rewind() throws DbException, TransactionAbortedException
	{
		// TODO Auto-generated method stub
		_pageno = 0;
		_pageTuple = OpenInternal();
		
	}
	
	@Override
	public void close()
	{
		_pageno = 0;
		_pageTuple = null;
		// TODO Auto-generated method stub
		
	}
	
}
