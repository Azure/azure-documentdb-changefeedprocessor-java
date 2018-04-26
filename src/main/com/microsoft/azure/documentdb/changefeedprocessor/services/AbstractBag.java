package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.AbstractCollection;

public abstract class AbstractBag<E> extends AbstractCollection<E> implements Bag<E> {
	protected AbstractBag() {
		
	}
	
	@Override
	public int count(E e)
	{
		int count = 0;
		for (E entry : this) {
			if (e == null ? entry == null : e.equals(entry)) {
				count++;
			}
		}
		return count;
	}

	@Override
	public boolean equals(Object o) {
		if(o == this) {
			return true;
		}
		
		if(!(o instanceof Bag)) {
			return false;
		}
		
		Bag<E> bag = (Bag<E>) o;
		if(bag.size() != size()) {
			return false;
		} else if(bag.size() == size()) {
			for(E element: this) {
				if(count(element) != bag.count(element)) {
					return false;
				}
			}
			return true;
		}
		
		try {
			
		} catch(ClassCastException unused) {
			return false;
		} catch(NullPointerException unused) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		int hash = 1;
		for (E entry : this) {
			hash = 31 * hash + count(entry);
		}
		return hash;

	}
}
