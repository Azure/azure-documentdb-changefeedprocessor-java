package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
*
* @author rogirdh
*/
public class ConcurrentHashBag<E> extends AbstractBag<E> implements ConcurrentBag<E>, Serializable {
	
	private static final long serialVersionUID = 1L;
	private final ConcurrentMap<E, CopyOnWriteArrayList<E>> elements;

	public ConcurrentHashBag(int initialSize) {
		if(initialSize >= 0) {
			this.elements = new ConcurrentHashMap<E, CopyOnWriteArrayList<E>>(initialSize);
		}
		else {
			throw new IllegalArgumentException();
		}
	}
	
	public ConcurrentHashBag() {
		this(5);
	}
	
	public ConcurrentHashBag(Collection<? extends E> c) {
		this(c.size());
		addAll(c);
	}

	public ConcurrentHashBag(Iterable<? extends E> i) {
		this();
		for (E e : i) {
			add(e);
		}
	}

	public ConcurrentHashBag(Iterator<? extends E> i) {
		this();
		while (i.hasNext()) {
			add(i.next());
		}
	}

	@Override
	public boolean add(E e) {
		return getEntry(e).add(e);
	}

	@Override
	public int addAllAbsent(Collection<? extends E> c) {
		int count = 0;
		for (E e : c) {
			if (addIfAbsent(e)) {
				count++;
			}
		}
		return count;
	}

	@Override
	public boolean addIfAbsent(E e) {
		return getEntry(e).addIfAbsent(e);
	}

	@Override
	public void clear() {
		elements.clear();
	}

	@Override
	public boolean contains(Object o) {
		List<E> entry = elements.get(o);
		return entry == null ? false : !entry.isEmpty();
	}

	@Override
	public int count(E e) {
		List<E> entry = elements.get(e);
		return entry == null ? 0 : entry.size();
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public Iterator<E> iterator() {
		return ((Iterable<E>) elements.values()).iterator();
	}

	@Override
	public boolean remove(Object o) {
		List<E> element = elements.get(o);
		return element == null ? false : element.remove(o);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean removed = false;
		for (Object o : c) {
			removed |= elements.remove(o) != null;
		}
		return removed;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean updated = false;
		for (List<E> element : elements.values()) {
			updated |= element.retainAll(c);
		}
		return updated;
	}

	@Override
	public int size() {
		int size = 0;
		for (List<E> value : elements.values()) {
			size += value.size();
		}
		return size;
	}

	private CopyOnWriteArrayList<E> getEntry(E e) {
		elements.putIfAbsent(e, new CopyOnWriteArrayList<E>());
		return elements.get(e);
	}


}
