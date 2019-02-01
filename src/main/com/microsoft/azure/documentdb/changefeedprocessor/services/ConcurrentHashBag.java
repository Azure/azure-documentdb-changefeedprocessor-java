/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
