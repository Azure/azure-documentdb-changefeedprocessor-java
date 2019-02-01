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
