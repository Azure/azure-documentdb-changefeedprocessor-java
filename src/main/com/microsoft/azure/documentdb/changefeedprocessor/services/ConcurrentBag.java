package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Collection;

public interface ConcurrentBag<E> extends Bag<E> {
	int addAllAbsent(Collection<? extends E> c);
	
	boolean addIfAbsent(E e);
}
