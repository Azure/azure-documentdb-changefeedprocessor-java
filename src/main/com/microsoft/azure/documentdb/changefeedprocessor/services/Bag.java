package com.microsoft.azure.documentdb.changefeedprocessor.services;

import java.util.Collection;

public interface Bag<E> extends Collection<E> {
	int count(E e);
}
