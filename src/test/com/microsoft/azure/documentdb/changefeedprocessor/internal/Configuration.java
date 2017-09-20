/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 *
 */

package com.microsoft.azure.documentdb.changefeedprocessor.internal;

public interface Configuration {
    String get(String name) throws ConfigurationException;
}
