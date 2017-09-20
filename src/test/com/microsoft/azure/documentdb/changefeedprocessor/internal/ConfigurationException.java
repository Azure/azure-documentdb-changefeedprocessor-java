/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 *
 */

package com.microsoft.azure.documentdb.changefeedprocessor.internal;

public class ConfigurationException extends Exception {
    public ConfigurationException(String message) {
        super(message);
    }
}
