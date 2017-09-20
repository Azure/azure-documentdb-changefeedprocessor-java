/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 *
 */

package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigurationFile implements Configuration {
    private Properties _props;

    public ConfigurationFile(String filename) throws ConfigurationException {
        FileReader reader = null;

        try {
            reader = new FileReader(filename);
            Properties props = new Properties();
            props.load(reader);

            _props = props;

            reader.close();
        }
        catch(FileNotFoundException ex) {
            throw new ConfigurationException("File not found: " + filename + " (Current directory = " + System.getProperty("user.dir") + ")");
        }
        catch(IOException ex) {
            throw new ConfigurationException("Failed to read properties from file: " + filename);
        }
    }

    public String get(String name) throws ConfigurationException {
        String value = _props.getProperty(name);

        if( value == null || value == "" )
            throw new ConfigurationException("Empty configuration for property: " + name);

        return value;
    }
}
