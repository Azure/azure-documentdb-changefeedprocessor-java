/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
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

        if( value == null || value.isEmpty() )
            throw new ConfigurationException("Empty configuration for property: " + name);

        return value;
    }
}
