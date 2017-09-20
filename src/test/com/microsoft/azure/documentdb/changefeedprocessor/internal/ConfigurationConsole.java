/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 *
 */

package com.microsoft.azure.documentdb.changefeedprocessor.internal;

import java.util.Scanner;

public class ConfigurationConsole implements Configuration {
    private Scanner _scanner;

    public ConfigurationConsole() {
        _scanner = new Scanner(System.in);
    }

    public String get(String name) {
        PrintLine(name);

        String value = null;

        while ( value == null || value == "" ) {
            value = ReadLine();
        }

        return value;
    }

    private void PrintLine(String name) {
        System.out.print(name + ": ");
    }

    private String ReadLine() {
        return _scanner.nextLine();
    }
}
