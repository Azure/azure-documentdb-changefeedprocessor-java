/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.documentdb.changefeedprocessor.internal;

/**
 *
 * @author yoterada
 */
public class TraceLog {
    private static int id = 0;
    enum TraceEventType {
        Verbose, Information, Warning, Error
    };

    public static void verbose(String message)
    {
        traceEvent(TraceEventType.Verbose, message);
    }

    public static void informational(String message)
    {
        traceEvent(TraceEventType.Information, message);
    }

    public static void warning(String message)
    {
        traceEvent(TraceEventType.Warning, message);
    }

    public static void error(String message)
    {
        traceEvent(TraceEventType.Error, message);
    }

    public static void exception(Exception ex)
    {
        // TODO
        // Error(GetExceptionText(ex));
    }

    private static void traceEvent(TraceEventType eventType, String message)
    {
        // TODO
        //traceSource.TraceEvent(eventType, Interlocked.Increment(ref id), String.Format("{0}: {1}", DateTime.Now, message));
    }

    private static String GetExceptionText(Exception ex)
    {
        return null;
        // TODO
        // String message = String.Empty;
        // try
        // {
        //     message = String.Format("Exception: {0}:{1}\n{2}", ex.GetType(), ex.Message, ex.StackTrace);
        // }
        // catch
        // {
        //     message = "Error formatting exception.";
        // }

        // return message;
    }
}
