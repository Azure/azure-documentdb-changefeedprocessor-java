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
public interface ICheckpointManager {
    /// <summary>
    /// Provides methods for running checkpoint asynchronously. Extensibility is provided to specify host-specific storage for storing the offset.
    /// </summary>

}
