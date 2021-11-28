package indexManagement

import com.github.michaelbull.result.Err
import com.github.michaelbull.result.Ok
import com.github.michaelbull.result.Result
import pagedFile.BufferManager
import pagedFile.FileManager
import utils.ErrorCode

/*
 * IndexHandler 是针对 Database 的
 * IndexFile 则是针对
 */

class IndexManager(_bufferManager: BufferManager) {
    val bufferManager = _bufferManager

    fun createIndex(fileName: String) : Result<Unit, ErrorCode> {

        return Ok(Unit)
    }

    fun destroyIndex(fileName: String, indexNumber: Int) : Result<Unit, ErrorCode> {
        return Ok(Unit)
    }

    fun openIndex(fileName: String, indexNumber: Int) : Result<IndexFile, ErrorCode> {
        return Err(1)
    }

    fun closeIndex(indexHandler: IndexHandler) : Result<Unit, ErrorCode> {
        return Ok(Unit)
    }
}