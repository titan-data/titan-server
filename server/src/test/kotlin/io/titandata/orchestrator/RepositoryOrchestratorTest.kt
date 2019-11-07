/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.mockk.just
import io.mockk.verify
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Repository
import io.titandata.operation.OperationExecutor
import io.titandata.remote.engine.EngineParameters
import io.titandata.remote.nop.NopParameters
import io.titandata.remote.nop.NopRemote
import io.titandata.remote.nop.NopRemoteProvider
import io.titandata.storage.OperationData
import io.titandata.storage.zfs.ZfsStorageProvider
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestratorTest : StringSpec() {

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @SpyK
    var nopRemoteProvider = NopRemoteProvider()

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    override fun beforeSpec(spec: Spec) {
        providers.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        providers.metadata.clear()
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        // TODO
    }
}
