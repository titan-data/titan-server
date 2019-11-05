/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.verifySequence
import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.exception.NoSuchObjectException
import io.titandata.util.CommandExecutor

class ZfsStorageProviderTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @InjectMockKs
    @OverrideMockKs
    var provider = ZfsStorageProvider("test")

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    /**
     * Utility method that will convert the openapi-generated properties type into a map that can
     * be used within kotlin. This avoids the needless proliferation of UNCHECKED_CAST warnings.
     */
    fun getProperties(properties: Any): Map<String, Any> {
        @Suppress("UNCHECKED_CAST")
        return properties as Map<String, Any>
    }

    init {
        "valid metadata is parsed correctly" {
            val result = provider.parseMetadata("{ \"foo\": \"bar\" }")
            result["foo"] shouldBe "bar"
        }

        "invalid metadata throws an exception" {
            shouldThrow<InvalidStateException> {
                provider.parseMetadata("this is not JSON")
            }
        }

        "complex metadata is parsed correctly" {
            val result = provider.parseMetadata("{ \"a\": 1, \"b\": { \"c\": \"two\" }}")
            result["a"] shouldBe 1.0
            getProperties(result.getValue("b"))["c"] shouldBe "two"
        }

        "tabs are allowed in metadata" {
            val result = provider.parseMetadata("{\t\"a\": \"b\tc\" }")
            result["a"] shouldBe "b\tc"
        }

        "validate name works for all characters" {
            provider.validateName("aB5:.-_aG", ZfsStorageProvider.ObjectType.REPOSITORY)
        }

        "validate name fails for zero length name" {
            shouldThrow<IllegalArgumentException> {
                provider.validateName("", ZfsStorageProvider.ObjectType.REPOSITORY)
            }
        }

        "validate name fails for invalid character" {
            shouldThrow<IllegalArgumentException> {
                provider.validateName("not/allowed", ZfsStorageProvider.ObjectType.REPOSITORY)
            }
        }

        "clone commit with no volumes creates empty dataset" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.cloneCommit("foo", "guid", "hash", "newguid")
            verifySequence {
                executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid")
                executor.exec("zfs", "create", "test/repo/foo/newguid")
            }
            confirmVerified()
        }

        "clone commit clones all volumes" {
            every { executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid") } returns
                    arrayOf("test/repo/foo/guid\t-", "test/repo/foo/guid/v0\t{\"a\":\"b\"}", "test/repo/foo/guid/v1\t{\"b\":\"c\"}").joinToString("\n")
            every { executor.exec("zfs", "create", "test/repo/foo/newguid") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid/v0@hash",
                    "test/repo/foo/newguid/v0") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={\"b\":\"c\"}", "test/repo/foo/guid/v1@hash",
                    "test/repo/foo/newguid/v1") } returns ""
            provider.cloneCommit("foo", "guid", "hash", "newguid")
            verifySequence {
                executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid")
                executor.exec("zfs", "create", "test/repo/foo/newguid")
                executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid/v0@hash", "test/repo/foo/newguid/v0")
                executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={\"b\":\"c\"}", "test/repo/foo/guid/v1@hash", "test/repo/foo/newguid/v1")
            }
            confirmVerified()
        }
    }
}
