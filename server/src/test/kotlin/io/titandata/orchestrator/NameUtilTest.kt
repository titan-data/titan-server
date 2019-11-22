package io.titandata.orchestrator

import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.util.UUID

class NameUtilTest : StringSpec() {

    init {
        "names cannot be zero length" {
            shouldThrow<IllegalArgumentException> {
                NameUtil.validateCommon("", "")
            }
        }

        "names can be 63 characters long" {
            NameUtil.validateCommon("a".repeat(63), "")
        }

        "names cannot be 64 characters long" {
            shouldThrow<IllegalArgumentException> {
                NameUtil.validateCommon("a".repeat(64), "")
            }
        }

        "names can contain valid characters" {
            NameUtil.validateCommon("aB09.-c", "")
        }

        "names cannot contain underscores" {
            shouldThrow<IllegalArgumentException> {
                NameUtil.validateCommon("a_b", "")
            }
        }

        "names cannot contain colons" {
            shouldThrow<IllegalArgumentException> {
                NameUtil.validateCommon("a:b", "")
            }
        }

        "operation IDs fail if non-UUID" {
            shouldThrow<IllegalArgumentException> {
                NameUtil.validateOperationId("a")
            }
        }

        "operation IDs success if UUID" {
            NameUtil.validateOperationId(UUID.randomUUID().toString())
        }
    }
}
