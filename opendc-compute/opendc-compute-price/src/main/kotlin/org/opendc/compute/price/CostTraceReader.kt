package org.opendc.org.opendc.compute.price

import java.io.File
import javax.management.InvalidAttributeValueException

/**
 * Construct a workload from a trace.
 */
public fun getPriceFragment(pathToFile: String?): List<CarbonFragment>? {
    if (pathToFile == null) {
        return null
    }

    if (!File(pathToFile).exists()) {
        throw InvalidAttributeValueException("The carbon trace cannot be found")
    }

    return CostTraceLoader().get(file)
}
