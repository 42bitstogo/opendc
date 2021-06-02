/*
 * Copyright (c) 2020 AtLarge Research
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

package org.opendc.simulator.compute

import kotlinx.coroutines.*
import org.opendc.simulator.compute.cpufreq.ScalingDriver
import org.opendc.simulator.compute.cpufreq.ScalingGovernor
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.resources.*
import org.opendc.simulator.resources.SimResourceInterpreter

/**
 * A simulated bare-metal machine that is able to run a single workload.
 *
 * A [SimBareMetalMachine] is a stateful object and you should be careful when operating this object concurrently. For
 * example. The class expects only a single concurrent call to [run].
 *
 * @param interpreter The [SimResourceInterpreter] to drive the simulation.
 * @param model The machine model to simulate.
 * @param scalingGovernor The CPU frequency scaling governor to use.
 * @param scalingDriver The CPU frequency scaling driver to use.
 * @param parent The parent simulation system.
 */
@OptIn(ExperimentalCoroutinesApi::class, InternalCoroutinesApi::class)
public class SimBareMetalMachine(
    interpreter: SimResourceInterpreter,
    override val model: SimMachineModel,
    scalingGovernor: ScalingGovernor,
    scalingDriver: ScalingDriver,
    parent: SimResourceSystem? = null,
) : SimAbstractMachine(interpreter, parent) {
    override val cpus: List<SimProcessingUnit> = model.cpus.map { ProcessingUnitImpl(it) }

    /**
     * Construct the [ScalingDriver.Logic] for this machine.
     */
    private val scalingDriver = scalingDriver.createLogic(this)

    /**
     * The scaling contexts associated with each CPU.
     */
    private val scalingGovernors = cpus.map { cpu ->
        scalingGovernor.createLogic(this.scalingDriver.createContext(cpu))
    }

    init {
        scalingGovernors.forEach { it.onStart() }
    }

    /**
     * The power draw of the machine.
     */
    public var powerDraw: Double = 0.0
        private set

    override fun updateUsage(usage: Double) {
        super.updateUsage(usage)

        scalingGovernors.forEach { it.onLimit() }
        powerDraw = scalingDriver.computePower()
    }

    /**
     * The [SimProcessingUnit] of this machine.
     */
    public inner class ProcessingUnitImpl(override val model: ProcessingUnit) : SimProcessingUnit {
        /**
         * The actual resource supporting the processing unit.
         */
        private val source = SimResourceSource(model.frequency, interpreter, this@SimBareMetalMachine)

        override val state: SimResourceState
            get() = source.state

        override val capacity: Double
            get() = source.capacity

        override val speed: Double
            get() = source.speed

        override val demand: Double
            get() = source.demand

        override val counters: SimResourceCounters
            get() = source.counters

        override fun startConsumer(consumer: SimResourceConsumer) {
            source.startConsumer(consumer)
        }

        override fun interrupt() {
            source.interrupt()
        }

        override fun cancel() {
            source.cancel()
        }

        override fun close() {
            source.close()
        }
    }
}
