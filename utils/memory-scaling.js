"use strict";

const os = require("os");

const TOTAL_MEM_BYTES = os.totalmem();
const BASELINE_MEM_BYTES = 4 * 1024 * 1024 * 1024; // 4 GB reference
const MEM_SCALE = Math.min(1.0, Math.max(0.125, TOTAL_MEM_BYTES / BASELINE_MEM_BYTES));
const LOW_MEMORY = TOTAL_MEM_BYTES <= 1024 * 1024 * 1024; // ≤1 GB

/**
 * Scale a capacity value proportionally to available system RAM.
 * On 4 GB the value is unchanged; on 512 MB it shrinks to 1/8th.
 * @param {number} base - The baseline value (tuned for a 4 GB box).
 * @param {number} [floor=10] - Minimum value to return.
 * @returns {number}
 */
function scaleForMemory(base, floor = 10) {
  return Math.max(floor, Math.round(base * MEM_SCALE));
}

module.exports = { scaleForMemory, MEM_SCALE, LOW_MEMORY, TOTAL_MEM_BYTES };
