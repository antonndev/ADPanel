#!/bin/bash
nice -n 19 ionice -c 3 npm audit fix --package-lock-only